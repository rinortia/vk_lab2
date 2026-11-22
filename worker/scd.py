"""Deterministic SCD Type 2 projection. Uses only the Python standard library."""
import hashlib
import json
from collections import defaultdict


def normalize(value, topic, partition, offset):
    if value is None:  # Kafka tombstone follows Debezium's delete event.
        return None
    event = json.loads(value)
    op = event.get("op")
    if op not in {"r", "c", "u", "d"}:
        raise ValueError(f"Unsupported CDC operation: {op!r}")
    row = event["before"] if op == "d" else event["after"]
    if not row:
        raise ValueError("Missing row; check REPLICA IDENTITY FULL")
    source = event["source"]
    if op == "r":
        identity = [topic, "snapshot", source["lsn"], row]
    else:
        tx = event.get("transaction")
        if not tx or not tx.get("id") or tx.get("total_order") is None:
            raise ValueError("Enable provide.transaction.metadata in Debezium")
        identity = [topic, str(tx["id"]), str(tx["total_order"])]
    source_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    # Snapshot timestamps describe observation time, not historical business time.
    timestamp = source.get("ts_ms") or event.get("ts_ms")
    if not timestamp:
        raise ValueError("Missing event timestamp")
    return {
        "event_id": f"{topic}:{partition}:{offset}", "source_id": source_id,
        "topic": topic, "partition": partition, "offset": offset,
        "account_id": int(row["account_id"]), "owner": row["owner"],
        "city": row["city"], "status": row["status"], "op": op,
        "event_time_ms": int(timestamp), "lsn": int(source["lsn"]),
        "raw_json": value,
    }


def merge_events(existing, incoming):
    """Idempotency for replay of already persisted Kafka offsets."""
    by_id = {e["event_id"]: e for e in existing}
    added = []
    for event in incoming:
        if event is not None and event["event_id"] not in by_id:
            by_id[event["event_id"]] = event
            added.append(event)
    return list(by_id.values()), added


def materialize(events):
    """Retain every actual change, including multiple changes in a single batch."""
    per_key = defaultdict(list)
    seen = set()
    # The demo topic has ONE partition; never change its partition count in place.
    for event in sorted(events, key=lambda e: (e["partition"], e["offset"])):
        if event["source_id"] in seen:
            continue  # Debezium can republish an event at a NEW Kafka offset.
        seen.add(event["source_id"])
        history = per_key[event["account_id"]]
        attrs = (event["owner"], event["city"], event["status"], event["op"] == "d")
        if history:
            prev = history[-1]
            if event["partition"] != prev["partition"]:
                raise ValueError("Account moved between partitions; rebuild required")
            if attrs == (prev["owner"], prev["city"], prev["status"], prev["is_deleted"]):
                continue
            if event["event_time_ms"] < prev["valid_from_ms"]:
                raise ValueError("Source clock moved backwards; manual correction required")
            prev["valid_to_ms"] = event["event_time_ms"]
            prev["is_current"] = False
        history.append({
            "version_id": event["source_id"], "account_id": event["account_id"],
            "version": len(history) + 1, "owner": attrs[0], "city": attrs[1],
            "status": attrs[2], "is_deleted": attrs[3],
            "valid_from_ms": event["event_time_ms"], "valid_to_ms": None,
            "is_current": True, "partition": event["partition"],
            "kafka_offset": event["offset"], "lsn": event["lsn"],
        })
    return [row for key in sorted(per_key) for row in per_key[key]]


def at_time(rows, timestamp_ms, account_id=None):
    return [r for r in rows if (account_id is None or r["account_id"] == account_id)
            and r["valid_from_ms"] <= timestamp_ms
            and (r["valid_to_ms"] is None or timestamp_ms < r["valid_to_ms"])
            and not r["is_deleted"]]
