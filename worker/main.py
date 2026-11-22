import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone

from scd import at_time, materialize, merge_events, normalize
from storage import tables, write

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def run():
    from kafka import KafkaConsumer
    raw, dim = tables()
    existing = raw.scan().to_arrow().to_pylist()
    # Repair a crash after raw commit and before dimension commit, even with no new input.
    if existing:
        write(dim, materialize(existing), overwrite=True)
    consumer = KafkaConsumer(
        os.environ.get("KAFKA_TOPIC", "bank.public.accounts"),
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
        group_id="accounts-scd2-v1", enable_auto_commit=False,
        auto_offset_reset="earliest", max_poll_interval_ms=900000,
    )
    try:
        while True:
            started = time.monotonic()
            batch = consumer.poll(timeout_ms=2000, max_records=500)
            if not batch:
                continue
            incoming = [normalize(m.value.decode() if m.value is not None else None,
                                  m.topic, m.partition, m.offset)
                        for messages in batch.values() for m in messages]
            merged, added = merge_events(existing, incoming)
            versions = materialize(merged)  # Validate BEFORE committing either table.
            write(raw, added)
            # Atomic within THIS Iceberg table. Raw and dim are two separate commits.
            write(dim, versions, overwrite=True)
            consumer.commit()  # At-least-once; only after durable Iceberg commits.
            existing = merged
            logging.info("batch kafka=%s new_raw=%s versions=%s offsets committed",
                         sum(map(len, batch.values())), len(added), len(versions))
            time.sleep(max(0, 2 - (time.monotonic() - started)))
    finally:
        consumer.close(autocommit=False)


def show(args):
    raw, dim = tables()
    table = raw if args.raw else dim
    rows = table.scan().to_arrow().to_pylist()
    if args.at:
        when = datetime.fromisoformat(args.at.replace("Z", "+00:00"))
        if when.tzinfo is None:
            raise SystemExit("Specify a timezone, e.g. 2026-11-22T12:00:00Z")
        rows = at_time(rows, int(when.timestamp() * 1000), args.account)
    elif args.account is not None:
        rows = [r for r in rows if r["account_id"] == args.account]
    if args.current:
        rows = [r for r in rows if r["is_current"] and not r["is_deleted"]]
    for row in rows:
        for key in ["valid_from_ms", "valid_to_ms", "event_time_ms"]:
            if row.get(key) is not None:
                row[key.removesuffix("_ms")] = datetime.fromtimestamp(row[key] / 1000, timezone.utc).isoformat()
    print(json.dumps(sorted(rows, key=lambda r: (r["account_id"], r.get("version", r.get("offset", 0)))),
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["run", "show"], nargs="?", default="run")
    parser.add_argument("--account", type=int)
    parser.add_argument("--at", help="ISO 8601 with timezone")
    parser.add_argument("--current", action="store_true")
    parser.add_argument("--raw", action="store_true")
    args = parser.parse_args()
    if args.raw and (args.at or args.current):
        parser.error("--raw cannot be used with --at or --current")
    run() if args.command == "run" else show(args)
