"""Run AFTER start.sh on a fresh lab. Fails on unexpected SCD results."""
import json
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True)


def history():
    return json.loads(command("docker", "compose", "exec", "-T", "worker",
                              "python", "main.py", "show"))


def wait_for(check):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        rows = history()
        if check(rows):
            return rows
        time.sleep(2)
    raise TimeoutError("CDC projection did not reach expected state within 120 seconds")


def sql(statement):
    print(command("docker", "compose", "exec", "-T", "source", "psql", "-v", "ON_ERROR_STOP=1",
                  "-U", "bank", "-d", "bank", "-c", statement).strip())


def main():
    initial = wait_for(lambda rows: len(rows) >= 2)
    assert len(initial) == 2 and all(r["version"] == 1 for r in initial), "Use a fresh lab; demo is one-shot"
    original = next(r for r in initial if r["account_id"] == 1001)
    assert original["owner"] == "Мария Соколова" and original["status"] == "active"
    changes = [
        "UPDATE accounts SET owner='Наталья Волкова' WHERE account_id=1001",
        "UPDATE accounts SET city='Санкт-Петербург' WHERE account_id=1001",
        "UPDATE accounts SET status='blocked' WHERE account_id=1001",
        "UPDATE accounts SET status='blocked' WHERE account_id=1001",  # no-op
        "DELETE FROM accounts WHERE account_id=1002",
        "INSERT INTO accounts VALUES (1003, 'Олег Лебедев', 'Пермь', 'active')",
    ]
    for statement in changes:
        sql(statement)
        time.sleep(1.1)  # Distinct millisecond timestamps, separate DB transactions.
    rows = wait_for(lambda r: len(r) >= 7)
    assert len(rows) == 7, rows
    account = [r for r in rows if r["account_id"] == 1001]
    assert len(account) == 4 and account[-1]["status"] == "blocked"
    assert [r["owner"] for r in account] == ["Мария Соколова"] + ["Наталья Волкова"] * 3
    assert sum(r["is_current"] and not r["is_deleted"] for r in rows) == 2
    for key in {r["account_id"] for r in rows}:
        versions = [r for r in rows if r["account_id"] == key]
        assert sum(r["is_current"] for r in versions) == 1
        for before, after in zip(versions, versions[1:]):
            assert before["valid_to_ms"] == after["valid_from_ms"]
    at = original["valid_from"]
    past = json.loads(command("docker", "compose", "exec", "-T", "worker", "python", "main.py",
                              "show", "--account", "1001", "--at", at))
    assert len(past) == 1 and past[0]["owner"] == "Мария Соколова"
    print("PASS: real CDC -> 7 versions; no-op, delete, current and historical query checked")
    print(json.dumps(rows, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
