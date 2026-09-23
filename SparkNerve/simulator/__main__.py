"""Drive the SQL Server RetailDB from the command line (used by the Docker stack).

    python -m simulator seed
    python -m simulator tick --changes 30 --bad 1
    python -m simulator bad-records --count 6
    python -m simulator bad-batch --count 24
    python -m simulator add-column --table customers --column loyalty_tier --type "VARCHAR(10)"
"""

from __future__ import annotations

import argparse
import time

from simulator.retail import MssqlBackend, RetailDB


def connect(retries: int = 30, wait_seconds: float = 5.0) -> RetailDB:
    last_error: Exception | None = None
    for _ in range(retries):
        try:
            return RetailDB(MssqlBackend.from_env(), seed=int(time.time()))
        except Exception as exc:  # noqa: BLE001 - SQL Server may still be starting
            last_error = exc
            time.sleep(wait_seconds)
    raise RuntimeError(f"Could not connect to SQL Server: {last_error}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m simulator", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("seed", help="create RetailDB and load the initial data (idempotent)")
    tick = sub.add_parser("tick", help="apply a burst of realistic changes")
    tick.add_argument("--changes", type=int, default=30)
    tick.add_argument("--bad", type=int, default=1, help="defective rows to mix in")
    bad = sub.add_parser("bad-records", help="insert rows that break data quality rules")
    bad.add_argument("--count", type=int, default=6)
    burst = sub.add_parser("bad-batch", help="insert a mostly-invalid burst (trips the circuit breaker)")
    burst.add_argument("--count", type=int, default=24)
    add = sub.add_parser("add-column", help="ALTER TABLE ... ADD (schema drift)")
    add.add_argument("--table", required=True)
    add.add_argument("--column", required=True)
    add.add_argument("--type", required=True)
    drop = sub.add_parser("drop-column", help="ALTER TABLE ... DROP COLUMN")
    drop.add_argument("--table", required=True)
    drop.add_argument("--column", required=True)
    args = parser.parse_args(argv)

    db = connect()
    if args.command == "seed":
        messages = [db.seed()]
    elif args.command == "tick":
        db.create_schema()
        messages = db.business_as_usual(args.changes) + db.inject_bad_records(args.bad)
    elif args.command == "bad-records":
        messages = db.inject_bad_records(args.count)
    elif args.command == "bad-batch":
        messages = db.bad_batch(args.count)
    elif args.command == "add-column":
        messages = [db.add_column(args.table, args.column, args.type)]
    else:
        messages = [db.drop_column(args.table, args.column)]
    for message in messages:
        print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
