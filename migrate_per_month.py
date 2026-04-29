#!/usr/bin/env python3
"""migrate_per_month.py — one-shot Redis schema migration for MT5Node.

Builds per-month indexes for:
  - closed_positions  (key:  closed_position:{ticket}, time field: close_time)
  - deposits_withdrawals (key: deposit_withdrawal:{ticket}, time field: time)

starting from the flat tickets SETs that older versions of MT5Node wrote.

Target schema (after migration):

    closed_positions:tickets             SET   global ticket index (unchanged)
    closed_position:{ticket}             STR   per-record JSON (unchanged)
    closed_positions:month:{YYYY-MM}     SET   tickets closed in that month  (NEW)
    closed_positions:month_meta:{YYYY-MM} HASH {month, count, last_update, synced=1} (NEW)
    closed_positions:months:synced       SET   months that are fully indexed (NEW)
    closed_positions:synced_migrated     STR   "1" once migration done       (marker)

    deposits_withdrawals:tickets             SET   global ticket index (unchanged)
    deposit_withdrawal:{ticket}              STR   per-record JSON (unchanged)
    deposits_withdrawals:month:{YYYY-MM}     SET   tickets in that month        (NEW)
    deposits_withdrawals:month_meta:{YYYY-MM} HASH {month, count, last_update, synced=1} (NEW)
    deposits_withdrawals:months:synced       SET   months that are fully indexed (NEW)
    deposits_withdrawals:migrated            STR   "1" once migration done       (marker)

Idempotent — safe to re-run. If a marker is set, the section is skipped unless
--force is passed. The migration only ADDS data; it never deletes the global
tickets SET or the per-record keys.

Why this exists
---------------
MT5-Elise's CLI runner (run.py) now calls /closed_positions/months/check and
/deposits_withdrawals/months/check to figure out which months are missing on
MT5Node, and only fetches those from MT5. Without per-month indexes every
month looks "missing" and the CLI re-fetches 14 years on every cycle.

Running
-------
  # Inspect what would happen without writing
  python migrate_per_month.py --dry-run

  # Migrate both tables
  python migrate_per_month.py

  # Just one
  python migrate_per_month.py --only closed_positions
  python migrate_per_month.py --only deposits_withdrawals

  # Force re-run even if marker is set (rebuilds the index from scratch)
  python migrate_per_month.py --force

Connects to Redis at REDIS_HOST:REDIS_PORT (env vars; defaults 127.0.0.1:6379).
Override with --host / --port flags."""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

try:
    import redis
except ImportError:
    print("ERROR: 'redis' package not installed. Run: pip install redis",
          file=sys.stderr)
    sys.exit(1)


REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))


def _month_key_from_ts(ts: int) -> str:
    d = datetime.fromtimestamp(ts, tz=timezone.utc)
    return f"{d.year}-{d.month:02d}"


def migrate_table(
    r: "redis.Redis",
    *,
    label: str,
    tickets_set: str,
    record_prefix: str,
    month_prefix: str,
    synced_set: str,
    meta_prefix: str,
    marker_key: str,
    time_field: str,
    dry_run: bool,
    force: bool,
    batch_size: int = 1000,
):
    """Walk every ticket in `tickets_set`, read its `time_field`, bucket by
    UTC YYYY-MM, and write the per-month SETs + meta + synced set."""
    print(f"\n=== {label} ===")

    if not force and r.get(marker_key):
        print(f"  Marker {marker_key!r} is set — skipping (pass --force to re-run).")
        return

    total = r.scard(tickets_set)
    if total == 0:
        print(f"  Source SET {tickets_set!r} is empty — nothing to migrate.")
        if not dry_run:
            r.set(marker_key, "1")
        return

    print(f"  Source: {tickets_set} contains {total:,} tickets")
    print(f"  Reading records (pipelined, batch={batch_size})…")

    months: dict[str, list[str]] = {}
    seen = 0
    skipped_missing = 0
    skipped_no_time = 0
    t0 = time.time()

    cursor = 0
    while True:
        cursor, batch = r.sscan(tickets_set, cursor, count=batch_size)
        if batch:
            pipe = r.pipeline()
            for t in batch:
                pipe.get(f"{record_prefix}{t}")
            records = pipe.execute()

            for t, raw in zip(batch, records):
                seen += 1
                if not raw:
                    skipped_missing += 1
                    continue
                try:
                    rec = json.loads(raw)
                    ts = int(rec.get(time_field, 0) or 0)
                except Exception:
                    skipped_no_time += 1
                    continue
                if ts <= 0:
                    skipped_no_time += 1
                    continue
                mk = _month_key_from_ts(ts)
                months.setdefault(mk, []).append(t)

            if seen % 100_000 < batch_size:
                rate = seen / max(1.0, time.time() - t0)
                print(f"    progress: {seen:,}/{total:,}  "
                      f"({rate:,.0f}/s)  months found: {len(months)}")

        if cursor == 0:
            break

    elapsed = time.time() - t0
    print(f"  Scan done: {seen:,} tickets in {elapsed:.1f}s "
          f"({seen / max(1.0, elapsed):,.0f}/s)")
    print(f"  Months found: {len(months)}")
    if skipped_missing:
        print(f"  WARN: {skipped_missing} tickets had no record at "
              f"{record_prefix}{{ticket}} (orphans)")
    if skipped_no_time:
        print(f"  WARN: {skipped_no_time} records had no usable {time_field!r}")

    if dry_run:
        print(f"  DRY RUN — would write {len(months)} per-month SETs + meta + synced")
        sample = sorted(months.items(), key=lambda kv: kv[0])[:5]
        for mk, ts_list in sample:
            print(f"    {month_prefix}{mk}: {len(ts_list):,} tickets")
        if len(months) > 5:
            print(f"    … and {len(months) - 5} more months")
        return

    print(f"  Writing per-month indexes…")
    now = datetime.now(timezone.utc).isoformat()
    written = 0
    write_t0 = time.time()
    for mk, ts_list in sorted(months.items()):
        # Big SADD broken into chunks to avoid one giant command.
        for i in range(0, len(ts_list), 1000):
            chunk = ts_list[i:i + 1000]
            r.sadd(f"{month_prefix}{mk}", *chunk)
        r.hset(f"{meta_prefix}{mk}", mapping={
            "month": mk,
            "count": len(ts_list),
            "last_update": now,
            "synced": 1,
        })
        r.sadd(synced_set, mk)
        written += 1
        if written % 20 == 0:
            print(f"    {written}/{len(months)} months written")

    r.set(marker_key, "1")
    print(f"  DONE — {len(months)} months indexed in "
          f"{time.time() - write_t0:.1f}s; marker {marker_key!r} set")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute and report counts without writing.")
    parser.add_argument("--only", choices=["closed_positions", "deposits_withdrawals"],
                        help="Migrate only the named table (default: both).")
    parser.add_argument("--force", action="store_true",
                        help="Re-run even if the migration marker is already set.")
    parser.add_argument("--host", default=REDIS_HOST,
                        help=f"Redis host (default: {REDIS_HOST})")
    parser.add_argument("--port", type=int, default=REDIS_PORT,
                        help=f"Redis port (default: {REDIS_PORT})")
    parser.add_argument("--db", type=int, default=0,
                        help="Redis db index (default: 0)")
    args = parser.parse_args()

    print(f"Connecting to Redis at {args.host}:{args.port} db={args.db}…")
    r = redis.Redis(host=args.host, port=args.port, db=args.db,
                    decode_responses=True)
    r.ping()
    print(f"  Connected. Total keys in db: {r.dbsize():,}")

    if args.only in (None, "closed_positions"):
        migrate_table(
            r,
            label="closed_positions",
            tickets_set="closed_positions:tickets",
            record_prefix="closed_position:",
            month_prefix="closed_positions:month:",
            synced_set="closed_positions:months:synced",
            meta_prefix="closed_positions:month_meta:",
            marker_key="closed_positions:synced_migrated",
            time_field="close_time",
            dry_run=args.dry_run,
            force=args.force,
        )

    if args.only in (None, "deposits_withdrawals"):
        migrate_table(
            r,
            label="deposits_withdrawals",
            tickets_set="deposits_withdrawals:tickets",
            record_prefix="deposit_withdrawal:",
            month_prefix="deposits_withdrawals:month:",
            synced_set="deposits_withdrawals:months:synced",
            meta_prefix="deposits_withdrawals:month_meta:",
            marker_key="deposits_withdrawals:migrated",
            time_field="time",
            dry_run=args.dry_run,
            force=args.force,
        )

    print("\nAll done.")


if __name__ == "__main__":
    main()
