"""Glue entrypoint (also runs locally with spark-submit or python -m).

python -m clean_events.job --raw_path data/raw --clean_path data/clean --run_date 2026-01-03
"""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime

from pyspark.sql import SparkSession

from clean_events.transform import build, write


def spark_session(app_name: str = "clean_events") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw_path", required=True, help="root of events/, e.g. s3://bucket/events")
    parser.add_argument("--clean_path", required=True)
    parser.add_argument("--run_date", type=date.fromisoformat, default=None)
    parser.add_argument("--lookback_days", type=int, default=3)
    # Glue adds its own arguments (--JOB_NAME, --job-bookmark-option, ...): ignore them.
    args, _ = parser.parse_known_args(argv)
    if args.run_date is None:
        args.run_date = datetime.now(UTC).date()
    return args


def main(argv=None, spark: SparkSession | None = None) -> dict[str, int]:
    args = parse_args(argv)
    spark = spark or spark_session()
    tables = build(spark, args.raw_path.rstrip("/"), args.run_date, args.lookback_days)
    if not tables:
        print(f"no raw data for {args.run_date} with lookback {args.lookback_days}")
        return {}
    clean = args.clean_path.rstrip("/")
    write(tables["events"], f"{clean}/events", "event_date")
    write(tables["order_items"], f"{clean}/order_items", "event_date")
    write(tables["rejected"], f"{clean}/rejected", "run_date")
    counts = {name: df.count() for name, df in tables.items()}
    print(counts)
    return counts


if __name__ == "__main__":
    main()
