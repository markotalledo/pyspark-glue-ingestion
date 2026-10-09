from conftest import FIXTURE_RAW
from pyspark.sql import functions as F

from clean_events.job import main

ARGS = ["--raw_path", str(FIXTURE_RAW), "--run_date", "2026-01-03", "--lookback_days", "2"]


def test_fixture_end_to_end_and_idempotent(spark, tmp_path):
    args = ARGS + ["--clean_path", str(tmp_path)]
    first = main(args, spark)
    second = main(args, spark)
    assert first == second  # rerunning the same window rewrites, never appends

    events = spark.read.parquet(str(tmp_path / "events"))
    assert events.count() == first["events"]
    assert events.groupBy("event_id").count().filter("count > 1").count() == 0
    # Sessions that start just before midnight finish on the next day.
    dates = {r.event_date.isoformat() for r in events.select("event_date").distinct().collect()}
    assert dates == {"2026-01-01", "2026-01-02"}
    assert events.filter(F.col("arrival_lag_hours") > 1).count() > 0  # late mobile events survived


def test_every_order_total_matches_its_lines(spark, tmp_path):
    main(ARGS + ["--clean_path", str(tmp_path)], spark)
    events = spark.read.parquet(str(tmp_path / "events"))
    lines = spark.read.parquet(str(tmp_path / "order_items"))
    totals = events.filter("event_name = 'order_completed'").select("order_id", "total_cents")
    summed = lines.groupBy("order_id").agg(F.sum("line_total_cents").alias("total_cents"))
    assert totals.count() > 0
    assert totals.exceptAll(summed).count() == 0 and summed.exceptAll(totals).count() == 0
