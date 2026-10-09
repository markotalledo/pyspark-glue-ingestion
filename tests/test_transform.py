from datetime import date

from conftest import event, write_raw
from pyspark.sql import functions as F

from clean_events.transform import arrival_dates, build


def rows(df, *cols):
    return sorted(tuple(r[c] for c in cols) for r in df.collect())


def test_arrival_window_is_inclusive():
    assert arrival_dates(date(2026, 1, 3), 2) == [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)]


def test_duplicate_across_arrival_days_keeps_first_arrival(spark, tmp_path):
    write_raw(tmp_path, "2026-01-01", [event("e1", "2026-01-01T10:00:00.000Z", "2026-01-01T10:00:01.000Z")])
    write_raw(tmp_path, "2026-01-02", [event("e1", "2026-01-01T10:00:00.000Z", "2026-01-02T08:00:00.000Z")])
    events = build(spark, str(tmp_path), date(2026, 1, 2), 1)["events"]
    assert events.count() == 1
    # Format inside Spark: collect() would convert to the machine's local time zone.
    kept = events.select(F.date_format("received_at", "yyyy-MM-dd HH:mm:ss").alias("r")).first()["r"]
    assert kept == "2026-01-01 10:00:01"


def test_late_event_lands_in_the_date_it_happened(spark, tmp_path):
    write_raw(tmp_path, "2026-01-02", [event("late", "2026-01-01T23:30:00.000Z", "2026-01-02T05:30:00.000Z")])
    events = build(spark, str(tmp_path), date(2026, 1, 2), 1)["events"]
    assert rows(events, "event_id", "event_date", "arrival_lag_hours") == [("late", date(2026, 1, 1), 6.0)]


def test_bad_rows_are_quarantined_with_a_reason(spark, tmp_path):
    write_raw(
        tmp_path,
        "2026-01-01",
        [
            event("ok", "2026-01-01T10:00:00.000Z", "2026-01-01T10:00:01.000Z"),
            event("bad_ts", "yesterday", "2026-01-01T10:00:01.000Z"),
            '{"event_id": "broken", ',
        ],
    )
    out = build(spark, str(tmp_path), date(2026, 1, 1), 0)
    assert rows(out["events"], "event_id") == [("ok",)]
    assert rows(out["rejected"], "reject_reason") == [("bad_occurred_at",), ("corrupt_json",)]


def test_events_older_than_the_window_are_not_rewritten(spark, tmp_path):
    # Arrived inside the window but happened before it: its date is not fully read, so skip it.
    write_raw(tmp_path, "2026-01-05", [event("old", "2026-01-01T10:00:00.000Z", "2026-01-05T00:00:00.000Z")])
    assert build(spark, str(tmp_path), date(2026, 1, 5), 2)["events"].count() == 0


def test_order_items_are_exploded(spark, tmp_path):
    items = [
        {"product_id": "p1", "quantity": 2, "price_cents": 500},
        {"product_id": "p2", "quantity": 1, "price_cents": 1200},
    ]
    order = event(
        "o",
        "2026-01-01T10:00:00.000Z",
        "2026-01-01T10:00:01.000Z",
        name="order_completed",
        order_id="o_1",
        items=items,
        total_cents=2200,
    )
    write_raw(tmp_path, "2026-01-01", [order])
    lines = build(spark, str(tmp_path), date(2026, 1, 1), 0)["order_items"]
    assert rows(lines, "order_id", "line_number", "line_total_cents") == [("o_1", 1, 1000), ("o_1", 2, 1200)]


def test_missing_arrival_days_do_not_fail(spark, tmp_path):
    assert build(spark, str(tmp_path), date(2026, 1, 1), 3) == {}
