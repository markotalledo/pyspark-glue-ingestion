"""PySpark transformations for the raw event stream.

The raw layer is partitioned by arrival date (that is all Firehose can do), so a
single event date is spread across several arrival partitions: late mobile events
show up hours or days after they happened. To rebuild one event date correctly you
have to read every arrival partition that can contain it. That is what the lookback
window does.
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSX"

ITEM = T.StructType(
    [
        T.StructField("product_id", T.StringType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("price_cents", T.LongType()),
    ]
)

PROPERTIES = T.StructType(
    [
        T.StructField("product_id", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("price_cents", T.LongType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("items", T.ArrayType(ITEM)),
        T.StructField("cart_value_cents", T.LongType()),
        T.StructField("order_id", T.StringType()),
        T.StructField("total_cents", T.LongType()),
        T.StructField("currency", T.StringType()),
        T.StructField("amount_cents", T.LongType()),
        T.StructField("method", T.StringType()),
    ]
)

# Explicit schema: no inference pass over the data, and a new field in the source
# cannot silently change a column type.
RAW_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("event_name", T.StringType()),
        T.StructField("occurred_at", T.StringType()),
        T.StructField("sent_at", T.StringType()),
        T.StructField("received_at", T.StringType()),
        T.StructField("anonymous_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("session_id", T.StringType()),
        T.StructField("source", T.StringType()),
        T.StructField("properties", PROPERTIES),
        T.StructField("_corrupt_record", T.StringType()),
    ]
)


def arrival_dates(run_date: date, lookback_days: int) -> list[date]:
    return [run_date - timedelta(days=n) for n in range(lookback_days, -1, -1)]


def existing_paths(spark: SparkSession, paths: list[str]) -> list[str]:
    """Keep only paths that exist. A day with no traffic should not fail the job."""
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    keep = []
    for p in paths:
        path = jvm.org.apache.hadoop.fs.Path(p)
        if path.getFileSystem(conf).exists(path):
            keep.append(p)
    return keep


def read_raw(spark: SparkSession, paths: list[str]) -> DataFrame:
    return (
        spark.read.schema(RAW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("recursiveFileLookup", "true")  # hour=HH folders are not partitions here
        .json(paths)
    )


def split_valid(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Parse timestamps and route bad rows to a quarantine frame with a reason."""
    parsed = (
        raw.withColumn("occurred_at_ts", F.to_timestamp("occurred_at", TS_FORMAT))
        .withColumn("sent_at_ts", F.to_timestamp("sent_at", TS_FORMAT))
        .withColumn("received_at_ts", F.to_timestamp("received_at", TS_FORMAT))
    )
    reason = (
        F.when(F.col("_corrupt_record").isNotNull(), "corrupt_json")
        .when(F.col("event_id").isNull(), "missing_event_id")
        .when(F.col("occurred_at_ts").isNull(), "bad_occurred_at")
        .when(F.col("received_at_ts").isNull(), "missing_received_at")
    )
    tagged = parsed.withColumn("reject_reason", reason)
    rejected = tagged.filter(F.col("reject_reason").isNotNull()).select(
        "reject_reason", "event_id", "event_name", "_corrupt_record"
    )
    valid = (
        tagged.filter(F.col("reject_reason").isNull())
        .drop("reject_reason", "_corrupt_record")
        .withColumn("event_date", F.to_date("occurred_at_ts"))
    )
    return valid, rejected


def dedupe(df: DataFrame) -> DataFrame:
    """One row per event_id, keeping the first copy that arrived."""
    first_arrival = Window.partitionBy("event_id").orderBy(
        F.col("received_at_ts").asc(), F.col("sent_at_ts").asc_nulls_last()
    )
    return df.withColumn("_rn", F.row_number().over(first_arrival)).filter("_rn = 1").drop("_rn")


def to_events(df: DataFrame) -> DataFrame:
    lag_hours = (F.unix_timestamp("received_at_ts") - F.unix_timestamp("occurred_at_ts")) / 3600
    return df.select(
        "event_id",
        "event_name",
        F.col("occurred_at_ts").alias("occurred_at"),
        F.col("sent_at_ts").alias("sent_at"),
        F.col("received_at_ts").alias("received_at"),
        F.round(lag_hours, 2).alias("arrival_lag_hours"),
        "anonymous_id",
        "customer_id",
        "session_id",
        "source",
        F.col("properties.product_id").alias("product_id"),
        F.col("properties.category").alias("category"),
        F.col("properties.price_cents").alias("price_cents"),
        F.col("properties.quantity").alias("quantity"),
        F.col("properties.cart_value_cents").alias("cart_value_cents"),
        F.col("properties.order_id").alias("order_id"),
        F.col("properties.total_cents").alias("total_cents"),
        F.col("properties.currency").alias("currency"),
        F.col("properties.amount_cents").alias("amount_cents"),
        F.col("properties.method").alias("payment_method"),
        "event_date",
    )


def to_order_items(df: DataFrame) -> DataFrame:
    """One row per order line, exploded from order_completed events."""
    orders = df.filter(F.col("event_name") == "order_completed")
    lines = orders.select(
        F.col("properties.order_id").alias("order_id"),
        "customer_id",
        F.col("occurred_at_ts").alias("ordered_at"),
        "event_date",
        F.posexplode("properties.items").alias("line_number", "item"),
    )
    return lines.select(
        "order_id",
        (F.col("line_number") + 1).alias("line_number"),
        "customer_id",
        "ordered_at",
        F.col("item.product_id").alias("product_id"),
        F.col("item.quantity").alias("quantity"),
        F.col("item.price_cents").alias("unit_price_cents"),
        (F.col("item.quantity") * F.col("item.price_cents")).alias("line_total_cents"),
        "event_date",
    )


def build(spark: SparkSession, raw_root: str, run_date: date, lookback_days: int) -> dict[str, DataFrame]:
    """Rebuild every event date in [run_date - lookback_days, run_date].

    Correct as long as lookback_days covers the maximum arrival lag: every arrival
    partition that can hold events of a rebuilt date is read, so each output
    partition is rebuilt from all of its data, not appended to.
    """
    paths = [f"{raw_root}/dt={d.isoformat()}/" for d in arrival_dates(run_date, lookback_days)]
    paths = existing_paths(spark, paths)
    if not paths:
        return {}
    raw = read_raw(spark, paths).cache()  # Spark needs a cache to query _corrupt_record
    valid, rejected = split_valid(raw)
    oldest = run_date - timedelta(days=lookback_days)
    # Filter before deduplicating: copies of an event share occurred_at, so the result is
    # the same, and the shuffle on event_id moves only rows that will be kept.
    in_window = dedupe(valid.filter(F.col("event_date").between(F.lit(oldest), F.lit(run_date))))
    return {
        "events": to_events(in_window),
        "order_items": to_order_items(in_window),
        "rejected": rejected.withColumn("run_date", F.lit(run_date)),
    }


def write(df: DataFrame, path: str, partition_col: str) -> None:
    """Overwrite only the partitions present in df (dynamic partition overwrite).

    Repartitioning by the partition column first gives one file per date instead of
    one file per task per date.
    """
    (df.repartition(partition_col).write.mode("overwrite").partitionBy(partition_col).parquet(path))
