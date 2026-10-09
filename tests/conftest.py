import gzip
import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

FIXTURE_RAW = Path(__file__).parent / "fixtures" / "raw"


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def event(event_id, occurred, received, name="product_viewed", **props):
    return {
        "event_id": event_id,
        "event_name": name,
        "occurred_at": occurred,
        "sent_at": occurred,
        "received_at": received,
        "anonymous_id": "a1",
        "customer_id": None,
        "session_id": "s1",
        "source": "ios",
        "properties": props,
    }


def write_raw(root: Path, arrival_date: str, lines: list) -> None:
    folder = root / f"dt={arrival_date}" / "hour=00"
    folder.mkdir(parents=True, exist_ok=True)
    n = len(list(folder.iterdir()))
    with gzip.open(folder / f"part-{n}.ndjson.gz", "wt") as f:
        for line in lines:
            f.write(line if isinstance(line, str) else json.dumps(line))
            f.write("\n")
