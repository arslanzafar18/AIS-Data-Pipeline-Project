import json
from decimal import Decimal
from pyspark.sql import functions as F, types as T
from spark import historic_logic as H, silver_logic as L

SCHEMA = T.StructType([
    T.StructField("ts", T.StringType()), T.StructField("mmsi", T.IntegerType()),
    T.StructField("longitude", T.DoubleType()), T.StructField("latitude", T.DoubleType()),
    T.StructField("status", T.ByteType()), T.StructField("course_over_ground", T.DecimalType(6, 2)),
    T.StructField("true_heading", T.ShortType()), T.StructField("speed_over_ground", T.DecimalType(6, 2)),
    T.StructField("rate_of_turn", T.ByteType()), T.StructField("ais_class", T.StringType())])


def hais(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    rows = [("2026-10-16 00:00:04", 257793000, 3.394307, 56.273533, 0, Decimal("282.10"), 168, Decimal("0.50"), 0, "A"),
            ("2026-10-16 00:00:08", 219468000, 3.250072, 56.539921, 15, Decimal("225.90"), 511, Decimal("102.30"), -128, "A")]
    return spark.createDataFrame(rows, SCHEMA).withColumn("date_time_utc", F.col("ts").cast("timestamp")).drop("ts")


def test_envelope_has_live_shape(spark):
    env = H.to_envelope(hais(spark), "hais_2026-10-16.snappy.parquet", "run1", "2026-10-27T10:00:00+00:00")
    assert set(env.columns) == {"_source", "_ingest_ts", "_run_id", "_file", "payload", "dt", "hour"}
    r = env.orderBy("dt").first()
    p = json.loads(r["payload"])
    assert p["msgtime"] == "2026-10-16T00:00:04+00:00" and p["mmsi"] == 257793000
    assert (r["dt"], r["hour"]) == ("2026-10-16", "00")
    assert "name" not in p and "shipType" not in p        # no static data without a lookup


def test_envelope_passes_silver_unchanged(spark):
    env = H.to_envelope(hais(spark), "hais_2026-10-16.snappy.parquet", "run1", "2026-10-27T10:00:00+00:00")
    s = L.validate(L.standardize(L.parse(env)))
    rows = s.select(F.date_format("event_ts", "yyyy-MM-dd HH:mm:ss").alias("t"), "mmsi", "reject_reason",
                    "speedOverGround", "trueHeading", "rateOfTurn").orderBy("mmsi").collect()
    assert all(r["reject_reason"] is None for r in rows)
    assert rows[1]["t"] == "2026-10-16 00:00:04" and rows[1]["speedOverGround"] == 0.5
    assert rows[0]["speedOverGround"] is None and rows[0]["trueHeading"] is None   # 102.3 / 511 sentinels -> null


def test_overlap_with_live_dedupes(spark):
    env = H.to_envelope(hais(spark), "hais_x", "run1", "2026-10-27T10:00:00+00:00")
    twice = env.unionByName(env.withColumn("_file", F.lit("ais_live")).withColumn("_ingest_ts", F.lit("2026-10-16T00:05:00+00:00")))
    v = L.validate(L.standardize(L.parse(twice)))
    assert L.dedupe(v).count() == 2
