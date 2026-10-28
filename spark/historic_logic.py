"""
HAIS (Kystverket historic AIS) -> Bronze envelope, as small testable DataFrame functions.
Rows look exactly like live Bronze rows (_source, _ingest_ts, _run_id, _file, payload JSON text), so Silver's
parse/standardize/validate/dedupe/enrich run unchanged. Needs spark.sql.session.timeZone=UTC (msgtime is built as UTC text).
"""
from pyspark.sql import DataFrame, functions as F

SOURCE = "kystverket_hais_v1"
HAIS_COLUMNS = ["date_time_utc", "mmsi", "latitude", "longitude", "status", "course_over_ground",
                "true_heading", "speed_over_ground", "rate_of_turn", "ais_class"]   # geometry is never read
STATIC_TYPES = [("name", "string"), ("shipType", "int"), ("callSign", "string"),
                ("imoNumber", "long"), ("shipLength", "int"), ("shipWidth", "int")]


def load_static(spark, silver_path: str) -> DataFrame:
    """Vessel master data (HAIS position files have none): one row per MMSI taken from existing live Silver."""
    s = spark.read.parquet(silver_path)
    agg = [F.first(c, ignorenulls=True).alias(c) for c in ["name", "shipType", "callSign", "shipLength", "shipWidth"]]
    agg.append(F.first("imo", ignorenulls=True).alias("imoNumber"))
    return s.groupBy(F.col("mmsi").cast("long").alias("mmsi")).agg(*agg)


def to_envelope(raw: DataFrame, file_name: str, run_id: str, ingest_ts: str, static: DataFrame = None) -> DataFrame:
    h = raw.select(*HAIS_COLUMNS).withColumn("mmsi", F.col("mmsi").cast("long"))
    if static is not None:
        h = h.join(F.broadcast(static), "mmsi", "left")
    else:
        for c, t in STATIC_TYPES:
            h = h.withColumn(c, F.lit(None).cast(t))
    msgtime = F.concat(F.date_format("date_time_utc", "yyyy-MM-dd'T'HH:mm:ss"), F.lit("+00:00"))
    payload = F.to_json(F.struct(
        F.col("mmsi"), msgtime.alias("msgtime"), F.col("latitude"), F.col("longitude"),
        F.col("speed_over_ground").cast("double").alias("speedOverGround"),
        F.col("course_over_ground").cast("double").alias("courseOverGround"),
        F.col("true_heading").cast("int").alias("trueHeading"),
        F.col("rate_of_turn").cast("double").alias("rateOfTurn"),
        F.col("status").cast("int").alias("navigationalStatus"),
        F.col("ais_class").alias("reportClass"),
        F.col("name"), F.col("shipType"), F.col("callSign"), F.col("imoNumber"),
        F.col("shipLength"), F.col("shipWidth")))
    return h.select(F.lit(SOURCE).alias("_source"), F.lit(ingest_ts).alias("_ingest_ts"),
                    F.lit(run_id).alias("_run_id"), F.lit(file_name).alias("_file"), payload.alias("payload"),
                    F.date_format("date_time_utc", "yyyy-MM-dd").alias("dt"),
                    F.date_format("date_time_utc", "HH").alias("hour"))
