"""
Silver transformation logic as small, testable DataFrame functions (PySpark DataFrame / Spark SQL APIs).

Grain: ONE validated position report per (mmsi, event_ts).
Essential: mmsi, msgtime, latitude, longitude.  Optional: name, shipType, destination, ...
Unreliable (self-reported / sentinel-prone): trueHeading (511), rateOfTurn (-128), speedOverGround (102.3),
shipType (0), name (blank), destination (free text).
"""
from pyspark.sql import DataFrame, Window, functions as F, types as T
from spark.common import haversine_m

PAYLOAD_SCHEMA = T.StructType([
    T.StructField("mmsi", T.LongType()), T.StructField("msgtime", T.StringType()),
    T.StructField("latitude", T.DoubleType()), T.StructField("longitude", T.DoubleType()),
    T.StructField("speedOverGround", T.DoubleType()), T.StructField("courseOverGround", T.DoubleType()),
    T.StructField("trueHeading", T.IntegerType()), T.StructField("rateOfTurn", T.DoubleType()),
    T.StructField("name", T.StringType()), T.StructField("shipType", T.IntegerType()),
    T.StructField("navigationalStatus", T.IntegerType()), T.StructField("destination", T.StringType()),
    T.StructField("callSign", T.StringType()), T.StructField("imo", T.LongType()),
    T.StructField("shipLength", T.IntegerType()), T.StructField("shipWidth", T.IntegerType()),
])
BRONZE_SCHEMA = T.StructType([T.StructField(n, T.StringType()) for n in
                              ["_source", "_ingest_ts", "_run_id", "_file", "payload"]])
REJECT_REASONS = ["parse_failed", "bad_mmsi", "bad_timestamp", "missing_position",
                  "sentinel_position", "out_of_range", "future_timestamp"]


def parse(bronze: DataFrame) -> DataFrame:
    """Raw payload text -> typed columns. Malformed JSON gives parse_failed=True (kept, never dropped silently).
    from_json is permissive and may half-parse truncated JSON, so a corrupt-record column decides parse_failed."""
    names = [f.name for f in PAYLOAD_SCHEMA.fields]
    ext = T.StructType(PAYLOAD_SCHEMA.fields + [T.StructField("_corrupt", T.StringType())])
    j = F.from_json("payload", ext, {"columnNameOfCorruptRecord": "_corrupt"})
    failed = j.isNull() | j["_corrupt"].isNotNull()
    keys = F.map_keys(F.from_json("payload", "map<string,string>"))
    drift = F.coalesce(F.size(F.array_except(keys, F.array(*[F.lit(n) for n in names]))) > 0, F.lit(False)) & ~failed
    cols = [j[n].alias(n) for n in names]
    return bronze.select("_run_id", "_file", "_ingest_ts", "payload", drift.alias("unknown_fields"),
                         failed.alias("parse_failed"), *cols)


def standardize(p: DataFrame) -> DataFrame:
    """Consistent types, UTC timestamps, units (knots, degrees), sentinel values -> null (counted as corrections)."""
    sog_bad = F.col("speedOverGround").isNotNull() & ((F.col("speedOverGround") >= 102.3) | (F.col("speedOverGround") < 0))
    return (p.withColumn("event_ts", F.to_timestamp("msgtime"))
             .withColumn("ingest_ts", F.to_timestamp("_ingest_ts"))
             .withColumn("latency_s", F.col("ingest_ts").cast("long") - F.col("event_ts").cast("long"))
             .withColumn("fix_sog", sog_bad)
             .withColumn("fix_heading", F.col("trueHeading") == 511)
             .withColumn("fix_rot", F.col("rateOfTurn") <= -128)
             .withColumn("fix_shiptype", F.col("shipType") == 0)
             .withColumn("speedOverGround", F.when(F.col("fix_sog"), None).otherwise(F.col("speedOverGround")))
             .withColumn("trueHeading", F.when(F.col("fix_heading"), None).otherwise(F.col("trueHeading")))
             .withColumn("rateOfTurn", F.when(F.col("fix_rot"), None).otherwise(F.col("rateOfTurn")))
             .withColumn("shipType", F.when(F.col("fix_shiptype"), None).otherwise(F.col("shipType")))
             .withColumn("name", F.when(F.trim("name") == "", None).otherwise(F.upper(F.trim("name")))))


def validate(s: DataFrame) -> DataFrame:
    """Adds reject_reason (first failing rule; null = accepted). Rejected rows are kept separately, not corrected."""
    lat, lon = F.col("latitude"), F.col("longitude")
    reason = (F.when(F.col("parse_failed"), "parse_failed")
               .when(F.col("mmsi").isNull() | (F.col("mmsi") < 100000000) | (F.col("mmsi") > 999999999), "bad_mmsi")
               .when(F.col("event_ts").isNull(), "bad_timestamp")
               .when(lat.isNull() | lon.isNull(), "missing_position")
               .when((lat == 91) | (lon == 181), "sentinel_position")
               .when(~lat.between(-90, 90) | ~lon.between(-180, 180), "out_of_range")
               .when(F.col("ingest_ts").isNotNull() & (F.col("latency_s") < -300), "future_timestamp"))
    return s.withColumn("reject_reason", reason)


def dedupe(v: DataFrame) -> DataFrame:
    """Exact duplicates on the grain key; keep the earliest-ingested copy (deterministic)."""
    w = Window.partitionBy("mmsi", "event_ts").orderBy(F.col("ingest_ts").asc_nulls_last(), "_file")
    return v.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def enrich(v: DataFrame, fac: DataFrame, near_m: float, at_m: float, stationary_kn: float) -> DataFrame:
    """Nearest installation within near_m. Bounding-box prefilter before exact haversine (see benchmark)."""
    dlat = near_m / 111000.0 * 1.05
    dlon = dlat / 0.30          # cos(72 deg): safe up to 72 N
    j = (v.select("mmsi", "event_ts", "latitude", "longitude")
          .crossJoin(F.broadcast(fac))
          .where((F.abs(F.col("latitude") - F.col("i_lat")) < dlat) & (F.abs(F.col("longitude") - F.col("i_lon")) < dlon))
          .withColumn("dist_m", haversine_m(F.col("latitude"), F.col("longitude"), F.col("i_lat"), F.col("i_lon")))
          .where(F.col("dist_m") <= near_m))
    w = Window.partitionBy("mmsi", "event_ts").orderBy("dist_m")
    nearest = j.withColumn("rn", F.row_number().over(w)).where("rn = 1").select("mmsi", "event_ts", "inst_name", "inst_kind", "dist_m")
    return (v.join(nearest, ["mmsi", "event_ts"], "left")
             .withColumn("is_near_installation", F.col("inst_name").isNotNull())
             .withColumn("is_at_installation", F.coalesce(F.col("dist_m") <= at_m, F.lit(False)))
             .withColumn("is_stationary", F.col("speedOverGround") < stationary_kn)
             .withColumn("dt", F.to_date("event_ts")).withColumn("hour", F.hour("event_ts")))


SILVER_COLUMNS = ["mmsi", "event_ts", "ingest_ts", "latency_s", "latitude", "longitude", "speedOverGround",
                  "courseOverGround", "trueHeading", "rateOfTurn", "name", "shipType", "navigationalStatus",
                  "destination", "callSign", "imo", "shipLength", "shipWidth", "inst_name", "inst_kind", "dist_m",
                  "is_near_installation", "is_at_installation", "is_stationary", "_run_id", "_file", "dt", "hour"]


def quality_agg(flagged: DataFrame) -> dict:
    """Before-processing quality numbers over ALL parsed rows of the batch (one Spark action)."""
    S = lambda c: F.sum(c.cast("int"))
    exprs = [F.count("*").alias("input_rows"), S(F.col("parse_failed")).alias("parse_failed"),
             S(F.col("unknown_fields")).alias("rows_with_unknown_fields"),
             S(F.col("reject_reason").isNull()).alias("valid_rows"),
             S(F.col("reject_reason").isNotNull()).alias("rejected_rows"),
             S(F.col("fix_sog") & F.col("reject_reason").isNull()).alias("corrected_sog"),
             S(F.col("fix_heading") & F.col("reject_reason").isNull()).alias("corrected_heading"),
             S(F.col("fix_rot") & F.col("reject_reason").isNull()).alias("corrected_rot"),
             S(F.col("fix_shiptype") & F.col("reject_reason").isNull()).alias("corrected_shiptype"),
             S(F.col("name").isNull() & F.col("reject_reason").isNull()).alias("null_name"),
             F.percentile_approx("latency_s", [0.5, 0.95]).alias("lat_pcts")]
    exprs += [S(F.col("reject_reason") == r).alias(f"rej_{r}") for r in REJECT_REASONS]
    row = flagged.agg(*exprs).collect()[0].asDict()
    pc = row.pop("lat_pcts") or [None, None]
    row["latency_p50_s"], row["latency_p95_s"] = pc[0], pc[1]
    return {k: (int(v) if isinstance(v, (int, float)) and k not in ("latency_p50_s", "latency_p95_s") and v is not None else v)
            for k, v in row.items()}
