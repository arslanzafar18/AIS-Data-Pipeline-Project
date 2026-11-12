from pyspark.sql import DataFrame, Window, functions as F


def _onshore(df):
    """True if the installation is onshore; null for Silver written before the flag existed."""
    c = F.col("is_onshore").cast("int") if "is_onshore" in df.columns else F.lit(None).cast("int")
    return F.max(c).cast("boolean").alias("is_onshore")


def build_visits(near: DataFrame, gap_s: int) -> DataFrame:
    """One row per vessel visit to an installation; a silence longer than gap_s starts a new visit."""
    w = Window.partitionBy("mmsi", "inst_name").orderBy("event_ts")
    gap = F.col("event_ts").cast("long") - F.lag("event_ts").over(w).cast("long")
    s = (near.withColumn("new_visit", F.when(gap.isNull() | (gap > gap_s), 1).otherwise(0))
             .withColumn("visit_no", F.sum("new_visit").over(w)))
    return (s.groupBy("mmsi", "inst_name", "inst_kind", "visit_no")
             .agg(F.first("name", ignorenulls=True).alias("vessel_name"), _onshore(near), F.first("shipType", ignorenulls=True).alias("ship_type"),
                  F.min("event_ts").alias("arrival_ts"), F.max("event_ts").alias("departure_ts"),
                  F.min("dist_m").alias("min_dist_m"), F.count("*").alias("n_msgs"),
                  F.max(F.col("is_at_installation").cast("int")).alias("entered_zone"),
                  F.sum(F.col("is_at_installation").cast("int")).alias("msgs_in_zone"))
             .withColumn("duration_min", (F.col("departure_ts").cast("long") - F.col("arrival_ts").cast("long")) / 60.0))


def build_hourly(near: DataFrame) -> DataFrame:
    return (near.groupBy("inst_name", "inst_kind", F.date_trunc("hour", "event_ts").alias("hour_ts"))
                .agg(F.countDistinct("mmsi").alias("distinct_vessels"), _onshore(near), F.count("*").alias("position_msgs"),
                     F.sum(F.col("is_at_installation").cast("int")).alias("msgs_in_zone"),
                     F.avg("speedOverGround").alias("avg_sog_kn")))


def _latest(c, order="event_ts"):
    """Most recent non-null value of column c (AIS identity fields are often missing in single messages)."""
    return F.max_by(F.col(c), F.when(F.col(c).isNotNull(), F.col(order)))


def build_vessel_days(pos: DataFrame) -> DataFrame:
    """One row per vessel per day, from ALL Silver positions (not only near installations)."""
    return (pos.groupBy("mmsi", "dt")
               .agg(F.count("*").alias("n_msgs"), F.min("event_ts").alias("first_ts"), F.max("event_ts").alias("last_ts"),
                    F.sum(F.col("is_near_installation").cast("int")).alias("n_near"),
                    F.sum(F.col("is_at_installation").cast("int")).alias("n_zone"),
                    F.countDistinct("inst_name").alias("installations"),
                    _latest("name").alias("name"), _latest("shipType").alias("ship_type"),
                    _latest("callSign").alias("call_sign"), _latest("imo").alias("imo"),
                    _latest("shipLength").alias("length_m"), _latest("destination").alias("destination")))


def build_vessels(vessel_days: DataFrame) -> DataFrame:
    """One row per vessel: latest identity, first and last seen, totals. Built from vessel_days (small)."""
    latest = lambda c: F.max_by(F.col(c), F.when(F.col(c).isNotNull(), F.col("last_ts"))).alias(c)
    return (vessel_days.groupBy("mmsi")
                       .agg(F.min("first_ts").alias("first_seen"), F.max("last_ts").alias("last_seen"),
                            F.countDistinct("dt").alias("days_seen"), F.sum("n_msgs").alias("n_msgs"),
                            F.sum("n_near").alias("n_near"), F.sum("n_zone").alias("n_zone"),
                            *[latest(c) for c in ("name", "ship_type", "call_sign", "imo", "length_m", "destination")]))


def build_installations(spark, cfg, path) -> DataFrame:
    """One row per installation in the facility filter, with its field, phase and position (from Bronze)."""
    from spark.common import filter_facilities
    df = spark.read.option("header", True).csv(path)
    kind, name = cfg.get("facility_kind_col", "fclKind"), cfg.get("facility_name_col", "fclName")
    df = filter_facilities(df, cfg.get("facility_filter"), kind)
    col = lambda c, alias: (F.col(c) if c in df.columns else F.lit(None).cast("string")).alias(alias)
    onshore = (F.coalesce(F.col(kind).isin(cfg.get("onshore_kinds", ["ONSHORE FACILITY"])), F.lit(False))
               if kind in df.columns else F.lit(False))
    return (df.select(col(name, "inst_name"), col(kind, "inst_kind"), col("belong2nm", "field"),
                      col("phaseName", "phase"), col("FixMov", "fixed_or_moveable"), onshore.alias("is_onshore"),
                      F.col("centroid_lat").cast("double").alias("lat"), F.col("centroid_lon").cast("double").alias("lon"))
              .where("inst_name is not null and lat is not null and lon is not null")
              .dropDuplicates(["inst_name"]))
