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


# ---- use cases 2 and 3: safety-zone compliance and infrastructure surveillance -----------------------------------

def _sessions(df, keys, gap_s, flag=None):
    """Numbers consecutive rows per `keys` (by event_ts) into sessions: a new session starts after a silence
    longer than gap_s, or, when `flag` is given, whenever that boolean column changes value."""
    w = Window.partitionBy(*keys).orderBy("event_ts")
    gap = F.col("event_ts").cast("long") - F.lag("event_ts").over(w).cast("long")
    new = gap.isNull() | (gap > gap_s)
    if flag is not None:
        new = new | (F.col(flag) != F.coalesce(F.lag(flag).over(w), F.col(flag)))
    return (df.withColumn("_new", F.when(new, 1).otherwise(0))
              .withColumn("session_no", F.sum("_new").over(w)).drop("_new"))


def build_zone_entries(near: DataFrame, gap_s: int) -> DataFrame:
    """One row per entry into an installation's 500 m safety zone (use case 2)."""
    z = _sessions(near.where("is_at_installation"), ["mmsi", "inst_name"], gap_s)
    return (z.groupBy("mmsi", "inst_name", "inst_kind", "session_no")
             .agg(F.first("name", ignorenulls=True).alias("vessel_name"), _onshore(z),
                  F.first("shipType", ignorenulls=True).alias("ship_type"),
                  F.min("event_ts").alias("entry_ts"), F.max("event_ts").alias("exit_ts"),
                  F.min("dist_m").alias("min_dist_m"), F.max("speedOverGround").alias("max_sog_kn"),
                  F.avg("speedOverGround").alias("avg_sog_kn"), F.count("*").alias("n_msgs"))
             .withColumn("minutes_inside", (F.col("exit_ts").cast("long") - F.col("entry_ts").cast("long")) / 60.0)
             .drop("session_no"))


def build_loitering(near: DataFrame, gap_s: int, max_kn: float, min_minutes: float) -> DataFrame:
    """Episodes of a vessel staying slow (< max_kn) within the near radius of one installation for at least
    min_minutes (use case 3). A position without speed counts as not slow."""
    slow = near.withColumn("_slow", F.coalesce(F.col("speedOverGround") < max_kn, F.lit(False)))
    s = _sessions(slow, ["mmsi", "inst_name"], gap_s, flag="_slow").where("_slow")
    return (s.groupBy("mmsi", "inst_name", "inst_kind", "session_no")
             .agg(F.first("name", ignorenulls=True).alias("vessel_name"), _onshore(s),
                  F.first("shipType", ignorenulls=True).alias("ship_type"),
                  F.min("event_ts").alias("start_ts"), F.max("event_ts").alias("end_ts"),
                  F.min("dist_m").alias("min_dist_m"), F.avg("speedOverGround").alias("avg_sog_kn"),
                  F.max(F.col("is_at_installation").cast("int")).alias("entered_zone"), F.count("*").alias("n_msgs"))
             .withColumn("duration_min", (F.col("end_ts").cast("long") - F.col("start_ts").cast("long")) / 60.0)
             .where(F.col("duration_min") >= min_minutes)
             .withColumn("event_type", F.lit("loitering")).drop("session_no"))


def build_ais_gaps(pos: DataFrame, min_gap_s: int, max_gap_s: int, since_ts: str) -> DataFrame:
    """AIS gaps near installations (use case 3): two CONSECUTIVE messages of a vessel (over all its positions, so a
    message anywhere in between rules the gap out), both within the near radius, between min_gap_s and max_gap_s
    apart. Only gaps ending at or after since_ts ('YYYY-MM-DD HH:MM:SS', UTC) are returned."""
    w = Window.partitionBy("mmsi").orderBy("event_ts")
    onshore = F.col("is_onshore") if "is_onshore" in pos.columns else F.lit(None).cast("boolean")
    p = (pos.withColumn("prev_ts", F.lag("event_ts").over(w))
            .withColumn("prev_near", F.lag("is_near_installation").over(w))
            .withColumn("prev_inst", F.lag("inst_name").over(w))
            .withColumn("prev_dist", F.lag("dist_m").over(w)))
    gap = F.col("event_ts").cast("long") - F.col("prev_ts").cast("long")
    return (p.where(F.col("is_near_installation") & F.col("prev_near") & (gap > min_gap_s) & (gap <= max_gap_s)
                    & (F.col("event_ts") >= F.to_timestamp(F.lit(since_ts))))
             .select("mmsi", "inst_name", "inst_kind", onshore.alias("is_onshore"),
                     F.col("name").alias("vessel_name"), F.col("shipType").alias("ship_type"),
                     F.col("prev_ts").alias("start_ts"), F.col("event_ts").alias("end_ts"),
                     F.col("prev_inst").alias("start_inst_name"),
                     F.least("prev_dist", "dist_m").alias("min_dist_m"), (gap / 60.0).alias("duration_min"))
             .withColumn("event_type", F.lit("ais_gap")))


def flag_regular(events: DataFrame, visits: DataFrame, on_col: str, min_days: int, window_days: int) -> DataFrame:
    """Adds visit_days_before (distinct days with a visit by this vessel to this installation in the window_days
    before the event's day) and regular_visitor (visit_days_before >= min_days)."""
    vd = visits.select("mmsi", "inst_name", F.to_date("arrival_ts").alias("vday")).distinct()
    e = events.withColumn("_eid", F.monotonically_increasing_id()).cache()
    cnt = (e.select("_eid", "mmsi", "inst_name", F.to_date(on_col).alias("_d")).join(vd, ["mmsi", "inst_name"])
            .where((F.col("vday") < F.col("_d")) & (F.col("vday") >= F.date_sub(F.col("_d"), window_days)))
            .groupBy("_eid").agg(F.countDistinct("vday").alias("visit_days_before")))
    return (e.join(cnt, "_eid", "left").fillna(0, ["visit_days_before"])
             .withColumn("regular_visitor", F.col("visit_days_before") >= min_days).drop("_eid"))
