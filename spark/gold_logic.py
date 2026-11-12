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


def _col_or_null(df, name, typ):
    return F.col(name) if name in df.columns else F.lit(None).cast(typ)


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
                  F.max(F.col("is_at_installation").cast("int")).alias("entered_zone"), F.count("*").alias("n_msgs"),
                  F.max(_col_or_null(s, "shipLength", "int")).alias("length_m"))
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
                     _col_or_null(p, "shipLength", "int").alias("length_m"),
                     _col_or_null(p, "speedOverGround", "double").alias("sog_kn"),
                     F.col("prev_ts").alias("start_ts"), F.col("event_ts").alias("end_ts"),
                     F.col("prev_inst").alias("start_inst_name"),
                     F.least("prev_dist", "dist_m").alias("min_dist_m"), (gap / 60.0).alias("duration_min"))
             .withColumn("event_type", F.lit("ais_gap")))


def flag_regular(events: DataFrame, visits: DataFrame, on_col: str, min_days: int, window_days: int) -> DataFrame:
    """Adds days_present_before (distinct days this vessel was present at this installation, counting every day
    from a visit's arrival to its departure, in the window_days before the event's day) and regular_visitor
    (days_present_before >= min_days). Counting presence, not arrivals, makes a vessel stationed for weeks regular."""
    vd = (visits.select("mmsi", "inst_name",
                        F.explode(F.sequence(F.to_date("arrival_ts"), F.to_date("departure_ts"))).alias("vday"))
                .distinct())
    e = events.withColumn("_eid", F.monotonically_increasing_id()).cache()
    cnt = (e.select("_eid", "mmsi", "inst_name", F.to_date(on_col).alias("_d")).join(vd, ["mmsi", "inst_name"])
            .where((F.col("vday") < F.col("_d")) & (F.col("vday") >= F.date_sub(F.col("_d"), window_days)))
            .groupBy("_eid").agg(F.countDistinct("vday").alias("days_present_before")))
    return (e.join(cnt, "_eid", "left").fillna(0, ["days_present_before"])
             .withColumn("regular_visitor", F.col("days_present_before") >= min_days).drop("_eid"))


# ---- flags that separate ordinary operations from events worth a look -------------------------------------------

def _norm_name(c):
    """Upper case, Norwegian letters spelled out, everything but letters and digits removed: 'Åsgard-B' -> 'ASGARDB'."""
    c = F.upper(F.trim(c))
    c = F.regexp_replace(F.regexp_replace(c, "Æ", "AE"), "Ø", "O")
    c = F.translate(c, "ÅÄÖÜÉ", "AAOUE")
    return F.regexp_replace(c, "[^A-Z0-9]", "")


FLOATING_KINDS = ["FPSO", "FSU", "FSO", "FPU", "SPAR", "TLP STEEL", "TLP CONCRETE", "SEMISUB STEEL", "SEMISUB CONCRETE",
                  "JACK-UP 3 LEGS", "JACK-UP 4 LEGS", "VESSEL", "DIVE SUPPORT VESSEL"]
TYPE_SUFFIXES = ["FPSO", "FSU", "FSO", "FPU", "SPAR", "TLP"]   # a type word at the end of a name, not part of it


def own_name(c):
    """An installation's own name without a trailing type word: 'AASTA HANSTEEN SPAR' -> 'AASTAHANSTEEN'."""
    return F.regexp_replace(_norm_name(c), "(" + "|".join(TYPE_SUFFIXES) + ")$", "")


def flag_facility(df: DataFrame, facility_names: DataFrame, name_col: str = "vessel_name",
                  speed_col: str = None, stationary_kn: float = 1.0) -> DataFrame:
    """is_facility: the AIS name matches a unit in Sodir's FULL facility list, i.e. a platform, FPSO or rig
    broadcasting AIS itself. Fixed platforms always count. Floating units (FPSO, FSU, SPAR, rigs, anything Sodir
    lists as MOVEABLE) count only while stationary (speed_col < stationary_kn): moving, they are vessels.
    facility_names has columns fac, and optionally kind and fixmov. Names under 4 characters never match."""
    kind = F.upper(F.col("kind")) if "kind" in facility_names.columns else F.lit(None).cast("string")
    fixmov = F.upper(F.col("fixmov")) if "fixmov" in facility_names.columns else F.lit(None).cast("string")
    floating = kind.isin(FLOATING_KINDS) | (fixmov == "MOVEABLE")
    names = (facility_names.select(_norm_name(F.col("fac")).alias("_n"), F.coalesce(floating, F.lit(False)).alias("_float"))
                           .where(F.length("_n") >= 4).groupBy("_n").agg(F.max("_float").alias("_float"))
                           .withColumn("_fac", F.lit(True)))
    still = F.coalesce(F.col(speed_col) < stationary_kn, F.lit(False)) if speed_col else F.lit(True)
    return (df.withColumn("_n", _norm_name(F.col(name_col))).join(F.broadcast(names), "_n", "left")
              .withColumn("is_facility", F.coalesce(F.col("_fac") & (~F.col("_float") | still), F.lit(False)))
              .drop("_n", "_fac", "_float"))


def build_coverage(pos: DataFrame) -> DataFrame:
    """Messages and distinct vessels per hour over ALL positions: how complete the feed was in each hour."""
    return (pos.groupBy(F.date_trunc("hour", "event_ts").alias("hour_ts"))
               .agg(F.count("*").alias("msgs"), F.countDistinct("mmsi").alias("vessels"))
               .withColumn("dt", F.to_date("hour_ts")))


def covered_hours(coverage: DataFrame, min_fraction: float, latest_ts) -> DataFrame:
    """Hours with at least min_fraction of the median hourly volume, plus every hour from latest_ts's hour on
    (the current hour is still filling up and must not count as an outage)."""
    median = coverage.agg(F.percentile_approx("msgs", 0.5)).collect()[0][0] or 0
    ok = coverage.where(F.col("msgs") >= F.lit(min_fraction * median)).select(F.col("hour_ts").alias("_h"))
    if latest_ts is not None:
        ok = ok.unionByName(coverage.where(F.col("hour_ts") >= F.date_trunc("hour", F.lit(latest_ts)))
                                    .select(F.col("hour_ts").alias("_h")))
    return ok.distinct().withColumn("_ok", F.lit(True))


def flag_censored(events: DataFrame, ok_hours: DataFrame, start_col: str) -> DataFrame:
    """censored: the hour before the event started was not covered, so it may have begun before the data did."""
    e = events.withColumn("_h", F.date_trunc("hour", F.col(start_col)) - F.expr("INTERVAL 1 HOUR"))
    return (e.join(F.broadcast(ok_hours), "_h", "left")
             .withColumn("censored", F.col("_ok").isNull()).drop("_h", "_ok"))


def flag_outage(events: DataFrame, ok_hours: DataFrame, start_col: str, end_col: str) -> DataFrame:
    """feed_outage: some hour inside the event was not covered, so the whole feed was quiet, not just this vessel."""
    e = events.withColumn("_eid", F.monotonically_increasing_id()).cache()
    hrs = (e.select("_eid", F.explode(F.sequence(F.date_trunc("hour", F.col(start_col)), F.date_trunc("hour", F.col(end_col)),
                                                 F.expr("INTERVAL 1 HOUR"))).alias("_h"))
            .join(F.broadcast(ok_hours), "_h", "left")
            .groupBy("_eid").agg(F.sum(F.col("_ok").isNull().cast("int")).alias("_bad")))
    return (e.join(hrs, "_eid", "left").withColumn("feed_outage", F.coalesce(F.col("_bad"), F.lit(0)) > 0)
             .drop("_eid", "_bad"))


def flag_ordinary(events: DataFrame, large_ship_min_m: int) -> DataFrame:
    """Two kinds of ordinary activity that are not a security concern:
    terminal_call  - a large ship (length_m >= large_ship_min_m) at an ONSHORE facility, e.g. a tanker waiting to berth;
    support_craft  - the vessel's name contains the installation's own name (type word such as FPSO or SPAR ignored),
                     e.g. 'DRAUGEN MOB BOAT' at DRAUGEN, 'AASTA HANSTEEN MOB' at AASTA HANSTEEN SPAR."""
    inst = own_name(F.col("inst_name"))
    return (events
            .withColumn("terminal_call", F.coalesce(F.col("is_onshore"), F.lit(False))
                        & F.coalesce(F.col("length_m") >= large_ship_min_m, F.lit(False)))
            .withColumn("support_craft", F.coalesce((F.length(inst) >= 4)
                        & (F.instr(_norm_name(F.col("vessel_name")), inst) > 0), F.lit(False))))


def mark_worth_a_look(events: DataFrame, data_flag: str) -> DataFrame:
    """worth_a_look: none of the ordinary explanations applies. data_flag is 'censored' (loitering) or
    'feed_outage' (AIS gaps)."""
    ordinary = (F.col("regular_visitor") | F.col("is_facility") | F.col(data_flag)
                | F.col("terminal_call") | F.col("support_craft"))
    return events.withColumn("worth_a_look", ~ordinary)
