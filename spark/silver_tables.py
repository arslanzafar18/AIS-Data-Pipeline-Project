"""
Silver split into purpose-built tables (PySpark DataFrame / Spark SQL APIs, no pandas).

Every parsed input row ends in exactly ONE fate, so the counts always reconcile:

    rejected      unusable (unparsable, bad timestamp, invalid MMSI, impossible position ...)  -> silver/rejects
    duplicate     same (mmsi, event_ts) seen again in the batch; earliest ingest wins            -> counted only
    non_vessel    real AIS stations that are not ships (aircraft, aids to navigation, beacons,
                  coast stations, group calls, auxiliary craft)                                   -> silver/non_vessel_positions
    suspect       well formed but implausible (null island, outside the data area, position spike) -> silver/suspect_positions
    position      a clean vessel position                                                         -> silver/positions (+ position_installation)

Reference and dimension tables: silver/installations (Sodir), silver/vessels (static data per MMSI).
"""
from functools import reduce
from pyspark.sql import DataFrame, Window, functions as F
from spark.common import haversine_m

VESSEL_MID = (201, 775)                     # ITU maritime identification digits that ship stations can carry
# Norwegian EEZ, Svalbard and Jan Mayen with a margin; a position outside this box is not plausible for this feed
AREA = {"lat_min": 53.0, "lat_max": 83.0, "lon_min": -12.0, "lon_max": 40.0}
STATIC_FIELDS = ["name", "shipType", "imo", "callSign", "shipLength", "shipWidth"]   # destination is voyage data


def _mid_ok(s, start):
    """3 digits of the zero padded MMSI string starting at 1-based position `start` are a valid MID."""
    return s.substr(start, 3).cast("int").between(*VESSEL_MID)


def classify_mmsi(df: DataFrame) -> DataFrame:
    """Adds mmsi_class from the ITU numbering plan. mmsi is a Long, so leading zeros are restored first:
    vessel (MID 201-775), aircraft (111...), aton (99MID), auxiliary_craft (98MID), beacon (970/972/974),
    coast_station (00MID), group_id (0MID), other_station (9 digits, no ITU pattern: AIS buoys and gear markers), invalid (null, 10+ digits, under 9 digits)."""
    m = F.col("mmsi")
    s = F.lpad(m.cast("string"), 9, "0")
    cls = (F.when(m.isNull() | (m > 999999999) | (m < 0), "invalid")
            .when(m.between(111000000, 111999999) | F.col("altitude").isNotNull(), "aircraft")
            .when(s.substr(1, 3).isin("970", "972", "974"), "beacon")
            .when(s.startswith("99") & _mid_ok(s, 3), "aton")
            .when(s.startswith("98") & _mid_ok(s, 3), "auxiliary_craft")
            .when(s.startswith("00") & _mid_ok(s, 3), "coast_station")
            .when(s.startswith("0") & _mid_ok(s, 2), "group_id")
            .when(_mid_ok(s, 1), "vessel")
            .when(m >= 100000000, "other_station")
            .otherwise("invalid"))
    return df.withColumn("mmsi_class", cls)


def flag_suspect(v: DataFrame, max_kn: float = 60.0, min_jump_m: float = 1000.0, area: dict = AREA) -> DataFrame:
    """Adds suspect_reason (null = plausible) for vessel positions:
      null_island   lat and lon both ~0
      out_of_area   outside AREA
      position_spike  the vessel would have to travel faster than max_kn BOTH to this point and away from it
                      (a lone bad fix). Using both legs means the good point after a spike is not blamed.
    Within one batch only, so a vessel that reports once per batch cannot be tested; a daily pass closes that gap."""
    lat, lon, ts = F.col("latitude"), F.col("longitude"), F.col("event_ts")
    w = Window.partitionBy("mmsi").orderBy("event_ts")
    p_lat, p_lon, p_ts = (F.lag(c).over(w) for c in ("latitude", "longitude", "event_ts"))
    n_lat, n_lon, n_ts = (F.lead(c).over(w) for c in ("latitude", "longitude", "event_ts"))
    d_in, d_out = haversine_m(lat, lon, p_lat, p_lon), haversine_m(lat, lon, n_lat, n_lon)
    dt_in, dt_out = ts.cast("long") - p_ts.cast("long"), n_ts.cast("long") - ts.cast("long")
    kn = lambda d, dt: F.when(dt > 0, d / 1852.0 / (dt / 3600.0))
    spike = (kn(d_in, dt_in) > max_kn) & (kn(d_out, dt_out) > max_kn) & (d_in > min_jump_m) & (d_out > min_jump_m)
    reason = (F.when((F.abs(lat) < 0.001) & (F.abs(lon) < 0.001), "null_island")
               .when(~lat.between(area["lat_min"], area["lat_max"]) | ~lon.between(area["lon_min"], area["lon_max"]), "out_of_area")
               .when(F.coalesce(spike, F.lit(False)), "position_spike"))
    return v.withColumn("suspect_reason", reason)


def stage_rows(flagged: DataFrame) -> dict:
    """flagged = parsed + standardized + classify_mmsi + validate (has reject_reason, mmsi_class).
    Returns five lazy DataFrames that partition the input rows (persist `flagged` before writing them)."""
    rejected = flagged.where(F.col("reject_reason").isNotNull())
    w = Window.partitionBy("mmsi", "event_ts").orderBy(F.col("ingest_ts").asc_nulls_last(), "_file")
    ok = flagged.where(F.col("reject_reason").isNull()).withColumn("_rn", F.row_number().over(w))
    first = ok.where("_rn = 1").drop("_rn")
    vessels = flag_suspect(first.where(F.col("mmsi_class") == "vessel"))
    return {"rejects": rejected,
            "duplicates": ok.where("_rn > 1").drop("_rn"),
            "non_vessel": first.where(F.col("mmsi_class") != "vessel"),
            "suspect": vessels.where(F.col("suspect_reason").isNotNull()),
            "positions": vessels.where(F.col("suspect_reason").isNull()).drop("suspect_reason")}


def fate_counts(stages: dict) -> DataFrame:
    """(outcome, reason, count): one row per fate. The counts add up to the input rows by construction."""
    def lab(name, df, reason_col=None):
        reason = F.col(reason_col) if reason_col else F.lit(None).cast("string")
        return df.select(F.lit(name).alias("outcome"), reason.alias("reason"))
    parts = [lab("rejected", stages["rejects"], "reject_reason"), lab("duplicate", stages["duplicates"]),
             lab("non_vessel", stages["non_vessel"], "mmsi_class"), lab("suspect", stages["suspect"], "suspect_reason"),
             lab("position", stages["positions"])]
    return reduce(DataFrame.unionByName, parts).groupBy("outcome", "reason").count().orderBy(F.desc("count"))


def installation_proximity(enriched: DataFrame) -> DataFrame:
    """Sparse table: only positions that are near an installation, with what Gold needs (no join back to positions)."""
    return (enriched.where("is_near_installation")
            .select("mmsi", "event_ts", "dt", "hour", "inst_id", "inst_name", "inst_kind", "is_onshore", "dist_m",
                    "is_at_installation", "is_stationary", "speedOverGround"))


def vessel_static_observations(v: DataFrame) -> DataFrame:
    """Per batch: one row per distinct static tuple per vessel (a few rows, not one per message)."""
    cols = [c for c in STATIC_FIELDS if c in v.columns]
    has_static = reduce(lambda a, b: a | b, [F.col(c).isNotNull() for c in cols])
    return (v.where(has_static).groupBy("mmsi", *cols)
             .agg(F.min("event_ts").alias("first_seen"), F.max("event_ts").alias("last_seen"), F.count("*").alias("n_msgs")))


def build_vessels(obs: DataFrame) -> DataFrame:
    """Dimension: latest non-null static value per MMSI, plus how often name / ship type changed (a quality signal)."""
    def latest(c, alias):
        pick = F.when(F.col(c).isNotNull(), F.struct(F.col("last_seen").alias("t"), F.col(c).alias("v")))
        return F.max(pick)["v"].alias(alias)
    return obs.groupBy("mmsi").agg(
        latest("name", "name"), latest("shipType", "ship_type"), latest("imo", "imo"), latest("callSign", "call_sign"),
        latest("shipLength", "length_m"), latest("shipWidth", "width_m"),
        F.min("first_seen").alias("first_seen"), F.max("last_seen").alias("last_seen"), F.sum("n_msgs").alias("n_msgs"),
        F.size(F.collect_set("name")).alias("name_variants"), F.size(F.collect_set("shipType")).alias("ship_type_variants"))
