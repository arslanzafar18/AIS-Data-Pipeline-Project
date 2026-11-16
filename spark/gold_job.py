"""
Gold job: builds consumer tables from Silver and PUBLISHES THEM ONLY IF QUALITY CHECKS PASS.

  gold/vessel_visits                 one row per vessel visit to an installation, with wait_min and alongside_min
                                                                                        (partitioned by arrival day)
  gold/installation_activity_hourly  vessels / messages / speed per installation-hour   (partitioned by day)
  gold/vessel_days                   one row per vessel per day, from all positions     (partitioned by day)
  gold/vessels                       one row per vessel: latest identity, first/last seen, fishing-register match,
                                     rescue craft, offshore fleet, vessel group, unexpected_in_zone (+ reason)
  gold/safety_zone_entries           every zone entry with earlier_entries, is_fast, risk_level/reason, is_unexpected
  gold/standby_gaps                  runs of hours with no vessel within 5 km of an offshore installation
  gold/feed_outage_hours             hours with under coverage_min_fraction of the median message volume
  gold/dark_detections               radar detections without AIS near installations (5 km) or pipelines (2 km)
                                     (rebuilt whole every run: the vessel classification changes as history grows)
  gold/installations                 one row per installation in the facility filter, with field and position
  gold/zone_entries                  one row per entry into a 500 m safety zone                (by entry day)
  gold/loitering_events              slow (< loiter_max_kn) within 5 km for >= loiter_min_minutes (by start day)
  gold/ais_gap_events                vessel silent >= ais_gap_min_minutes between two positions near an
                                     installation, nothing sent anywhere in between             (by end day)
  gold/feed_coverage_hourly          messages and vessels per hour over all positions          (by day)
  gold/vessel_tracks                 latest position per vessel per track_slot_s (10 min)      (by day)
  gold/helicopter_landings           aircraft (SAR MMSI 111xxxxxx or altitude reported) inside a 500 m zone (by day)
  Aircraft are left out of visits, hourly activity, zone entries and surveillance events.
  Warnings (Gold still publishes): speed glitches above gate.max_sog_glitch_rate of positions in the last 24 h.
  Event tables carry: regular_visitor (present at that installation on >= regular_visitor.min_days distinct days
  in the regular_visitor.window_days before the event), is_facility (the "vessel" is a platform or rig in Sodir's
  list broadcasting AIS; floating units such as FPSOs and rigs only while stationary), censored (started right after a feed gap or the start of the data), for AIS gaps
  feed_outage (the whole feed was quiet during the gap), terminal_call (large ship at an onshore facility),
  support_craft (name contains the installation's name) and worth_a_look (none of these applies).
  Surveillance events (loitering, AIS gaps) use live data only: they start at or after surveillance.live_since.

INCREMENTAL: a run recomputes only from the earliest changed day S (the last `gold_recent_days` days, plus any older
day whose Silver files changed since the last successful run) to the newest day; a published visit still open at S (a
standby vessel can stay for weeks) is rebuilt from its own arrival, reading older days only for that vessel and
installation, so no visit is cut in two and the other, final visits on those days are kept as published. Recomputed days
are written to gold/_staging/<run>, checked against Silver for the same days, then swapped in one day at a time;
days older than lookback_days are dropped. --full (and the first run on unpartitioned tables) rebuilds everything.
KEYS: the installation key is Sodir's facility number (inst_id). Every Silver row is mapped to the CURRENT name of its
number before any table is built (rows written before Silver stored the number get it from their name), so a renamed
installation keeps one history; when the numbers or names in the facility list change, the next run is a full rebuild.
Every recomputed table is checked for repeated keys (KEYS; a full run checks its newest days, so the check stays
cheap), and the installations dimension for repeated inst_id and inst_name. Repeats are a WARNING, not a gate check:
Gold still publishes, and the warning is printed to the Gold log and stored in _READY.json and the trace.
On failure the previous Gold stays untouched, gold/_LAST_FAILED.json is written and the process exits non-zero.
"""
import argparse, hashlib, json, os, shutil, sys, uuid
from datetime import datetime, timedelta, timezone
from pyspark import StorageLevel
from pyspark.sql import functions as F

import observability as obs
import pipeline_config
from spark import gold_plan as plan
from spark.gold_logic import (build_ais_gaps, build_coverage, build_hourly, build_installations, build_loitering,
                              build_vessel_days, build_vessels, build_visits, build_zone_entries, covered_hours,
                              build_vessel_tracks, conform_installations, duplicate_keys, with_position_flags, flag_censored, flag_facility, flag_ordinary, flag_outage, flag_regular,
                              mark_worth_a_look, match_fishing_register, no_fishing_register,
                              visit_zone_times, classify_vessels, assess_zone_entries,
                              standby_gaps, feed_outage_hours, dark_near_infrastructure,
                              STANDBY_SCHEMA, OUTAGE_SCHEMA, DARK_SCHEMA)
from spark.session import get_spark

PARTITIONED = ("vessel_visits", "installation_activity_hourly", "vessel_days",
               "zone_entries", "loitering_events", "ais_gap_events", "feed_coverage_hourly", "vessel_tracks", "helicopter_landings")
LAYOUT = "dt-partitioned-v9"      # bump when tables are added or change shape: the next run is then a full rebuild
REBUILT_FROM_OPEN_VISITS = {"vessel_visits": "arrival_ts", "zone_entries": "entry_ts", "loitering_events": "start_ts"}
DIMENSIONS = ("vessels", "installations", "safety_zone_entries",          # rebuilt whole every run
              "standby_gaps", "feed_outage_hours", "dark_detections")
KEYS = {"vessel_visits": ["mmsi", "inst_name", "arrival_ts"], "installation_activity_hourly": ["inst_name", "hour_ts"],
        "vessel_days": ["mmsi", "dt"], "vessel_tracks": ["mmsi", "slot_ts"], "zone_entries": ["mmsi", "inst_name", "entry_ts"],
        "loitering_events": ["mmsi", "inst_name", "start_ts"], "ais_gap_events": ["mmsi", "end_ts"],
        "feed_coverage_hourly": ["hour_ts"], "helicopter_landings": ["mmsi", "inst_name", "arrival_ts"]}


def silver_reject_rate(cfg, path, lookback_days):
    if not os.path.exists(path):
        return None, 0
    since = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    rej = inp = 0
    for line in open(path):
        m = json.loads(line)
        if datetime.fromisoformat(m["processed_at"]) >= since:
            rej += m["rejected_rows"]; inp += m["input_rows"]
    return (rej / inp if inp else None), inp


def read_table(spark, path):
    """A published Gold table, or None when it has no rows (Spark cannot read a folder without data files)."""
    return spark.read.parquet(path) if plan.has_data(path) else None


def union_kept(new, kept):
    return new if kept is None else new.unionByName(kept)


def glitch_rate(path, hours=24):
    """Share of accepted Silver positions whose speed was nulled as a glitch, over the last `hours`."""
    if not os.path.exists(path):
        return None, 0
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    g = n = 0
    for line in open(path):
        m = json.loads(line)
        if datetime.fromisoformat(m["processed_at"]) >= since:
            g += m.get("corrected_sog_glitch", 0) or 0; n += m.get("valid_rows", 0) or 0
    return (g / n if n else None), n


def kept_published(spark, path, ts_col, open_visits, v_day, s_day):
    """Rows of a published table on days [v_day, s_day) that are final: everything except what belongs to a visit
    still open at S (same vessel and installation, at or after that visit's arrival), which is rebuilt instead.
    None when the table has no rows."""
    pub = read_table(spark, path)
    if pub is None:
        return None
    pub = pub.where((F.col("dt") >= F.lit(v_day)) & (F.col("dt") < F.lit(s_day)))
    return (pub.join(F.broadcast(open_visits), ["mmsi", "inst_name"], "left")
               .where(F.col("open_from").isNull() | (F.col(ts_col) < F.col("open_from"))).drop("open_from"))


def whole_window_tables(spark, cfg, P, stage, manifest, hourly, coverage, newest, window_start):
    """Tables over the whole published window, rebuilt every run because each depends on all of it: standby gaps
    (hours without a vessel near an offshore installation), feed outage hours, and radar detections without AIS near
    installations and pipelines. A failure here writes an empty table and a warning: it never blocks Gold."""
    sc = cfg.get("surveillance", {})
    min_frac = sc.get("coverage_min_fraction", 0.5)
    hourly_pub = read_table(spark, P("gold/installation_activity_hourly"))
    hourly_pub = hourly if hourly_pub is None else hourly_pub
    cov_pub = read_table(spark, P("gold/feed_coverage_hourly"))
    cov_pub = coverage if cov_pub is None else cov_pub

    def write(name, build, schema):
        try:
            df = build()
        except Exception as e:      # optional output: log it, publish an empty table, carry on
            manifest.setdefault("warnings", []).append(f"{name} not built: {e!r}"[:300])
            df = None
        (df if df is not None else spark.createDataFrame([], schema)).write.mode("overwrite").parquet(f"{stage}/{name}")

    def gaps():
        span = hourly_pub.agg(F.min("hour_ts"), F.max("hour_ts")).collect()[0]
        if span[0] is None:
            return None
        start = span[0].replace(hour=0, minute=0, second=0, microsecond=0)
        return standby_gaps(hourly_pub, cov_pub, min_frac, start, span[1])
    write("standby_gaps", gaps, STANDBY_SCHEMA)
    write("feed_outage_hours", lambda: feed_outage_hours(cov_pub, min_frac, newest), OUTAGE_SCHEMA)

    def dark():
        sar, seg_path, fac_path = P("bronze/gfw/sar_detections.csv"), P("bronze/reference/pipelines.csv"), P("bronze/reference/facilities.csv")
        if not os.path.exists(sar) or not os.path.exists(fac_path):
            return None
        from spark.common import load_facilities
        from spark.gold_pipelines import segments_df
        det = spark.read.option("header", True).csv(sar).where(F.col("date") >= F.lit(str(window_start))).cache()
        meta = det.agg(F.count("*").alias("cells"), F.max("date").alias("newest"), F.max("fetched_at").alias("accessed"),
                       F.collect_set("dataset").alias("versions")).collect()[0]
        inst_m, pipe_m = sc.get("dark_inst_m", 5000), sc.get("dark_pipe_m", 2000)
        manifest["radar"] = {"cells": meta["cells"], "newest": meta["newest"], "accessed": meta["accessed"],
                             "versions": sorted(meta["versions"] or []), "inst_m": inst_m, "pipe_m": pipe_m}
        inst = (load_facilities(spark, cfg, fac_path)
                .where("NOT is_onshore AND i_lat IS NOT NULL AND i_lon IS NOT NULL AND NOT isnan(i_lat) AND NOT isnan(i_lon)"))
        seg = (segments_df(spark, seg_path, sc.get("pipeline_phases", ["IN SERVICE"])) if os.path.exists(seg_path)
               else spark.createDataFrame([], "pipeline_id string, name string, label string, medium string, phase string, "
                                              "seg_no string, lon1 double, lat1 double, lon2 double, lat2 double"))
        return dark_near_infrastructure(det, inst, seg, inst_m, pipe_m)
    write("dark_detections", dark, DARK_SCHEMA)


def log_run(P, manifest):
    """Appends one line per Gold run (published or blocked) to quality/gold_runs.jsonl: the history behind the
    dashboard's Pipeline health tab. _READY.json and _LAST_FAILED.json only keep the latest run."""
    try:
        failed = [c["check"] for c in manifest.get("checks", []) if not c["passed"]]
        rk = (manifest.get("keys") or {}).get("repeated_keys") or {}
        line = {k: manifest.get(k) for k in ("run_id", "started_at", "finished_at", "runtime_s", "status", "mode",
                                              "recompute_from", "silver_rows")}
        line.update(checks=len(manifest.get("checks", [])), failed=failed, warnings=manifest.get("warnings", []),
                    repeated_keys=sum(rk.values()), duplicates_removed=manifest.get("duplicates_removed"))
        os.makedirs(P("quality"), exist_ok=True)
        with open(P("quality/gold_runs.jsonl"), "a") as f:
            f.write(json.dumps(line) + "\n")
    except Exception as e:                       # the run log must never fail Gold
        print(f"run log not written: {e!r}")


def build_dashboard(spark, cfg, P, manifest):
    """Rebuilds the dashboard page from the PUBLISHED Gold; never fails Gold."""
    try:
        from spark import dashboard
        from spark.common import load_facilities
        with obs.span("gold.dashboard"):
            fac = load_facilities(spark, dict(cfg, facility_filter=None), P("bronze/reference/facilities.csv"))
            dashboard.build_from_gold(spark, P("gold"), manifest, P("gold/dashboard"), cfg, fac=fac)
    except Exception as e:
        print(f"dashboard not updated: {e!r}")


def main(full=False):
    cfg = pipeline_config.load()
    cfg["lookback_days"] = int(os.environ.get("GOLD_LOOKBACK_DAYS", cfg["lookback_days"]))
    P = lambda rel: pipeline_config.path(cfg, rel)
    gate = cfg["gate"]
    run_id = uuid.uuid4().hex[:10]
    started = datetime.now(timezone.utc)
    spark = get_spark(cfg, "gold")
    if cfg.get("gold_shuffle_partitions"):
        spark.conf.set("spark.sql.shuffle.partitions", str(cfg["gold_shuffle_partitions"]))

    # ---- installation key: Sodir's number; a change in the numbers or names means a full rebuild ----------
    from spark.common import load_facilities
    inst_ref = load_facilities(spark, cfg, P("bronze/reference/facilities.csv")).select("inst_id", "inst_name").cache()
    pairs = sorted((r["inst_id"], r["inst_name"]) for r in inst_ref.where("inst_id IS NOT NULL").collect())
    names_hash = hashlib.sha1(json.dumps(pairs, default=str).encode()).hexdigest()[:16]

    # ---- plan: which days to recompute -------------------------------------------------------------------
    _sp = obs.start("gold.plan")
    day_mtimes = plan.silver_day_mtimes(P("silver/positions"))
    if not day_mtimes:
        sys.exit("Gold: no Silver data yet")
    max_dt = max(day_mtimes)
    window_start = max_dt - timedelta(days=cfg["lookback_days"] - 1)
    state = plan.read_json(P("gold/_state.json")) or {}
    last_ok = state.get("last_success_epoch")
    full = bool(full or last_ok is None or state.get("layout") != LAYOUT or state.get("installation_names") != names_hash
                or not all(os.path.isdir(P(f"gold/{t}")) for t in PARTITIONED + DIMENSIONS))
    open_visits = None
    if full:
        s_day = v_day = window_start
    else:
        s_day = plan.plan_start(day_mtimes, max_dt, window_start, cfg.get("gold_recent_days", 2), last_ok)
        s_start = f"{s_day} 00:00:00"
        cut = (datetime.fromisoformat(s_start) - timedelta(seconds=cfg["visit_gap_s"])).isoformat(sep=" ")
        # Published visits still open at S (e.g. a standby vessel staying for weeks): only THESE are rebuilt from
        # their arrival; every other visit on those older days is already final and is kept as published.
        published_visits = read_table(spark, P("gold/vessel_visits"))
        if published_visits is None:
            published_visits = spark.createDataFrame([], "mmsi long, inst_name string, arrival_ts timestamp, departure_ts timestamp")
        open_visits = (published_visits
                       .where((F.col("arrival_ts") < F.to_timestamp(F.lit(s_start)))
                              & (F.col("departure_ts") >= F.to_timestamp(F.lit(cut))))
                       .select("mmsi", "inst_name", F.col("arrival_ts").alias("open_from")).cache())
        first = open_visits.agg(F.min(F.to_date("open_from"))).collect()[0][0]
        v_day = max(window_start, first) if first is not None else s_day
    obs.annotate(mode="full" if full else "incremental", recompute_from=str(s_day), visits_from=str(v_day))
    _sp.end()

    # ---- read Silver: every row from S; before S only the rows of the visits still open at S ---------------
    base = conform_installations(with_position_flags(spark.read.option("mergeSchema", "true").parquet(P("silver/positions"))),
                                 inst_ref)
    raw = base.where((F.col("dt") >= F.lit(s_day)) & (F.col("dt") <= F.lit(max_dt)))
    if full:      # whole window: Silver already dedupes across batches, so skip the full-table shuffle
        read, raw_n = raw, None
    else:         # a few days: deduplicate as a safety net, it is cheap at this size
        raw_n = raw.count()
        read = raw.dropDuplicates(["mmsi", "event_ts"]).persist(StorageLevel.MEMORY_AND_DISK)
    near = read.where("is_near_installation AND NOT is_aircraft").cache()     # ships only
    near_v = near
    if v_day < s_day:
        early = (base.where((F.col("dt") >= F.lit(v_day)) & (F.col("dt") < F.lit(s_day)) & F.col("is_near_installation")
                            & ~F.col("is_aircraft"))
                     .join(F.broadcast(open_visits), ["mmsi", "inst_name"])
                     .where(F.col("event_ts") >= F.col("open_from")).drop("open_from")
                     .dropDuplicates(["mmsi", "event_ts"]))
        near_v = near.unionByName(early, allowMissingColumns=True).cache()

    visits_new = build_visits(near_v, cfg["visit_gap_s"]).withColumn("dt", F.to_date("arrival_ts"))   # + zone times below
    hourly = build_hourly(near).withColumn("dt", F.to_date("hour_ts")).cache()
    vdays = build_vessel_days(read).cache()
    coverage = build_coverage(read).cache()
    tracks = build_vessel_tracks(read, int(cfg.get("track_slot_s", 600))).cache()

    # ---- use cases 2 and 3: zone entries, loitering, AIS gaps --------------------------------------------
    sc, rv = cfg.get("surveillance", {}), cfg.get("regular_visitor", {"min_days": 3, "window_days": 30})
    zone_gap = min(cfg.get("zone_gap_s", cfg["visit_gap_s"]), cfg["visit_gap_s"])   # an open entry implies an open visit
    gap_min_s, gap_max_s = int(sc.get("ais_gap_min_minutes", 30) * 60), int(sc.get("ais_gap_max_hours", 12) * 3600)
    s_start_ts = f"{s_day} 00:00:00"
    zones_new = build_zone_entries(near_v, zone_gap).withColumn("dt", F.to_date("entry_ts")).cache()
    visits_new = visit_zone_times(visits_new, zones_new).cache()      # waiting and alongside minutes per visit
    visits = visits_new
    if v_day < s_day:   # older days = published visits minus the rebuilt open ones, plus their rebuilt versions
        visits = union_kept(visits_new, kept_published(spark, P("gold/vessel_visits"), "arrival_ts",
                                                       open_visits, v_day, s_day)).cache()
    loiter_new = build_loitering(near_v, cfg["visit_gap_s"], sc.get("loiter_max_kn", 2.0), sc.get("loiter_min_minutes", 120))
    cols = [c for c in ("mmsi", "event_ts", "is_near_installation", "inst_name", "inst_kind", "is_onshore", "dist_m",
                        "name", "shipType", "shipLength", "speedOverGround") if c in read.columns]
    before = (base.where((F.col("dt") == F.date_sub(F.lit(s_day), 1))     # each vessel's messages just before S
                  & (F.col("event_ts") >= F.to_timestamp(F.lit(s_start_ts)) - F.expr(f"INTERVAL {gap_max_s} SECONDS")))
                  .select(*(cols + ["is_aircraft"])).dropDuplicates(["mmsi", "event_ts"]))
    gaps_new = build_ais_gaps(read.where("NOT is_aircraft").select(*cols).unionByName(before.where(~F.col("is_aircraft")).select(*cols)),
                              gap_min_s, gap_max_s, s_start_ts)
    # aircraft (SAR helicopters) inside a 500 m zone: helideck landings, kept apart from ship traffic
    landings = (build_zone_entries(read.where("is_near_installation AND is_aircraft"), zone_gap)
                .withColumnRenamed("entry_ts", "arrival_ts").withColumnRenamed("exit_ts", "departure_ts")
                .withColumnRenamed("minutes_inside", "minutes_in_zone").withColumn("dt", F.to_date("arrival_ts")).cache())
    history = visits                                                           # visits used for the regular-visitor flag
    older = None if full else read_table(spark, P("gold/vessel_visits"))
    if older is not None:
        history = visits.unionByName(older.where(
            (F.col("dt") < F.lit(v_day)) & (F.col("dt") >= F.date_sub(F.lit(v_day), int(rv["window_days"]) + 1))))
    # flags: Sodir names (all facilities, fixed and mobile) and hourly feed coverage over the whole window
    fac_csv = spark.read.option("header", True).csv(P("bronze/reference/facilities.csv"))
    kind_col, fix_col = cfg.get("facility_kind_col", "fclKind"), (cfg.get("facility_filter") or {}).get("fixed_col", "FixMov")
    fac_names = fac_csv.select(F.col(cfg.get("facility_name_col", "fclName")).alias("fac"),
                               (F.col(kind_col) if kind_col in fac_csv.columns else F.lit(None).cast("string")).alias("kind"),
                               (F.col(fix_col) if fix_col in fac_csv.columns else F.lit(None).cast("string")).alias("fixmov"))
    still_kn = sc.get("facility_stationary_kn", 1.0)
    cov_all = coverage
    older_cov = None if full else read_table(spark, P("gold/feed_coverage_hourly"))
    if older_cov is not None:
        cov_all = coverage.unionByName(older_cov.where((F.col("dt") < F.lit(s_day)) & (F.col("dt") >= F.lit(window_start))))
    newest = read.agg(F.max("event_ts")).collect()[0][0]
    ok_hours = covered_hours(cov_all, sc.get("coverage_min_fraction", 0.5), newest).cache()
    zones_new = flag_censored(flag_facility(zones_new, fac_names, speed_col="avg_sog_kn", stationary_kn=still_kn),
                              ok_hours, "entry_ts").cache()
    large = sc.get("large_ship_min_m", 100)
    if sc.get("live_since"):   # surveillance uses live data only; the historic backfill is sparser per vessel
        live = F.col("start_ts") >= F.to_timestamp(F.lit(sc["live_since"]))
        loiter_new, gaps_new = loiter_new.where(live), gaps_new.where(live)
    loiter_new = flag_regular(loiter_new, history, "start_ts", rv["min_days"], rv["window_days"])
    loiter_new = flag_censored(flag_facility(loiter_new, fac_names, speed_col="avg_sog_kn", stationary_kn=still_kn),
                               ok_hours, "start_ts")
    loiter_new = mark_worth_a_look(flag_ordinary(loiter_new, large), "censored").withColumn("dt", F.to_date("start_ts")).cache()
    gaps = flag_regular(gaps_new, history, "end_ts", rv["min_days"], rv["window_days"])
    gaps = flag_outage(flag_facility(gaps, fac_names, speed_col="sog_kn", stationary_kn=still_kn), ok_hours, "start_ts", "end_ts")
    gaps = mark_worth_a_look(flag_ordinary(gaps, large), "feed_outage").withColumn("dt", F.to_date("end_ts")).cache()
    zones, loiter = zones_new, loiter_new   # (zones_new keeps the same rows: the flags only add columns)
    if v_day < s_day:
        zones = union_kept(zones_new, kept_published(spark, P("gold/zone_entries"), "entry_ts", open_visits, v_day, s_day)).cache()
        loiter = union_kept(loiter_new, kept_published(spark, P("gold/loitering_events"), "start_ts", open_visits, v_day, s_day)).cache()

    _sp = obs.start("gold.silver_stats")
    ship, air = ~F.col("is_aircraft"), F.col("is_aircraft")
    s = read.agg(F.count("*").alias("n"), F.max("event_ts").alias("max_ts"),
                 F.sum((F.col("is_near_installation") & ship).cast("int")).alias("near_s"),
                 F.sum((F.col("is_at_installation") & ship).cast("int")).alias("zone_s"),
                 F.sum((F.col("is_at_installation") & air).cast("int")).alias("zone_air"),
                 F.sum(air.cast("int")).alias("aircraft_rows"), F.sum(F.col("sog_glitch").cast("int")).alias("glitches")).collect()[0]
    hl = landings.agg(F.count("*").alias("n"), F.sum("n_msgs").alias("msgs")).collect()[0]
    sv = near_v.agg(F.count("*").alias("near_v"), F.sum(F.col("is_at_installation").cast("int")).alias("zone_v")).collect()[0]
    _sp.end(); _sp = obs.start("gold.build_visits")
    v = visits_new.agg(F.count("*").alias("n"), F.sum("n_msgs").alias("msgs"), F.sum("msgs_in_zone").alias("zone"),
                   F.sum((F.col("duration_min") < 0).cast("int")).alias("neg"),
                   F.sum(F.col("mmsi").isNull().cast("int") + F.col("inst_name").isNull().cast("int")).alias("nullkeys")).collect()[0]
    _sp.end(); _sp = obs.start("gold.build_hourly")
    h = hourly.agg(F.count("*").alias("n"), F.sum("position_msgs").alias("msgs"), F.sum("msgs_in_zone").alias("zone")).collect()[0]
    _sp.end(); _sp = obs.start("gold.build_vessel_days")
    d = vdays.agg(F.count("*").alias("n"), F.sum("n_msgs").alias("msgs"), F.countDistinct("mmsi").alias("vessels")).collect()[0]
    tr = tracks.agg(F.count("*").alias("n"), F.countDistinct("mmsi").alias("vessels"),
                    F.sum((F.col("lat").isNull() | F.col("lon").isNull()).cast("int")).alias("nopos")).collect()[0]
    _sp.end(); _sp = obs.start("gold.build_events")
    z = zones_new.agg(F.count("*").alias("n"), F.sum("n_msgs").alias("msgs"),
                      F.sum((F.col("minutes_inside") < 0).cast("int")).alias("neg")).collect()[0]
    lo = loiter_new.agg(F.count("*").alias("n"), F.sum("n_msgs").alias("msgs"),
                        F.sum((F.col("duration_min") < sc.get("loiter_min_minutes", 120)).cast("int")).alias("short")).collect()[0]
    flags = {"loitering_worth_a_look": loiter_new.where("worth_a_look").count(),
             "ais_gaps_worth_a_look": gaps.where("worth_a_look").count()}
    g = gaps.agg(F.count("*").alias("n"), F.sum(((F.col("duration_min") * 60 <= gap_min_s) |
                                                 (F.col("duration_min") * 60 > gap_max_s)).cast("int")).alias("bad")).collect()[0]
    _sp.end()
    rr, rr_in = silver_reject_rate(cfg, P("quality/silver_metrics.jsonl"), cfg["lookback_days"])
    gr, gr_n = glitch_rate(P("quality/silver_metrics.jsonl"))
    warnings = []
    if gr is not None and gr > gate.get("max_sog_glitch_rate", 0.001):
        warnings.append(f"speed glitches (> {cfg.get('max_ship_kn', 50)} kn, nulled) are {gr:.4%} of {gr_n} positions in the "
                        f"last 24 h, above {gate.get('max_sog_glitch_rate', 0.001):.2%}: check the feed or a faulty transponder")

    # ---- keys: every recomputed table should be unique on its key (a full run checks its newest days only) ---
    _sp = obs.start("gold.keys")
    check_from = s_day if not full else max(window_start, max_dt - timedelta(days=cfg.get("gold_recent_days", 2) - 1))
    recomputed = {"vessel_visits": visits_new, "installation_activity_hourly": hourly, "vessel_days": vdays,
                  "vessel_tracks": tracks, "zone_entries": zones_new, "loitering_events": loiter_new,
                  "ais_gap_events": gaps, "feed_coverage_hourly": coverage, "helicopter_landings": landings}
    dups = {t: duplicate_keys(df.where(F.col("dt") >= F.lit(check_from)), KEYS[t]) for t, df in recomputed.items()}
    inst_dim = build_installations(spark, cfg, P("bronze/reference/facilities.csv")).cache()
    dups["installations"] = (duplicate_keys(inst_dim.where("inst_id IS NOT NULL"), ["inst_id"])
                             + duplicate_keys(inst_dim, ["inst_name"]))
    _sp.end()
    repeated = {t: n for t, n in dups.items() if n}
    if repeated:     # a warning, not a gate check: Gold still publishes, the log, _READY.json and the trace say where
        warnings.append(f"repeated keys from {check_from}: " + ", ".join(f"{t}={n}" for t, n in repeated.items())
                        + "; run scripts/check_keys.py to see them")
    if raw_n is not None and raw_n - (s["n"] or 0) > 0:
        warnings.append(f"Silver held {raw_n - s['n']} duplicate (mmsi, event_ts) rows from {s_day}; Gold removed them, "
                        "but Silver's own deduplication should have: check the left_anti step in silver_job")

    # ---- quality gate: every recomputed table must reconcile with Silver for the same days ---------------
    checks = []
    def chk(name, ok, detail):
        checks.append({"check": name, "passed": bool(ok), "detail": detail})
    n_s = s["n"] or 0
    chk("silver_has_enough_rows", n_s >= gate["min_silver_rows"], f"{n_s} rows from {s_day} (min {gate['min_silver_rows']})")
    chk("silver_reject_rate_ok", rr is None or rr <= gate["max_reject_rate"], f"{rr} over {rr_in} input rows (max {gate['max_reject_rate']})")
    if gate["max_silver_staleness_min"] is not None:
        age = (datetime.now(timezone.utc) - s["max_ts"].replace(tzinfo=timezone.utc)).total_seconds() / 60
        chk("silver_fresh", age <= gate["max_silver_staleness_min"], f"newest Silver row is {age:.0f} min old")
    chk("visits_msgs_equal_silver_near_rows", (v["msgs"] or 0) == (sv["near_v"] or 0), f"rebuilt visits={v['msgs']} silver={sv['near_v']} (from {v_day})")
    chk("hourly_msgs_equal_silver_near_rows", (h["msgs"] or 0) == (s["near_s"] or 0), f"hourly={h['msgs']} silver={s['near_s']} (from {s_day})")
    chk("zone_msgs_reconcile", (v["zone"] or 0) == (sv["zone_v"] or 0) and (h["zone"] or 0) == (s["zone_s"] or 0),
        f"visits={v['zone']}/{sv['zone_v']} hourly={h['zone']}/{s['zone_s']}")
    chk("vessel_days_msgs_equal_silver_rows", (d["msgs"] or 0) == n_s, f"vessel_days={d['msgs']} silver={n_s}")
    chk("zone_entry_msgs_equal_silver_zone_rows", (z["msgs"] or 0) == (sv["zone_v"] or 0) and (z["neg"] or 0) == 0,
        f"zone entries={z['msgs']} silver={sv['zone_v']} negative={z['neg']}")
    chk("events_within_rules", (lo["msgs"] or 0) <= (sv["near_v"] or 0) and (lo["short"] or 0) == 0 and (g["bad"] or 0) == 0,
        f"loitering={lo['n']} (too short {lo['short']}), ais gaps={g['n']} (out of range {g['bad']})")
    chk("vessel_tracks_cover_every_vessel", (tr["n"] or 0) <= n_s and (tr["nopos"] or 0) == 0
        and (tr["vessels"] or 0) == (d["vessels"] or 0),
        f"tracks={tr['n']} rows for {tr['vessels']} vessels (vessel_days {d['vessels']}), missing positions={tr['nopos']}")
    chk("helicopter_landing_msgs_equal_silver_aircraft_zone_rows", (hl["msgs"] or 0) == (s["zone_air"] or 0),
        f"landings={hl['n']} ({hl['msgs']} msgs) silver aircraft in zones={s['zone_air']}")
    chk("no_negative_durations_or_null_keys", (v["neg"] or 0) == 0 and (v["nullkeys"] or 0) == 0, f"neg={v['neg']} nullkeys={v['nullkeys']}")
    ok = all(c["passed"] for c in checks)

    manifest_keys = {"check_from": str(check_from), "repeated_keys": dups}
    manifest = {"run_id": run_id, "env": cfg["env"], "started_at": started.isoformat(), "mode": "full" if full else "incremental",
                "recompute_from": str(s_day), "visits_from": str(v_day), "newest_day": str(max_dt),
                "silver_rows": n_s, "silver_rows_before_dedupe": raw_n,
                "duplicates_removed": (raw_n - n_s) if raw_n is not None else None,
                "open_visits_rebuilt": open_visits.count() if open_visits is not None else 0,
                "silver_max_event_ts": str(s["max_ts"]),
                "recomputed": {"vessel_visits": v["n"], "installation_activity_hourly": h["n"], "vessel_days": d["n"],
                               "zone_entries": z["n"], "loitering_events": lo["n"], "ais_gap_events": g["n"],
                               "vessel_tracks": tr["n"], "helicopter_landings": hl["n"]},
                "events_worth_a_look_recomputed": flags, "aircraft_rows": s["aircraft_rows"], "sog_glitches": s["glitches"],
                "sog_glitch_rate_24h": gr, "keys": manifest_keys, "warnings": warnings, "checks": checks}
    for w in warnings:
        obs.event("warning", detail=w); print("WARNING:", w)
    obs.annotate(run_id=run_id, silver_rows=n_s, vessel_visits=v["n"], hourly_rows=h["n"], gate_passed=ok,
                 failed_checks=[c["check"] for c in checks if not c["passed"]])
    for c in checks:
        obs.event("check", check=c["check"], passed=c["passed"], detail=c["detail"])
    os.makedirs(P("gold"), exist_ok=True)
    if not ok:
        obs.mark_error("quality gate failed")
        fin = datetime.now(timezone.utc)
        manifest.update(status="FAILED", finished_at=fin.isoformat(), runtime_s=round((fin - started).total_seconds(), 1))
        json.dump(manifest, open(P("gold/_LAST_FAILED.json"), "w"), indent=2)
        log_run(P, manifest)
        published = plan.read_json(P("gold/_READY.json"))
        if published:          # the page keeps showing the published Gold, and its health tab shows this failure
            build_dashboard(spark, cfg, P, published)
        print(json.dumps(manifest, indent=2)); sys.exit("Gold NOT published: quality checks failed")

    # ---- publish: staged days swapped in one partition at a time; dimensions rebuilt and swapped -----------
    _sp = obs.start("gold.publish")
    stage = P(f"gold/_staging/{run_id}")
    visits.write.mode("overwrite").partitionBy("dt").parquet(f"{stage}/vessel_visits")
    hourly.write.mode("overwrite").partitionBy("dt").parquet(f"{stage}/installation_activity_hourly")
    vdays.write.mode("overwrite").partitionBy("dt").parquet(f"{stage}/vessel_days")
    zones.write.mode("overwrite").partitionBy("dt").parquet(f"{stage}/zone_entries")
    loiter.write.mode("overwrite").partitionBy("dt").parquet(f"{stage}/loitering_events")
    gaps.write.mode("overwrite").partitionBy("dt").parquet(f"{stage}/ais_gap_events")
    coverage.write.mode("overwrite").partitionBy("dt").parquet(f"{stage}/feed_coverage_hourly")
    tracks.write.mode("overwrite").partitionBy("dt").parquet(f"{stage}/vessel_tracks")
    landings.write.mode("overwrite").partitionBy("dt").parquet(f"{stage}/helicopter_landings")
    days = {t: plan.days_between(v_day, max_dt) for t in REBUILT_FROM_OPEN_VISITS}
    for t in PARTITIONED:
        if full:
            plan.swap_dir(f"{stage}/{t}", P(f"gold/{t}"))
        else:
            plan.swap_partitions(f"{stage}/{t}", P(f"gold/{t}"), days.get(t, plan.days_between(s_day, max_dt)))
        plan.drop_partitions_before(P(f"gold/{t}"), window_start)
    vessels = flag_facility(build_vessels(spark.read.parquet(P("gold/vessel_days")) if plan.has_data(P("gold/vessel_days"))
                                          else vdays, sc.get("faulty_speed_min_count", 10), sc.get("faulty_speed_min_share", 0.01)),
                            fac_names, "name")
    reg_path = P("bronze/reference/fishing_vessels.csv")        # fishing register: matched here, read by the dashboard
    if os.path.exists(reg_path):
        reg = spark.read.option("header", True).option("escape", '"').csv(reg_path)
        vessels = match_fishing_register(vessels, reg, sc.get("fishing_length_tol_m", 3.0))
    else:
        vessels = no_fishing_register(vessels)
    # safety classification over everything published (partitions were swapped above): fleet, rescue craft, fishing
    zones_pub = read_table(spark, P("gold/zone_entries"))
    zones_pub = zones if zones_pub is None else zones_pub
    visits_pub = read_table(spark, P("gold/vessel_visits"))
    visits_pub = visits if visits_pub is None else visits_pub
    vessels = classify_vessels(vessels, zones_pub, visits_pub, cfg.get("offshore_fleet"))
    vessels.write.mode("overwrite").parquet(f"{stage}/vessels")
    ves_staged = spark.read.parquet(f"{stage}/vessels")
    fm = {r["fishing_match"]: r["count"] for r in ves_staged.groupBy("fishing_match").count().collect()}
    manifest["fishing_register"] = {"loaded": os.path.exists(reg_path), "strong": fm.get("strong", 0), "weak": fm.get("weak", 0)}
    assess_zone_entries(zones_pub, ves_staged, sc.get("zone_fast_kn", 10)).write.mode("overwrite").parquet(f"{stage}/safety_zone_entries")
    sz = spark.read.parquet(f"{stage}/safety_zone_entries").agg(
        F.count("*").alias("n"), F.sum(F.col("is_unexpected").cast("int")).alias("unexpected"),
        F.sum((F.col("risk_level") == "high").cast("int")).alias("high_risk")).collect()[0]
    manifest["safety"] = {"zone_entries": sz["n"], "unexpected": sz["unexpected"] or 0, "high_risk_fast": sz["high_risk"] or 0}
    inst_dim.write.mode("overwrite").parquet(f"{stage}/installations")
    whole_window_tables(spark, cfg, P, stage, manifest, hourly, coverage, newest, window_start)
    for t in DIMENSIONS:
        plan.swap_dir(f"{stage}/{t}", P(f"gold/{t}"))
    shutil.rmtree(P("gold/_staging"), ignore_errors=True)
    _sp.end()

    tables = {t: (read_table(spark, P(f"gold/{t}")).count() if plan.has_data(P(f"gold/{t}")) else 0)
              for t in PARTITIONED + DIMENSIONS}
    fin = datetime.now(timezone.utc)
    manifest.update(status="READY", tables=tables, finished_at=fin.isoformat(), runtime_s=round((fin - started).total_seconds(), 1))
    json.dump(manifest, open(P("gold/_READY.json"), "w"), indent=2)
    log_run(P, manifest)
    json.dump({"last_success_epoch": started.timestamp(), "last_success_at": started.isoformat(), "run_id": run_id,
               "layout": LAYOUT, "installation_names": names_hash},
              open(P("gold/_state.json"), "w"), indent=2)
    print(json.dumps(manifest, indent=2))
    try:                                            # pipeline loitering reads the published tracks; never fails Gold
        from spark import gold_pipelines
        with obs.span("gold.pipeline_loitering"):
            gold_pipelines.run(spark, cfg, P)
    except Exception as e:
        print(f"pipeline loitering not updated: {e!r}")
    build_dashboard(spark, cfg, P, manifest)


def traced_main():
    ap = argparse.ArgumentParser(description="Build and publish Gold")
    ap.add_argument("--full", action="store_true", help="rebuild the whole lookback window instead of changed days")
    a = ap.parse_args()
    cfg = pipeline_config.load()
    obs.init("ais-gold", cfg["env"])
    started = datetime.now(timezone.utc)
    try:
        with obs.span("gold.run"):
            main(full=a.full)
    except Exception as e:      # a crash (e.g. out of memory) is logged as a run too; a blocked gate exits via SystemExit
        fin = datetime.now(timezone.utc)
        log_run(lambda rel: pipeline_config.path(cfg, rel),
                {"started_at": started.isoformat(), "finished_at": fin.isoformat(), "status": "ERROR",
                 "runtime_s": round((fin - started).total_seconds(), 1), "mode": "full" if a.full else None,
                 "warnings": [f"Gold crashed: {e!r}"[:400]]})
        raise


if __name__ == "__main__":
    traced_main()
