"""
Gold job: builds consumer tables from Silver and PUBLISHES THEM ONLY IF QUALITY CHECKS PASS.

  gold/vessel_visits                 one row per vessel visit to an installation        (partitioned by arrival day)
  gold/installation_activity_hourly  vessels / messages / speed per installation-hour   (partitioned by day)
  gold/vessel_days                   one row per vessel per day, from all positions     (partitioned by day)
  gold/vessels                       one row per vessel: latest identity, first/last seen
  gold/installations                 one row per installation in the facility filter, with field and position
  gold/zone_entries                  one row per entry into a 500 m safety zone                (by entry day)
  gold/loitering_events              slow (< loiter_max_kn) within 5 km for >= loiter_min_minutes (by start day)
  gold/ais_gap_events                vessel silent >= ais_gap_min_minutes between two positions near an
                                     installation, nothing sent anywhere in between             (by end day)
  gold/feed_coverage_hourly          messages and vessels per hour over all positions          (by day)
  Event tables carry: regular_visitor (present at that installation on >= regular_visitor.min_days distinct days
  in the regular_visitor.window_days before the event), is_facility (the "vessel" is a platform or rig in Sodir's
  list broadcasting AIS), censored (started right after a feed gap or the start of the data) and, for AIS gaps,
  feed_outage (the whole feed was quiet during the gap).

INCREMENTAL: a run recomputes only from the earliest changed day S (the last `gold_recent_days` days, plus any older
day whose Silver files changed since the last successful run) to the newest day; a published visit still open at S (a
standby vessel can stay for weeks) is rebuilt from its own arrival, reading older days only for that vessel and
installation, so no visit is cut in two and the other, final visits on those days are kept as published. Recomputed days
are written to gold/_staging/<run>, checked against Silver for the same days, then swapped in one day at a time;
days older than lookback_days are dropped. --full (and the first run on unpartitioned tables) rebuilds everything.
On failure the previous Gold stays untouched, gold/_LAST_FAILED.json is written and the process exits non-zero.
"""
import argparse, json, os, shutil, sys, uuid
from datetime import datetime, timedelta, timezone
from pyspark import StorageLevel
from pyspark.sql import functions as F

import observability as obs
import pipeline_config
from spark import gold_plan as plan
from spark.gold_logic import (build_ais_gaps, build_coverage, build_hourly, build_installations, build_loitering,
                              build_vessel_days, build_vessels, build_visits, build_zone_entries, covered_hours,
                              flag_censored, flag_facility, flag_outage, flag_regular)
from spark.session import get_spark

PARTITIONED = ("vessel_visits", "installation_activity_hourly", "vessel_days",
               "zone_entries", "loitering_events", "ais_gap_events", "feed_coverage_hourly")
LAYOUT = "dt-partitioned-v3"      # bump when tables are added or change shape: the next run is then a full rebuild
REBUILT_FROM_OPEN_VISITS = {"vessel_visits": "arrival_ts", "zone_entries": "entry_ts", "loitering_events": "start_ts"}
DIMENSIONS = ("vessels", "installations")


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

    # ---- plan: which days to recompute -------------------------------------------------------------------
    _sp = obs.start("gold.plan")
    day_mtimes = plan.silver_day_mtimes(P("silver/positions"))
    if not day_mtimes:
        sys.exit("Gold: no Silver data yet")
    max_dt = max(day_mtimes)
    window_start = max_dt - timedelta(days=cfg["lookback_days"] - 1)
    state = plan.read_json(P("gold/_state.json")) or {}
    last_ok = state.get("last_success_epoch")
    full = bool(full or last_ok is None or state.get("layout") != LAYOUT
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
    base = spark.read.option("mergeSchema", "true").parquet(P("silver/positions"))
    raw = base.where((F.col("dt") >= F.lit(s_day)) & (F.col("dt") <= F.lit(max_dt)))
    if full:      # whole window: Silver already dedupes across batches, so skip the full-table shuffle
        read, raw_n = raw, None
    else:         # a few days: deduplicate as a safety net, it is cheap at this size
        raw_n = raw.count()
        read = raw.dropDuplicates(["mmsi", "event_ts"]).persist(StorageLevel.MEMORY_AND_DISK)
    near = read.where("is_near_installation").cache()
    near_v = near
    if v_day < s_day:
        early = (base.where((F.col("dt") >= F.lit(v_day)) & (F.col("dt") < F.lit(s_day)) & F.col("is_near_installation"))
                     .join(F.broadcast(open_visits), ["mmsi", "inst_name"])
                     .where(F.col("event_ts") >= F.col("open_from")).drop("open_from")
                     .dropDuplicates(["mmsi", "event_ts"]))
        near_v = near.unionByName(early, allowMissingColumns=True).cache()

    visits_new = build_visits(near_v, cfg["visit_gap_s"]).withColumn("dt", F.to_date("arrival_ts")).cache()
    visits = visits_new
    if v_day < s_day:   # older days = published visits minus the rebuilt open ones, plus their rebuilt versions
        visits = union_kept(visits_new, kept_published(spark, P("gold/vessel_visits"), "arrival_ts",
                                                       open_visits, v_day, s_day)).cache()
    hourly = build_hourly(near).withColumn("dt", F.to_date("hour_ts")).cache()
    vdays = build_vessel_days(read).cache()
    coverage = build_coverage(read).cache()

    # ---- use cases 2 and 3: zone entries, loitering, AIS gaps --------------------------------------------
    sc, rv = cfg.get("surveillance", {}), cfg.get("regular_visitor", {"min_days": 3, "window_days": 30})
    zone_gap = min(cfg.get("zone_gap_s", cfg["visit_gap_s"]), cfg["visit_gap_s"])   # an open entry implies an open visit
    gap_min_s, gap_max_s = int(sc.get("ais_gap_min_minutes", 30) * 60), int(sc.get("ais_gap_max_hours", 12) * 3600)
    s_start_ts = f"{s_day} 00:00:00"
    zones_new = build_zone_entries(near_v, zone_gap).withColumn("dt", F.to_date("entry_ts")).cache()
    loiter_new = build_loitering(near_v, cfg["visit_gap_s"], sc.get("loiter_max_kn", 2.0), sc.get("loiter_min_minutes", 120))
    cols = [c for c in ("mmsi", "event_ts", "is_near_installation", "inst_name", "inst_kind", "is_onshore", "dist_m",
                        "name", "shipType") if c in read.columns]
    before = (base.where((F.col("dt") == F.date_sub(F.lit(s_day), 1))     # each vessel's messages just before S
                  & (F.col("event_ts") >= F.to_timestamp(F.lit(s_start_ts)) - F.expr(f"INTERVAL {gap_max_s} SECONDS")))
                  .select(*cols).dropDuplicates(["mmsi", "event_ts"]))
    gaps_new = build_ais_gaps(read.select(*cols).unionByName(before), gap_min_s, gap_max_s, s_start_ts)
    history = visits                                                           # visits used for the regular-visitor flag
    older = None if full else read_table(spark, P("gold/vessel_visits"))
    if older is not None:
        history = visits.unionByName(older.where(
            (F.col("dt") < F.lit(v_day)) & (F.col("dt") >= F.date_sub(F.lit(v_day), int(rv["window_days"]) + 1))))
    # flags: Sodir names (all facilities, fixed and mobile) and hourly feed coverage over the whole window
    fac_names = (spark.read.option("header", True).csv(P("bronze/reference/facilities.csv"))
                      .select(F.col(cfg.get("facility_name_col", "fclName")).alias("fac")))
    cov_all = coverage
    older_cov = None if full else read_table(spark, P("gold/feed_coverage_hourly"))
    if older_cov is not None:
        cov_all = coverage.unionByName(older_cov.where((F.col("dt") < F.lit(s_day)) & (F.col("dt") >= F.lit(window_start))))
    newest = read.agg(F.max("event_ts")).collect()[0][0]
    ok_hours = covered_hours(cov_all, sc.get("coverage_min_fraction", 0.5), newest).cache()
    zones_new = flag_censored(flag_facility(zones_new, fac_names), ok_hours, "entry_ts").cache()
    loiter_new = flag_regular(loiter_new, history, "start_ts", rv["min_days"], rv["window_days"])
    loiter_new = flag_censored(flag_facility(loiter_new, fac_names), ok_hours, "start_ts").withColumn("dt", F.to_date("start_ts")).cache()
    gaps = flag_regular(gaps_new, history, "end_ts", rv["min_days"], rv["window_days"])
    gaps = flag_outage(flag_facility(gaps, fac_names), ok_hours, "start_ts", "end_ts").withColumn("dt", F.to_date("end_ts")).cache()
    zones, loiter = zones_new, loiter_new   # (zones_new keeps the same rows: the flags only add columns)
    if v_day < s_day:
        zones = union_kept(zones_new, kept_published(spark, P("gold/zone_entries"), "entry_ts", open_visits, v_day, s_day)).cache()
        loiter = union_kept(loiter_new, kept_published(spark, P("gold/loitering_events"), "start_ts", open_visits, v_day, s_day)).cache()

    _sp = obs.start("gold.silver_stats")
    s = read.agg(F.count("*").alias("n"), F.max("event_ts").alias("max_ts"),
                 F.sum(F.col("is_near_installation").cast("int")).alias("near_s"),
                 F.sum(F.col("is_at_installation").cast("int")).alias("zone_s")).collect()[0]
    sv = near_v.agg(F.count("*").alias("near_v"), F.sum(F.col("is_at_installation").cast("int")).alias("zone_v")).collect()[0]
    _sp.end(); _sp = obs.start("gold.build_visits")
    v = visits_new.agg(F.count("*").alias("n"), F.sum("n_msgs").alias("msgs"), F.sum("msgs_in_zone").alias("zone"),
                   F.sum((F.col("duration_min") < 0).cast("int")).alias("neg"),
                   F.sum(F.col("mmsi").isNull().cast("int") + F.col("inst_name").isNull().cast("int")).alias("nullkeys")).collect()[0]
    _sp.end(); _sp = obs.start("gold.build_hourly")
    h = hourly.agg(F.count("*").alias("n"), F.sum("position_msgs").alias("msgs"), F.sum("msgs_in_zone").alias("zone")).collect()[0]
    _sp.end(); _sp = obs.start("gold.build_vessel_days")
    d = vdays.agg(F.count("*").alias("n"), F.sum("n_msgs").alias("msgs")).collect()[0]
    _sp.end(); _sp = obs.start("gold.build_events")
    z = zones_new.agg(F.count("*").alias("n"), F.sum("n_msgs").alias("msgs"),
                      F.sum((F.col("minutes_inside") < 0).cast("int")).alias("neg")).collect()[0]
    lo = loiter_new.agg(F.count("*").alias("n"), F.sum("n_msgs").alias("msgs"),
                        F.sum((F.col("duration_min") < sc.get("loiter_min_minutes", 120)).cast("int")).alias("short")).collect()[0]
    flags = {"loitering_worth_a_look": loiter_new.where("NOT regular_visitor AND NOT is_facility AND NOT censored").count(),
             "ais_gaps_worth_a_look": gaps.where("NOT regular_visitor AND NOT is_facility AND NOT feed_outage").count()}
    g = gaps.agg(F.count("*").alias("n"), F.sum(((F.col("duration_min") * 60 <= gap_min_s) |
                                                 (F.col("duration_min") * 60 > gap_max_s)).cast("int")).alias("bad")).collect()[0]
    _sp.end()
    rr, rr_in = silver_reject_rate(cfg, P("quality/silver_metrics.jsonl"), cfg["lookback_days"])

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
    chk("no_negative_durations_or_null_keys", (v["neg"] or 0) == 0 and (v["nullkeys"] or 0) == 0, f"neg={v['neg']} nullkeys={v['nullkeys']}")
    ok = all(c["passed"] for c in checks)

    manifest = {"run_id": run_id, "env": cfg["env"], "mode": "full" if full else "incremental",
                "recompute_from": str(s_day), "visits_from": str(v_day), "newest_day": str(max_dt),
                "silver_rows": n_s, "silver_rows_before_dedupe": raw_n,
                "duplicates_removed": (raw_n - n_s) if raw_n is not None else None,
                "open_visits_rebuilt": open_visits.count() if open_visits is not None else 0,
                "silver_max_event_ts": str(s["max_ts"]),
                "recomputed": {"vessel_visits": v["n"], "installation_activity_hourly": h["n"], "vessel_days": d["n"],
                               "zone_entries": z["n"], "loitering_events": lo["n"], "ais_gap_events": g["n"]},
                "events_worth_a_look_recomputed": flags, "checks": checks}
    obs.annotate(run_id=run_id, silver_rows=n_s, vessel_visits=v["n"], hourly_rows=h["n"], gate_passed=ok,
                 failed_checks=[c["check"] for c in checks if not c["passed"]])
    for c in checks:
        obs.event("check", check=c["check"], passed=c["passed"], detail=c["detail"])
    os.makedirs(P("gold"), exist_ok=True)
    if not ok:
        obs.mark_error("quality gate failed")
        manifest["status"] = "FAILED"; manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
        json.dump(manifest, open(P("gold/_LAST_FAILED.json"), "w"), indent=2)
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
    days = {t: plan.days_between(v_day, max_dt) for t in REBUILT_FROM_OPEN_VISITS}
    for t in PARTITIONED:
        if full:
            plan.swap_dir(f"{stage}/{t}", P(f"gold/{t}"))
        else:
            plan.swap_partitions(f"{stage}/{t}", P(f"gold/{t}"), days.get(t, plan.days_between(s_day, max_dt)))
        plan.drop_partitions_before(P(f"gold/{t}"), window_start)
    flag_facility(build_vessels(spark.read.parquet(P("gold/vessel_days")) if plan.has_data(P("gold/vessel_days"))
                                else vdays), fac_names, "name").write.mode("overwrite").parquet(f"{stage}/vessels")
    build_installations(spark, cfg, P("bronze/reference/facilities.csv")).write.mode("overwrite").parquet(f"{stage}/installations")
    for t in DIMENSIONS:
        plan.swap_dir(f"{stage}/{t}", P(f"gold/{t}"))
    shutil.rmtree(P("gold/_staging"), ignore_errors=True)
    _sp.end()

    tables = {t: (read_table(spark, P(f"gold/{t}")).count() if plan.has_data(P(f"gold/{t}")) else 0)
              for t in PARTITIONED + DIMENSIONS}
    manifest.update(status="READY", tables=tables, finished_at=datetime.now(timezone.utc).isoformat())
    json.dump(manifest, open(P("gold/_READY.json"), "w"), indent=2)
    json.dump({"last_success_epoch": started.timestamp(), "last_success_at": started.isoformat(), "run_id": run_id,
               "layout": LAYOUT},
              open(P("gold/_state.json"), "w"), indent=2)
    print(json.dumps(manifest, indent=2))
    try:                                            # the dashboard must never fail Gold
        from spark import dashboard
        from spark.common import load_facilities
        with obs.span("gold.dashboard"):
            fac = load_facilities(spark, dict(cfg, facility_filter=None), P("bronze/reference/facilities.csv"))
            dashboard.build(spark.read.parquet(P("gold/vessel_visits")),
                            spark.read.parquet(P("gold/installation_activity_hourly")), manifest, P("gold/dashboard"), fac=fac)
    except Exception as e:
        print(f"dashboard not updated: {e!r}")


def traced_main():
    ap = argparse.ArgumentParser(description="Build and publish Gold")
    ap.add_argument("--full", action="store_true", help="rebuild the whole lookback window instead of changed days")
    a = ap.parse_args()
    obs.init("ais-gold", pipeline_config.load()["env"])
    with obs.span("gold.run"):
        main(full=a.full)


if __name__ == "__main__":
    traced_main()
