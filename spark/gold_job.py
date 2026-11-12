"""
Gold job: builds consumer tables from Silver and PUBLISHES THEM ONLY IF QUALITY CHECKS PASS.

  gold/vessel_visits                 one row per vessel visit to an installation        (partitioned by arrival day)
  gold/installation_activity_hourly  vessels / messages / speed per installation-hour   (partitioned by day)
  gold/vessel_days                   one row per vessel per day, from all positions     (partitioned by day)
  gold/vessels                       one row per vessel: latest identity, first/last seen
  gold/installations                 one row per installation in the facility filter, with field and position

INCREMENTAL: a run recomputes only from the earliest changed day S (the last `gold_recent_days` days, plus any older
day whose Silver files changed since the last successful run) to the newest day; visits are rebuilt from an earlier
day when a published visit was still open at S, so no visit is cut in two (see spark/gold_plan.py). Recomputed days
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
from spark.gold_logic import (build_hourly, build_installations, build_vessel_days, build_vessels, build_visits)
from spark.session import get_spark

PARTITIONED = ("vessel_visits", "installation_activity_hourly", "vessel_days")
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
    full = bool(full or last_ok is None or not all(plan.is_partitioned(P(f"gold/{t}")) for t in PARTITIONED))
    if full:
        s_day = v_day = window_start
    else:
        s_day = plan.plan_start(day_mtimes, max_dt, window_start, cfg.get("gold_recent_days", 2), last_ok)
        open_visits = [(r[0], r[1]) for r in spark.read.parquet(P("gold/vessel_visits"))
                       .where(F.col("departure_ts") >= F.lit(window_start))
                       .select(F.unix_timestamp("arrival_ts"), F.unix_timestamp("departure_ts")).collect()]
        v_day = plan.visit_rebuild_start(open_visits, s_day, cfg["visit_gap_s"], window_start)
    obs.annotate(mode="full" if full else "incremental", recompute_from=str(s_day), visits_from=str(v_day))
    _sp.end()

    # ---- read Silver for the recompute range (visits need from v_day, everything else from s_day) ---------
    raw = (spark.read.option("mergeSchema", "true").parquet(P("silver/positions"))
                .where((F.col("dt") >= F.lit(v_day)) & (F.col("dt") <= F.lit(max_dt))))
    if full:      # whole window: Silver already dedupes across batches, so skip the full-table shuffle
        read, raw_n = raw, None
    else:         # a few days: deduplicate as a safety net, it is cheap at this size
        raw_n = raw.count()
        read = raw.dropDuplicates(["mmsi", "event_ts"]).persist(StorageLevel.MEMORY_AND_DISK)
    in_s = F.col("dt") >= F.lit(s_day)
    near = read.where("is_near_installation").cache()

    visits = build_visits(near, cfg["visit_gap_s"]).withColumn("dt", F.to_date("arrival_ts")).cache()
    hourly = build_hourly(near.where(in_s)).withColumn("dt", F.to_date("hour_ts")).cache()
    vdays = build_vessel_days(read.where(in_s)).cache()

    _sp = obs.start("gold.silver_stats")
    s = read.agg(F.count("*").alias("n_read"), F.sum(in_s.cast("int")).alias("n"), F.max("event_ts").alias("max_ts"),
                 F.sum(F.col("is_near_installation").cast("int")).alias("near_v"),
                 F.sum(F.col("is_at_installation").cast("int")).alias("zone_v"),
                 F.sum((in_s & F.col("is_near_installation")).cast("int")).alias("near_s"),
                 F.sum((in_s & F.col("is_at_installation")).cast("int")).alias("zone_s")).collect()[0]
    _sp.end(); _sp = obs.start("gold.build_visits")
    v = visits.agg(F.count("*").alias("n"), F.sum("n_msgs").alias("msgs"), F.sum("msgs_in_zone").alias("zone"),
                   F.sum((F.col("duration_min") < 0).cast("int")).alias("neg"),
                   F.sum(F.col("mmsi").isNull().cast("int") + F.col("inst_name").isNull().cast("int")).alias("nullkeys")).collect()[0]
    _sp.end(); _sp = obs.start("gold.build_hourly")
    h = hourly.agg(F.count("*").alias("n"), F.sum("position_msgs").alias("msgs"), F.sum("msgs_in_zone").alias("zone")).collect()[0]
    _sp.end(); _sp = obs.start("gold.build_vessel_days")
    d = vdays.agg(F.count("*").alias("n"), F.sum("n_msgs").alias("msgs")).collect()[0]
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
    chk("visits_msgs_equal_silver_near_rows", (v["msgs"] or 0) == (s["near_v"] or 0), f"visits={v['msgs']} silver={s['near_v']} (from {v_day})")
    chk("hourly_msgs_equal_silver_near_rows", (h["msgs"] or 0) == (s["near_s"] or 0), f"hourly={h['msgs']} silver={s['near_s']} (from {s_day})")
    chk("zone_msgs_reconcile", (v["zone"] or 0) == (s["zone_v"] or 0) and (h["zone"] or 0) == (s["zone_s"] or 0),
        f"visits={v['zone']}/{s['zone_v']} hourly={h['zone']}/{s['zone_s']}")
    chk("vessel_days_msgs_equal_silver_rows", (d["msgs"] or 0) == n_s, f"vessel_days={d['msgs']} silver={n_s}")
    chk("no_negative_durations_or_null_keys", (v["neg"] or 0) == 0 and (v["nullkeys"] or 0) == 0, f"neg={v['neg']} nullkeys={v['nullkeys']}")
    ok = all(c["passed"] for c in checks)

    manifest = {"run_id": run_id, "env": cfg["env"], "mode": "full" if full else "incremental",
                "recompute_from": str(s_day), "visits_from": str(v_day), "newest_day": str(max_dt),
                "silver_rows": n_s, "silver_rows_before_dedupe": raw_n,
                "duplicates_removed": (raw_n - (s["n_read"] or 0)) if raw_n is not None else None,
                "silver_max_event_ts": str(s["max_ts"]),
                "recomputed": {"vessel_visits": v["n"], "installation_activity_hourly": h["n"], "vessel_days": d["n"]},
                "checks": checks}
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
    days = {"vessel_visits": plan.days_between(v_day, max_dt)}
    for t in PARTITIONED:
        if full:
            plan.swap_dir(f"{stage}/{t}", P(f"gold/{t}"))
        else:
            plan.swap_partitions(f"{stage}/{t}", P(f"gold/{t}"), days.get(t, plan.days_between(s_day, max_dt)))
        plan.drop_partitions_before(P(f"gold/{t}"), window_start)
    build_vessels(spark.read.parquet(P("gold/vessel_days"))).write.mode("overwrite").parquet(f"{stage}/vessels")
    build_installations(spark, cfg, P("bronze/reference/facilities.csv")).write.mode("overwrite").parquet(f"{stage}/installations")
    for t in DIMENSIONS:
        plan.swap_dir(f"{stage}/{t}", P(f"gold/{t}"))
    shutil.rmtree(P("gold/_staging"), ignore_errors=True)
    _sp.end()

    tables = {t: spark.read.parquet(P(f"gold/{t}")).count() for t in PARTITIONED + DIMENSIONS}
    manifest.update(status="READY", tables=tables, finished_at=datetime.now(timezone.utc).isoformat())
    json.dump(manifest, open(P("gold/_READY.json"), "w"), indent=2)
    json.dump({"last_success_epoch": started.timestamp(), "last_success_at": started.isoformat(), "run_id": run_id},
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
