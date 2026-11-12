"""
Gold job: builds consumer tables from Silver and PUBLISHES THEM ONLY IF QUALITY CHECKS PASS.

  gold/vessel_visits                 one row per vessel visit to an installation        (partitioned by arrival day)
  gold/installation_activity_hourly  vessels / messages / speed per installation-hour   (partitioned by day)
  gold/vessel_days                   one row per vessel per day, from all positions     (partitioned by day)
  gold/vessels                       one row per vessel: latest identity, first/last seen
  gold/installations                 one row per installation in the facility filter, with field and position

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
    open_visits = None
    if full:
        s_day = v_day = window_start
    else:
        s_day = plan.plan_start(day_mtimes, max_dt, window_start, cfg.get("gold_recent_days", 2), last_ok)
        s_start = f"{s_day} 00:00:00"
        cut = (datetime.fromisoformat(s_start) - timedelta(seconds=cfg["visit_gap_s"])).isoformat(sep=" ")
        # Published visits still open at S (e.g. a standby vessel staying for weeks): only THESE are rebuilt from
        # their arrival; every other visit on those older days is already final and is kept as published.
        open_visits = (spark.read.parquet(P("gold/vessel_visits"))
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
        kept = (spark.read.parquet(P("gold/vessel_visits"))
                     .where((F.col("dt") >= F.lit(v_day)) & (F.col("dt") < F.lit(s_day)))
                     .join(open_visits.withColumnRenamed("open_from", "arrival_ts"), ["mmsi", "inst_name", "arrival_ts"], "left_anti"))
        visits = visits_new.unionByName(kept).cache()
    hourly = build_hourly(near).withColumn("dt", F.to_date("hour_ts")).cache()
    vdays = build_vessel_days(read).cache()

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
    chk("no_negative_durations_or_null_keys", (v["neg"] or 0) == 0 and (v["nullkeys"] or 0) == 0, f"neg={v['neg']} nullkeys={v['nullkeys']}")
    ok = all(c["passed"] for c in checks)

    manifest = {"run_id": run_id, "env": cfg["env"], "mode": "full" if full else "incremental",
                "recompute_from": str(s_day), "visits_from": str(v_day), "newest_day": str(max_dt),
                "silver_rows": n_s, "silver_rows_before_dedupe": raw_n,
                "duplicates_removed": (raw_n - n_s) if raw_n is not None else None,
                "open_visits_rebuilt": open_visits.count() if open_visits is not None else 0,
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
