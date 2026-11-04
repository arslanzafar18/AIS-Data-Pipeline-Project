"""
Gold job: builds consumer tables from Silver and PUBLISHES THEM ONLY IF QUALITY CHECKS PASS.
  gold/vessel_visits                   one row per vessel visit to an installation
  gold/installation_activity_hourly    vessels / messages / speed per installation per hour
Publish protocol: write to gold/_staging/<run>, run checks, swap into place, write gold/_READY.json
(consumers read that manifest). On failure the previous Gold stays untouched, gold/_LAST_FAILED.json is
written and the process exits non-zero.  Run hourly.
"""
import json, os, shutil, sys, uuid
from datetime import datetime, timezone, timedelta
from pyspark import StorageLevel
from pyspark.sql import functions as F

import observability as obs
import pipeline_config
from spark.gold_logic import build_hourly, build_visits
from spark.session import get_spark


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


def _dupe_check_due(path, hours):
    """True if Gold should run its full-table duplicate check now (None/0 hours = every run, the old behaviour)."""
    if not hours:
        return True
    try:
        last = datetime.fromisoformat(json.load(open(path))["checked_at"])
    except (OSError, ValueError, KeyError, TypeError):
        return True
    return datetime.now(timezone.utc) - last >= timedelta(hours=hours)


def main():
    cfg = pipeline_config.load()
    cfg["lookback_days"] = int(os.environ.get("GOLD_LOOKBACK_DAYS", cfg["lookback_days"]))
    P = lambda rel: pipeline_config.path(cfg, rel)
    gate = cfg["gate"]
    run_id = uuid.uuid4().hex[:10]
    spark = get_spark(cfg, "gold")
    if cfg.get("gold_shuffle_partitions"):          # Gold shuffles the whole lookback; Silver keeps its small value
        spark.conf.set("spark.sql.shuffle.partitions", str(cfg["gold_shuffle_partitions"]))

    silver = spark.read.option("mergeSchema", "true").parquet(P("silver/positions"))
    max_dt = silver.agg(F.max("dt")).collect()[0][0]
    silver = silver.where(F.col("dt") >= F.date_sub(F.lit(max_dt), cfg["lookback_days"] - 1))
    # Silver already dedupes within and across batches (prod: 52,521,133 rows before and after Gold's dedupe),
    # so the full-table shuffle runs only once per dupe_check_hours, as a safety check.
    check_path = P("quality/gold_dupe_check.json")
    due = _dupe_check_due(check_path, cfg.get("dupe_check_hours"))
    deduped = silver.dropDuplicates(["mmsi", "event_ts"]).persist(StorageLevel.MEMORY_AND_DISK) if due else silver
    near = deduped.where("is_near_installation").cache()

    visits = build_visits(near, cfg["visit_gap_s"]).cache()
    hourly = build_hourly(near).cache()

    _sp = obs.start("gold.silver_stats")   # includes reading Silver + dedupe
    s = deduped.agg(F.count("*").alias("n"), F.max("event_ts").alias("max_ts"),
                    F.sum(F.col("is_near_installation").cast("int")).alias("near"),
                    F.sum(F.col("is_at_installation").cast("int")).alias("zone")).collect()[0]
    _sp.end(); _sp = obs.start("gold.build_visits")
    v = visits.agg(F.count("*").alias("n"), F.sum("n_msgs").alias("msgs"), F.sum("msgs_in_zone").alias("zone"),
                   F.sum((F.col("duration_min") < 0).cast("int")).alias("neg"),
                   F.sum(F.col("mmsi").isNull().cast("int") + F.col("inst_name").isNull().cast("int")).alias("nullkeys")).collect()[0]
    _sp.end(); _sp = obs.start("gold.build_hourly")
    h = hourly.agg(F.count("*").alias("n"), F.sum("position_msgs").alias("msgs"), F.sum("msgs_in_zone").alias("zone")).collect()[0]
    _sp.end(); _sp = obs.start("gold.count_raw")
    raw_n = silver.count() if due else None
    if due:
        os.makedirs(P("quality"), exist_ok=True)
        json.dump({"checked_at": datetime.now(timezone.utc).isoformat(), "rows": raw_n, "unique": s["n"],
                   "duplicates": raw_n - s["n"]}, open(check_path, "w"), indent=2)
    _sp.end()
    rr, rr_in = silver_reject_rate(cfg, P("quality/silver_metrics.jsonl"), cfg["lookback_days"])

    checks = []
    def chk(name, ok, detail):
        checks.append({"check": name, "passed": bool(ok), "detail": detail})
    chk("silver_has_enough_rows", s["n"] >= gate["min_silver_rows"], f"{s['n']} rows (min {gate['min_silver_rows']})")
    chk("silver_reject_rate_ok", rr is None or rr <= gate["max_reject_rate"], f"{rr} over {rr_in} input rows (max {gate['max_reject_rate']})")
    if gate["max_silver_staleness_min"] is not None:
        age = (datetime.now(timezone.utc) - s["max_ts"].replace(tzinfo=timezone.utc)).total_seconds() / 60
        chk("silver_fresh", age <= gate["max_silver_staleness_min"], f"newest Silver row is {age:.0f} min old")
    chk("visits_msgs_equal_silver_near_rows", (v["msgs"] or 0) == (s["near"] or 0), f"visits={v['msgs']} silver={s['near']}")
    chk("hourly_msgs_equal_silver_near_rows", (h["msgs"] or 0) == (s["near"] or 0), f"hourly={h['msgs']} silver={s['near']}")
    chk("zone_msgs_reconcile", (v["zone"] or 0) == (s["zone"] or 0) == (h["zone"] or 0), f"visits={v['zone']} hourly={h['zone']} silver={s['zone']}")
    chk("no_negative_durations_or_null_keys", (v["neg"] or 0) == 0 and (v["nullkeys"] or 0) == 0, f"neg={v['neg']} nullkeys={v['nullkeys']}")
    ok = all(c["passed"] for c in checks)

    manifest = {"run_id": run_id, "env": cfg["env"], "finished_at": datetime.now(timezone.utc).isoformat(),
                "silver_rows": s["n"], "silver_rows_before_dedupe": raw_n, "dupe_check_ran": due, "silver_max_event_ts": str(s["max_ts"]),
                "tables": {"vessel_visits": v["n"], "installation_activity_hourly": h["n"]}, "checks": checks}
    obs.annotate(run_id=run_id, silver_rows=s["n"], vessel_visits=v["n"], hourly_rows=h["n"], gate_passed=ok,
                 failed_checks=[c["check"] for c in checks if not c["passed"]])
    for c in checks:
        obs.event("check", check=c["check"], passed=c["passed"], detail=c["detail"])
    os.makedirs(P("gold"), exist_ok=True)
    if not ok:
        obs.mark_error("quality gate failed")
        manifest["status"] = "FAILED"
        json.dump(manifest, open(P("gold/_LAST_FAILED.json"), "w"), indent=2)
        print(json.dumps(manifest, indent=2)); sys.exit("Gold NOT published: quality checks failed")

    _sp = obs.start("gold.publish")
    stage = P(f"gold/_staging/{run_id}")
    visits.write.mode("overwrite").parquet(f"{stage}/vessel_visits")
    hourly.write.mode("overwrite").parquet(f"{stage}/installation_activity_hourly")
    for t in ("vessel_visits", "installation_activity_hourly"):
        final, prev = P(f"gold/{t}"), P(f"gold/_prev_{t}")
        if os.path.exists(final):
            shutil.move(final, prev)
        shutil.move(f"{stage}/{t}", final)
        shutil.rmtree(prev, ignore_errors=True)
    shutil.rmtree(P("gold/_staging"), ignore_errors=True)
    _sp.end()
    manifest["status"] = "READY"
    json.dump(manifest, open(P("gold/_READY.json"), "w"), indent=2)
    print(json.dumps(manifest, indent=2))


def traced_main():
    obs.init("ais-gold", pipeline_config.load()["env"])
    with obs.span("gold.run"):
        main()


if __name__ == "__main__":
    traced_main()
