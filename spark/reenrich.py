"""Re-enrich existing Silver with the current facility list and radii, without reprocessing Bronze.

Rows are unchanged; only the installation columns (inst_name, inst_kind, is_onshore, dist_m,
is_near_installation, is_at_installation, is_stationary) are recomputed with silver_logic.enrich.
Day by day (bounded memory). Writes to a SEPARATE folder (default silver/positions_reenriched) and never
modifies silver/positions. Rerun-safe: each processed day is overwritten in the output.

  APP_ENV=prod PYTHONPATH=. python -m spark.reenrich [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--driver-memory 3g]
"""
import argparse, json, time
from pyspark.sql import functions as F

import pipeline_config
from spark import silver_logic as L
from spark.common import load_facilities
from spark.session import get_spark

RECOMPUTED = ["inst_name", "inst_kind", "is_onshore", "dist_m", "is_near_installation",
              "is_at_installation", "is_stationary", "dt", "hour"]


def reenrich_day(rows, fac, cfg):
    """Drop the old installation columns and compute them again (dt/hour are recomputed from event_ts)."""
    v = rows.drop(*[c for c in RECOMPUTED if c in rows.columns])
    return L.enrich(v, fac, cfg["near_m"], cfg["at_m"], cfg["stationary_kn"], cfg.get("near_m_onshore"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="first")
    ap.add_argument("--to", dest="last")
    ap.add_argument("--out", default="silver/positions_reenriched")
    ap.add_argument("--driver-memory", default="3g")
    a = ap.parse_args()
    if a.out.rstrip("/") == "silver/positions":
        raise SystemExit("refusing to overwrite silver/positions; write elsewhere and swap later")
    cfg = pipeline_config.load()
    cfg["driver_memory"] = a.driver_memory
    P = lambda rel: pipeline_config.path(cfg, rel)
    spark = get_spark(cfg, "silver-reenrich")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    fac = load_facilities(spark, cfg, P("bronze/reference/facilities.csv")).cache()
    src = spark.read.option("mergeSchema", "true").parquet(P("silver/positions"))
    days = sorted(str(r[0]) for r in src.select("dt").distinct().collect())
    days = [d for d in days if (not a.first or d >= a.first) and (not a.last or d <= a.last)]
    print(json.dumps({"days": len(days), "first": days[0] if days else None,
                      "last": days[-1] if days else None, "facilities": fac.count()}), flush=True)
    S = lambda c: F.sum(F.col(c).cast("int"))
    for d in days:
        t0 = time.time()
        rows = src.where(F.col("dt") == F.lit(d).cast("date"))
        old = rows.agg(F.count("*").alias("n"), S("is_near_installation").alias("near"),
                       S("is_at_installation").alias("zone")).collect()[0]
        out = reenrich_day(rows, fac, cfg).select(*L.SILVER_COLUMNS).cache()
        new = out.agg(F.count("*").alias("n"), S("is_near_installation").alias("near"),
                      S("is_at_installation").alias("zone"), S("is_onshore").alias("onshore")).collect()[0]
        if new["n"] != old["n"]:
            raise SystemExit(f"STOP {d}: row count changed {old['n']} -> {new['n']}")
        out.coalesce(4).write.mode("overwrite").partitionBy("dt", "hour").parquet(P(a.out))
        out.unpersist()
        print(json.dumps({"dt": d, "rows": new["n"], "near_old": old["near"], "near_new": new["near"],
                          "zone_old": old["zone"], "zone_new": new["zone"], "onshore_rows": new["onshore"],
                          "runtime_s": round(time.time() - t0, 1)}), flush=True)


if __name__ == "__main__":
    main()
