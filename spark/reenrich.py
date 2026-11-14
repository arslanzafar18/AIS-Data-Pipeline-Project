"""Re-enrich existing Silver with the current facility list, radii and row-level rules, without reprocessing Bronze.

Rows are unchanged; recomputed per row:
  - installation columns (inst_name, inst_kind, is_onshore, dist_m, is_near_installation, is_at_installation,
    is_stationary) with silver_logic.enrich;
  - is_aircraft (SAR-aircraft MMSI range, or an altitude where Silver already has one);
  - the ship speed cap: speedOverGround above max_ship_kn on a non-aircraft row is nulled and flagged sog_glitch.
Altitude cannot be recovered here for rows written before Silver kept it (it is only in Bronze); it stays null.
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


def refresh_flags(v, max_ship_kn):
    """Applies today's row-level rules to rows written before they existed (and is a no-op on rows that have them)."""
    alt = F.col("altitude") if "altitude" in v.columns else F.lit(None).cast("int")
    air = F.coalesce(F.col("mmsi").between(*L.AIRCRAFT_MMSI) | alt.isNotNull(), F.lit(False))
    before = F.coalesce(F.col("sog_glitch"), F.lit(False)) if "sog_glitch" in v.columns else F.lit(False)
    glitch = before | (F.coalesce(F.col("speedOverGround") > max_ship_kn, F.lit(False)) & ~air)
    return (v.withColumn("altitude", alt).withColumn("is_aircraft", air).withColumn("sog_glitch", glitch)
             .withColumn("speedOverGround", F.when(F.col("sog_glitch"), None).otherwise(F.col("speedOverGround"))))


def reenrich_day(rows, fac, cfg):
    """Drop the old installation columns, refresh the row-level flags, and enrich again."""
    v = rows.drop(*[c for c in RECOMPUTED if c in rows.columns])
    v = refresh_flags(v, cfg.get("max_ship_kn", 50.0))
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
                      S("is_at_installation").alias("zone"), S("is_onshore").alias("onshore"),
                      S("is_aircraft").alias("aircraft"), S("sog_glitch").alias("glitches")).collect()[0]
        if new["n"] != old["n"]:
            raise SystemExit(f"STOP {d}: row count changed {old['n']} -> {new['n']}")
        out.coalesce(4).write.mode("overwrite").partitionBy("dt", "hour").parquet(P(a.out))
        out.unpersist()
        print(json.dumps({"dt": d, "rows": new["n"], "near_old": old["near"], "near_new": new["near"],
                          "zone_old": old["zone"], "zone_new": new["zone"], "onshore_rows": new["onshore"],
                          "aircraft_rows": new["aircraft"], "sog_glitches": new["glitches"],
                          "runtime_s": round(time.time() - t0, 1)}), flush=True)


if __name__ == "__main__":
    main()
