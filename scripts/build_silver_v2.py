"""
Builds the split Silver tables from the existing Silver positions (read-only), one day at a time, into <root>/silver_v2.
The live Silver and Gold are not touched. Re-running a day replaces that day's partitions.

  positions              clean vessel positions (non-vessel stations and suspect fixes removed)
  non_vessel_positions   aircraft, aids to navigation, beacons, coast stations, buoys and other stations
  suspect_positions      null island, outside the data area, lone position spike
  position_installation  vessel positions near an installation (the rows Gold needs)
  vessel_static              one row per MMSI: latest name, ship type, IMO, call sign, size, name changes
  installation_ref        Sodir installation reference (inst_id, name, kind, onshore flag, position)
Rejects stay in silver/rejects.

  PYTHONPATH=. python scripts/build_silver_v2.py --from-dt 2026-11-23 --to-dt 2026-11-23
"""
import argparse, json, os
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession, functions as F

import pipeline_config
from spark import silver_tables as T
from spark.common import load_facilities

# columns the split needs; older partitions may not have them, so missing ones are added as nulls
NEEDED = {"altitude": "int", "inst_id": "long", "inst_name": "string", "inst_kind": "string", "is_onshore": "boolean",
          "dist_m": "double", "is_near_installation": "boolean", "is_at_installation": "boolean",
          "is_stationary": "boolean", "speedOverGround": "double", "name": "string", "shipType": "int",
          "imo": "long", "callSign": "string", "shipLength": "int", "shipWidth": "int"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.expanduser("~/AIS-Data-Pipeline-Project/data/prod"),
                    help="data root that holds silver/ and bronze/reference/")
    ap.add_argument("--from-dt")
    ap.add_argument("--last-days", type=int, help="build the last N days up to --to-dt (used by the daily timer)")
    ap.add_argument("--to-dt", default=(datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d"),
                    help="last day to build (default: yesterday, because today is still being written)")
    a = ap.parse_args()
    if a.last_days:
        a.from_dt = (datetime.now(timezone.utc) - timedelta(days=a.last_days)).strftime("%Y-%m-%d")
    src, out = f"{a.root}/silver/positions", f"{a.root}/silver_v2"
    cfg = pipeline_config.load("prod")
    spark = (SparkSession.builder.appName("silver_v2").master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "16").config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    days = sorted(d[3:] for d in os.listdir(src) if d.startswith("dt="))
    days = [d for d in days if (not a.from_dt or d >= a.from_dt) and d <= a.to_dt]
    print(f"{len(days)} days to build into {out}")
    if not days:
        return
    os.makedirs(out, exist_ok=True)
    log = open(f"{out}/_build_log.jsonl", "a")

    def write(df, name):
        df.write.mode("overwrite").partitionBy("dt", "hour").parquet(f"{out}/{name}")

    for day in days:
        d = spark.read.option("basePath", src).parquet(f"{src}/dt={day}")
        for c, t in NEEDED.items():
            if c not in d.columns:
                d = d.withColumn(c, F.lit(None).cast(t))
        d = T.classify_mmsi(d).persist()
        non_vessel = d.where(F.col("mmsi_class") != "vessel")
        flagged = T.flag_suspect(d.where(F.col("mmsi_class") == "vessel")).persist()
        suspect = flagged.where("suspect_reason IS NOT NULL")
        pos = flagged.where("suspect_reason IS NULL").drop("suspect_reason", "mmsi_class").persist()

        write(pos, "positions")
        write(non_vessel, "non_vessel_positions")
        write(suspect, "suspect_positions")
        write(T.installation_proximity(pos), "position_installation")
        T.vessel_static_observations(pos).write.mode("overwrite").parquet(f"{out}/_vessel_obs/dt={day}")

        n_in, n_pos, n_nv, n_su = d.count(), pos.count(), non_vessel.count(), suspect.count()
        assert n_pos + n_nv + n_su == n_in, f"{day}: the three outcomes do not add up to the input rows"
        rec = {"dt": day, "input_rows": n_in, "positions": n_pos, "non_vessel": n_nv, "suspect": n_su,
               "position_installation": pos.where("is_near_installation").count(),
               "non_vessel_by_class": {r["mmsi_class"]: r["count"] for r in non_vessel.groupBy("mmsi_class").count().collect()},
               "suspect_by_reason": {r["suspect_reason"]: r["count"] for r in suspect.groupBy("suspect_reason").count().collect()},
               "built_at": datetime.now(timezone.utc).isoformat()}
        log.write(json.dumps(rec) + "\n")
        log.flush()
        print(json.dumps(rec))
        for x in (d, flagged, pos):
            x.unpersist()

    T.build_vessels(spark.read.parquet(f"{out}/_vessel_obs")).coalesce(1).write.mode("overwrite").parquet(f"{out}/vessel_static")
    fac = load_facilities(spark, cfg, f"{a.root}/bronze/reference/facilities.csv")
    fac.coalesce(1).write.mode("overwrite").parquet(f"{out}/installation_ref")
    for name in ("positions", "non_vessel_positions", "suspect_positions", "position_installation", "vessel_static", "installation_ref"):
        print(f"{name:<22}{spark.read.parquet(f'{out}/{name}').count():>14,}")
    spark.stop()


if __name__ == "__main__":
    main()
