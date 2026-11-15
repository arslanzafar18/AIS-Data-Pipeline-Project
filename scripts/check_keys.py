"""
Read-only key report: for every published Gold table, how many key values occur more than once (the same keys the
Gold gate checks, spark.gold_job.KEYS), plus Silver's key (mmsi, event_ts) on the newest days and the facility list's
Sodir numbers and names. Writes nothing. Use it before changing a key rule, or when gold_keys_unique fails.
  APP_ENV=prod PYTHONPATH=. python scripts/check_keys.py [--silver-days 2]
"""
import argparse, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pyspark.sql import functions as F

import pipeline_config
from spark import gold_plan as plan
from spark.common import load_facilities
from spark.gold_job import KEYS
from spark.gold_logic import duplicate_keys
from spark.session import get_spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-days", type=int, default=2)
    a = ap.parse_args()
    cfg = pipeline_config.load()
    P = lambda rel: pipeline_config.path(cfg, rel)
    spark = get_spark(cfg, "check-keys")
    out = {"gold": {}, "silver": None, "facilities": None}
    for t, keys in KEYS.items():
        path = P(f"gold/{t}")
        if plan.has_data(path):
            df = spark.read.parquet(path)
            out["gold"][t] = {"rows": df.count(), "duplicate_keys": duplicate_keys(df, keys), "key": keys}
    days = sorted(plan.silver_day_mtimes(P("silver/positions")))[-a.silver_days:]
    if days:
        s = spark.read.parquet(P("silver/positions")).where(F.col("dt") >= F.lit(days[0]))
        out["silver"] = {"from": str(days[0]), "rows": s.count(), "duplicate_keys": duplicate_keys(s, ["mmsi", "event_ts"])}
    fac_path = P("bronze/reference/facilities.csv")
    if os.path.exists(fac_path):
        f = load_facilities(spark, cfg, fac_path)
        out["facilities"] = {"rows": f.count(), "without_number": f.where("inst_id IS NULL").count(),
                             "repeated_numbers": duplicate_keys(f.where("inst_id IS NOT NULL"), ["inst_id"]),
                             "repeated_names": duplicate_keys(f, ["inst_name"]),
                             "names_used_twice": [r["inst_name"] for r in
                                                  f.groupBy("inst_name").count().where("count > 1").limit(20).collect()]}
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
