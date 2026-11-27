"""
Dry run of the split Silver rules on Bronze files. Writes NOTHING; prints what would happen to every input row.

    PYTHONPATH=. python scripts/silver_dryrun.py --src "data/prod/bronze/raw/dt=2026-11-10/hour=*/*.gz"
    PYTHONPATH=. python scripts/silver_dryrun.py --src "data/prod/bronze/historic_json/dt=2026-10-12/*/*.gz" --sample 0.5

Use it to MEASURE the reject / suspect / non-vessel shares before turning the new tables on, and quote the measured
numbers (they come from the data, they are not tuned to a target).
"""
import argparse
from pyspark.sql import SparkSession
from spark.silver_logic import BRONZE_SCHEMA, parse, standardize, validate
from spark.silver_tables import classify_mmsi, stage_rows, fate_counts

ap = argparse.ArgumentParser()
ap.add_argument("--src", required=True, help="Bronze JSON(L) path or glob (gzip is fine)")
ap.add_argument("--sample", type=float, default=1.0, help="fraction of rows to use, e.g. 0.2 (deterministic seed)")
args = ap.parse_args()

spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

bronze = spark.read.schema(BRONZE_SCHEMA).json(args.src)
if args.sample < 1.0:
    bronze = bronze.sample(False, args.sample, seed=42)
flagged = validate(classify_mmsi(standardize(parse(bronze)))).persist()

rows = fate_counts(stage_rows(flagged)).collect()
total = sum(r["count"] for r in rows)
print(f"\ninput rows: {total:,}\n")
print(f"{'outcome':<12}{'reason':<20}{'rows':>14}{'share':>9}")
by_outcome = {}
for r in rows:
    by_outcome[r["outcome"]] = by_outcome.get(r["outcome"], 0) + r["count"]
    print(f"{r['outcome']:<12}{str(r['reason'] or ''):<20}{r['count']:>14,}{100.0 * r['count'] / total:>8.2f}%")
print("\nshare of input rows by outcome")
for k in ("rejected", "duplicate", "non_vessel", "suspect", "position"):
    print(f"  {k:<11}{by_outcome.get(k, 0):>14,}{100.0 * by_outcome.get(k, 0) / total:>8.2f}%")
not_pos = total - by_outcome.get("position", 0)
print(f"  {'not a position':<11}{not_pos:>11,}{100.0 * not_pos / total:>8.2f}%   (everything that did not become a clean vessel position)")
spark.stop()
