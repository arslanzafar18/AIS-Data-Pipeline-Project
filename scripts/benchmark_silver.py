"""
Evidence for Silver performance decisions (run on SYNTHETIC demo data scaled up; local mode, 1 core).
 1) enrichment join: naive haversine on every (position x installation) pair  vs  bounding-box prefilter
 2) shuffle partitions: Spark default (200) vs configured small value for this data volume
Prints wall time (best of 2) and writes the physical plan to docs/evidence/enrich_plan.txt.
Usage: APP_ENV=dev PYTHONPATH=. python scripts/benchmark_silver.py [scale] [n_installations]
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))   # run from anywhere
import random, sys, time
from pyspark.sql import functions as F

import pipeline_config
from spark import silver_logic as L
from spark.common import haversine_m
from spark.session import get_spark

scale = int(sys.argv[1]) if len(sys.argv) > 1 else 10
n_inst = int(sys.argv[2]) if len(sys.argv) > 2 else 300
cfg = pipeline_config.load()
spark = get_spark(cfg, "bench")

bronze = (spark.read.schema(L.BRONZE_SCHEMA).option("recursiveFileLookup", "true").option("pathGlobFilter", "*.jsonl.gz")
          .json(pipeline_config.path(cfg, "bronze/raw")))
base = L.dedupe(L.validate(L.standardize(L.parse(bronze))).where("reject_reason is null"))
v = (base.crossJoin(spark.range(scale).withColumnRenamed("id", "k"))
         .withColumn("mmsi", F.col("mmsi") + F.col("k") * 1000).drop("k").cache())
rows = v.count()
random.seed(1)                      # installations scattered over the Norwegian shelf box
fac = spark.createDataFrame([(f"I{i}", "PLATFORM", random.uniform(56, 71), random.uniform(1, 10)) for i in range(n_inst)],
                            "inst_name string, inst_kind string, i_lat double, i_lon double").cache()
fac.count()

def near_pairs(v, fac, prefilter):
    """Join step only (same output in both variants): all (position, installation) pairs within near_m."""
    j = v.select("mmsi", "event_ts", "latitude", "longitude").crossJoin(F.broadcast(fac))
    if prefilter:
        dlat = cfg["near_m"] / 111000.0 * 1.05
        j = j.where((F.abs(F.col("latitude") - F.col("i_lat")) < dlat) & (F.abs(F.col("longitude") - F.col("i_lon")) < dlat / 0.30))
    return (j.withColumn("dist_m", haversine_m(F.col("latitude"), F.col("longitude"), F.col("i_lat"), F.col("i_lon")))
             .where(F.col("dist_m") <= cfg["near_m"]))

def prefiltered(v, fac):
    return L.enrich(v, fac, cfg["near_m"], cfg["at_m"], cfg["stationary_kn"])

def timed(label, fn, shuffle):
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle))
    best = 1e9
    for _ in range(2):
        t = time.time(); n = fn().count(); best = min(best, time.time() - t)
    print(f"{label:<62}{best:>8.2f} s   (result rows {n})")

print(f"positions: {rows:,}   installations: {n_inst}   pairs evaluated by naive join: {rows*n_inst:,}")
timed("A  join step: haversine on ALL pairs, then filter", lambda: near_pairs(v, fac, False), cfg["shuffle_partitions"])
timed("B  join step: bounding-box prefilter, then haversine", lambda: near_pairs(v, fac, True), cfg["shuffle_partitions"])
timed("C  full enrich (join + nearest + left join), shuffle=%d" % cfg["shuffle_partitions"], lambda: prefiltered(v, fac), cfg["shuffle_partitions"])
timed("C' full enrich with Spark default shuffle partitions (200)", lambda: prefiltered(v, fac), 200)
with open("docs/evidence/enrich_plan.txt", "w") as f:
    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        prefiltered(v, fac).explain()
    f.write(buf.getvalue())
print("plan written to docs/evidence/enrich_plan.txt")
