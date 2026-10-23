"""
Backfill probe (step 2). Order area + time range at https://hais.kystverket.no, download the
(Geo)Parquet file(s), then:  python scripts/probe_historic.py path/to/hais_*.parquet
Uses PySpark (no pandas): prints rows, file size, bytes/row, columns, time range.
"""
import os, sys
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("probe").master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
for p in sys.argv[1:]:
    df = spark.read.parquet(p)
    n = df.count()
    mb = os.path.getsize(p) / 1e6
    print(f"{p}: {n:,} rows, {mb:.1f} MB, ~{mb*1e6/max(n,1):.0f} B/row")
    print("columns:", df.columns)
    tcol = next((c for c in df.columns if "time" in c.lower() or "date" in c.lower()), None)
    if tcol:
        df.agg(F.min(tcol).alias("from"), F.max(tcol).alias("to")).show(truncate=False)
