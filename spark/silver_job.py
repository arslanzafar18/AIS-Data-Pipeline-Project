"""
Silver job. Structured Streaming with trigger(availableNow=True): processes only NEW Bronze files
(checkpoint-tracked) and exits; the scheduler loop starts it every 120 s. Safe to rerun.

Per micro-batch: parse -> standardize -> validate -> (rejects saved with reason) -> dedupe (in-batch and
against existing Silver) -> enrich with nearest Sodir installation -> append to Silver.
Quality numbers for every batch are appended to <data_root>/quality/silver_metrics.jsonl.
"""
import json, os, time
from datetime import datetime, timezone
from pyspark.sql import functions as F

import observability as obs
import pipeline_config
from spark import silver_logic as L
from spark.common import load_facilities
from spark.session import get_spark


def main(src="bronze/raw", ckpt="_checkpoints/silver", max_files=None):
    cfg = pipeline_config.load()
    obs.init("ais-silver" if src == "bronze/raw" else "ais-silver-historic", cfg["env"])
    P = lambda rel: pipeline_config.path(cfg, rel)
    spark = get_spark(cfg, "silver")
    fac = load_facilities(spark, cfg, P("bronze/reference/facilities.csv")).cache()
    os.makedirs(P("quality"), exist_ok=True)

    def process(df, batch_id):
        if df.isEmpty():
            return
        with obs.span("silver.batch", batch_id=int(batch_id), env=cfg["env"], source=src):
            _process(df, batch_id)

    def _process(df, batch_id):
        t0 = time.time()
        flagged = L.validate(L.standardize(L.parse(df))).cache()
        with obs.span("silver.quality_agg"):
            q = L.quality_agg(flagged)                               # BEFORE-processing quality (one action)
        with obs.span("silver.lineage"):
            files = sorted(r[0] for r in flagged.select("_file").distinct().collect() if r[0])
        obs.annotate(**{"bronze.file_count": len(files), "bronze.first_file": files[0] if files else "",
                        "bronze.last_file": files[-1] if files else ""})

        with obs.span("silver.write_rejects"):
            (flagged.where("reject_reason is not null")
                    .select(F.lit(batch_id).alias("batch_id"), "_run_id", "_file", "reject_reason", "payload", "ingest_ts")
                    .write.mode("append").parquet(P("silver/rejects")))   # rejected rows are kept, with the reason

        with obs.span("silver.dedupe"):
            d = L.dedupe(flagged.where("reject_reason is null")).cache()
            n_dedup = d.count()
            q["in_batch_duplicates"] = q["valid_rows"] - n_dedup
            q["cross_batch_duplicates"] = 0
            if cfg["cross_batch_dedupe"] and os.path.exists(P("silver/positions")):
                dts = [r[0] for r in d.select(F.to_date("event_ts")).distinct().collect()]
                keys = spark.read.parquet(P("silver/positions")).where(F.col("dt").isin(dts)).select("mmsi", "event_ts")
                d2 = d.join(keys, ["mmsi", "event_ts"], "left_anti").cache()
                q["cross_batch_duplicates"] = n_dedup - d2.count()
            else:
                d2 = d

        with obs.span("silver.enrich"):
            out = (L.enrich(d2, fac, cfg["near_m"], cfg["at_m"], cfg["stationary_kn"], cfg.get("near_m_onshore"))
                    .select(*L.SILVER_COLUMNS).cache())
            a = out.agg(F.count("*").alias("n"), F.sum(F.col("is_near_installation").cast("int")).alias("near"),
                        F.sum(F.col("is_at_installation").cast("int")).alias("at_zone")).collect()[0]
        q.update(accepted_rows=int(a["n"]), near_installation_rows=int(a["near"] or 0), at_zone_rows=int(a["at_zone"] or 0))
        with obs.span("silver.write"):
            out.coalesce(1).write.mode("append").partitionBy("dt", "hour").parquet(P("silver/positions"))

        q.update(batch_id=int(batch_id), env=cfg["env"], source=("live" if src == "bronze/raw" else "historic"), processed_at=datetime.now(timezone.utc).isoformat(),
                 runtime_s=round(time.time() - t0, 2),
                 reject_rate=round(q["rejected_rows"] / max(q["input_rows"], 1), 4))
        with open(P("quality/silver_metrics.jsonl"), "a") as f:
            f.write(json.dumps(q) + "\n")
        print(json.dumps(q))
        obs.annotate(**{f"q.{k}": v for k, v in q.items()})
        for x in (flagged, d, out):
            x.unpersist()

    (spark.readStream.schema(L.BRONZE_SCHEMA)
          .option("maxFilesPerTrigger", str(max_files or cfg.get("max_files_per_trigger", 1000)))
          .option("pathGlobFilter", "*.jsonl.gz").option("recursiveFileLookup", "true")
          .json(P(src))
          .writeStream.foreachBatch(process).option("checkpointLocation", P(ckpt))
          .trigger(availableNow=True).start().awaitTermination())


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="bronze/raw")
    ap.add_argument("--ckpt", default="_checkpoints/silver")
    ap.add_argument("--max-files", type=int, default=None)
    a = ap.parse_args()
    main(a.src, a.ckpt, a.max_files)
