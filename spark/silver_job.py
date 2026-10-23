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

import pipeline_config
from spark import silver_logic as L
from spark.common import load_facilities
from spark.session import get_spark


def main():
    cfg = pipeline_config.load()
    P = lambda rel: pipeline_config.path(cfg, rel)
    spark = get_spark(cfg, "silver")
    fac = load_facilities(spark, cfg, P("bronze/reference/facilities.csv")).cache()
    os.makedirs(P("quality"), exist_ok=True)

    def process(df, batch_id):
        if df.isEmpty():
            return
        t0 = time.time()
        flagged = L.validate(L.standardize(L.parse(df))).cache()
        q = L.quality_agg(flagged)                                   # BEFORE-processing quality (one action)

        (flagged.where("reject_reason is not null")
                .select(F.lit(batch_id).alias("batch_id"), "_run_id", "_file", "reject_reason", "payload", "ingest_ts")
                .write.mode("append").parquet(P("silver/rejects")))   # rejected rows are kept, with the reason

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

        out = (L.enrich(d2, fac, cfg["near_m"], cfg["at_m"], cfg["stationary_kn"])
                .select(*L.SILVER_COLUMNS).cache())
        a = out.agg(F.count("*").alias("n"), F.sum(F.col("is_near_installation").cast("int")).alias("near"),
                    F.sum(F.col("is_at_installation").cast("int")).alias("at_zone")).collect()[0]
        q.update(accepted_rows=int(a["n"]), near_installation_rows=int(a["near"] or 0), at_zone_rows=int(a["at_zone"] or 0))
        out.coalesce(1).write.mode("append").partitionBy("dt", "hour").parquet(P("silver/positions"))

        q.update(batch_id=int(batch_id), env=cfg["env"], processed_at=datetime.now(timezone.utc).isoformat(),
                 runtime_s=round(time.time() - t0, 2),
                 reject_rate=round(q["rejected_rows"] / max(q["input_rows"], 1), 4))
        with open(P("quality/silver_metrics.jsonl"), "a") as f:
            f.write(json.dumps(q) + "\n")
        print(json.dumps(q))
        for x in (flagged, d, out):
            x.unpersist()

    (spark.readStream.schema(L.BRONZE_SCHEMA)
          .option("maxFilesPerTrigger", str(cfg.get("max_files_per_trigger", 1000)))
          .option("pathGlobFilter", "*.jsonl.gz").option("recursiveFileLookup", "true")
          .json(P("bronze/raw"))
          .writeStream.foreachBatch(process).option("checkpointLocation", P("_checkpoints/silver"))
          .trigger(availableNow=True).start().awaitTermination())


if __name__ == "__main__":
    main()
