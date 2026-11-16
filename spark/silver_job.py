"""
Silver job. Structured Streaming with trigger(availableNow=True): processes only NEW Bronze files
(checkpoint-tracked) and exits; the scheduler loop starts it every 120 s. Safe to rerun.

Per micro-batch: parse -> standardize -> validate -> (rejects saved with reason) -> dedupe (in-batch and
against existing Silver) -> enrich with nearest Sodir installation -> append to Silver.
Quality numbers for every batch are appended to <data_root>/quality/silver_metrics.jsonl.
"""
import json, os, time
from datetime import datetime, timezone
from types import SimpleNamespace
from pyspark.sql import functions as F

import observability as obs
import pipeline_config
from spark import silver_logic as L
from spark.common import load_facilities
from spark.session import get_spark


def clean(df, job):
    """Parse the raw JSON, standardise the values and mark each row with its first failing rule (or none)."""
    cfg = job.cfg
    parsed = L.parse(df)
    standard = L.standardize(parsed, cfg.get("max_ship_kn", 50.0))
    return L.validate(standard, cfg.get("max_event_age_days", 365)).cache()


def write_rejects(flagged, batch_id, job):
    """Rejected rows are kept, with their reason and raw payload, so a rule can be checked against real cases."""
    with obs.span("silver.write_rejects"):
        (flagged.where("reject_reason is not null")
                .select(F.lit(batch_id).alias("batch_id"), "_run_id", "_file", "reject_reason", "payload", "ingest_ts")
                .write.mode("append").parquet(job.P("silver/rejects")))


def remove_duplicates(flagged, q, job):
    """One row per (mmsi, event_ts): first within the batch, then against Silver rows already stored for the same
    days (a left anti join). Returns (rows after the batch dedupe, rows after both) and counts both in q."""
    P = job.P
    with obs.span("silver.dedupe"):
        in_batch = L.dedupe(flagged.where("reject_reason is null")).cache()
        n = in_batch.count()
        q["in_batch_duplicates"] = q["valid_rows"] - n
        q["cross_batch_duplicates"] = 0
        if not (job.cfg["cross_batch_dedupe"] and os.path.exists(P("silver/positions"))):
            return in_batch, in_batch
        days = [r[0] for r in in_batch.select(F.to_date("event_ts")).distinct().collect()]
        stored = job.spark.read.parquet(P("silver/positions")).where(F.col("dt").isin(days)).select("mmsi", "event_ts")
        new = in_batch.join(stored, ["mmsi", "event_ts"], "left_anti").cache()
        q["cross_batch_duplicates"] = n - new.count()
        return in_batch, new


def enrich_and_write(rows, q, job):
    """Add the nearest installation (Sodir number, name, distance, inside 500 m) and append to Silver."""
    cfg = job.cfg
    with obs.span("silver.enrich"):
        out = (L.enrich(rows, job.fac, cfg["near_m"], cfg["at_m"], cfg["stationary_kn"], cfg.get("near_m_onshore"))
                .select(*L.SILVER_COLUMNS).cache())
        a = out.agg(F.count("*").alias("n"), F.sum(F.col("is_near_installation").cast("int")).alias("near"),
                    F.sum(F.col("is_at_installation").cast("int")).alias("at_zone")).collect()[0]
    q.update(accepted_rows=int(a["n"]), near_installation_rows=int(a["near"] or 0), at_zone_rows=int(a["at_zone"] or 0))
    with obs.span("silver.write"):
        out.coalesce(1).write.mode("append").partitionBy("dt", "hour").parquet(job.P("silver/positions"))
    return out


def save_quality(q, batch_id, started, job):
    """Append this batch's quality numbers to quality/silver_metrics.jsonl (read by Gold's gate and the health tab)."""
    q.update(batch_id=int(batch_id), env=job.cfg["env"], source=("live" if job.src == "bronze/raw" else "historic"),
             processed_at=datetime.now(timezone.utc).isoformat(), runtime_s=round(time.time() - started, 2),
             reject_rate=round(q["rejected_rows"] / max(q["input_rows"], 1), 4))
    with open(job.P("quality/silver_metrics.jsonl"), "a") as f:
        f.write(json.dumps(q) + "\n")
    print(json.dumps(q))
    obs.annotate(**{f"q.{k}": v for k, v in q.items()})


def process_batch(df, batch_id, job):
    """One micro-batch of new Bronze files, from raw text to Silver rows plus its quality record."""
    started = time.time()
    df = df.cache()
    with obs.span("silver.bronze_columns"):
        columns = L.bronze_columns(df)                       # does Bronze still have the fields we expect?
    flagged = clean(df, job)
    with obs.span("silver.quality_agg"):
        q = L.quality_agg(flagged)                           # quality before processing, in one Spark action
    q.update(columns)
    with obs.span("silver.lineage"):
        files = sorted(r[0] for r in flagged.select("_file").distinct().collect() if r[0])
    obs.annotate(**{"bronze.file_count": len(files), "bronze.first_file": files[0] if files else "",
                    "bronze.last_file": files[-1] if files else ""})
    write_rejects(flagged, batch_id, job)
    in_batch, new = remove_duplicates(flagged, q, job)
    out = enrich_and_write(new, q, job)
    save_quality(q, batch_id, started, job)
    for x in (flagged, in_batch, out, df):
        x.unpersist()


def main(src="bronze/raw", ckpt="_checkpoints/silver", max_files=None):
    cfg = pipeline_config.load()
    obs.init("ais-silver" if src == "bronze/raw" else "ais-silver-historic", cfg["env"])
    P = lambda rel: pipeline_config.path(cfg, rel)
    spark = get_spark(cfg, "silver")
    job = SimpleNamespace(cfg=cfg, P=P, spark=spark, src=src,
                          fac=load_facilities(spark, cfg, P("bronze/reference/facilities.csv")).cache())
    os.makedirs(P("quality"), exist_ok=True)

    def on_batch(df, batch_id):
        if df.isEmpty():
            return
        with obs.span("silver.batch", batch_id=int(batch_id), env=cfg["env"], source=src):
            process_batch(df, batch_id, job)

    (spark.readStream.schema(L.BRONZE_SCHEMA)
          .option("maxFilesPerTrigger", str(max_files or cfg.get("max_files_per_trigger", 1000)))
          .option("pathGlobFilter", "*.jsonl.gz").option("recursiveFileLookup", "true")
          .json(P(src))
          .writeStream.foreachBatch(on_batch).option("checkpointLocation", P(ckpt))
          .trigger(availableNow=True).start().awaitTermination())


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="bronze/raw")
    ap.add_argument("--ckpt", default="_checkpoints/silver")
    ap.add_argument("--max-files", type=int, default=None)
    a = ap.parse_args()
    main(a.src, a.ckpt, a.max_files)
