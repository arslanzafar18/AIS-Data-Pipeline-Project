"""
Historic loader (batch path). Reads Kystverket HAIS daily Parquet files (kept unchanged under bronze/historic/)
and writes live-format Bronze envelope files (*.jsonl.gz) to bronze/historic_json/ for the normal Silver code.
A manifest records source, size, checksum, rows, ingest time and run id per file; finished files are skipped on rerun.
Usage: PYTHONPATH=. python spark/historic_job.py [--static-from data/prod/silver/positions] [--limit 1]
"""
import argparse, glob, hashlib, json, os, uuid
from datetime import datetime, timezone

import pipeline_config
from spark import historic_logic as H
from spark.session import get_spark


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def finalize(out_dir):
    """Spark names files part-*.json.gz; Silver reads *.jsonl.gz. Rename and drop checksum/marker files."""
    n = 0
    for root, _, names in os.walk(out_dir):
        for fn in names:
            p = os.path.join(root, fn)
            if fn.endswith(".crc") or fn == "_SUCCESS":
                os.remove(p)
            elif fn.endswith(".json.gz"):
                os.rename(p, os.path.join(root, "hais_" + fn[:-len(".json.gz")] + ".jsonl.gz"))
                n += 1
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="bronze/historic/position")
    ap.add_argument("--out", default="bronze/historic_json")
    ap.add_argument("--static-from", default=None, help="live Silver path used to fill name/shipType/... by MMSI")
    ap.add_argument("--limit", type=int, default=None, help="process only the first N files")
    ap.add_argument("--order-note", default="")
    ap.add_argument("--allow-prod", action="store_true")
    a = ap.parse_args()

    cfg = pipeline_config.load()
    if cfg["env"] != "dev" and not a.allow_prod:
        raise SystemExit(f"refusing to run with env={cfg['env']}; test in dev first")
    P = lambda rel: pipeline_config.path(cfg, rel)
    spark = get_spark(cfg, "historic")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    print("data_root:", cfg["data_root"], "| env:", cfg["env"])

    static = H.load_static(spark, a.static_from).cache() if a.static_from else None
    if static is not None:
        print("static vessel records from live Silver:", static.count())

    man_p = P("bronze/historic/manifest.json")
    man = json.load(open(man_p)) if os.path.exists(man_p) else {"source": H.SOURCE, "files": []}
    man["order_note"] = a.order_note or man.get("order_note", "")
    done = {x["file"] for x in man["files"]}
    run_id, ingest_ts = uuid.uuid4().hex[:12], datetime.now(timezone.utc).isoformat()

    files = sorted(glob.glob(P(a.src) + "/*.parquet"))
    todo = [f for f in files if os.path.basename(f) not in done][: a.limit]
    print(f"{len(files)} files found, {len(todo)} to process, run_id={run_id}")
    for f in todo:
        name = os.path.basename(f)
        raw = spark.read.parquet(f)
        rows = raw.count()
        (H.to_envelope(raw, name, run_id, ingest_ts, static)
          .repartition("dt", "hour").write.mode("append").option("compression", "gzip")
          .partitionBy("dt", "hour").json(P(a.out)))
        n = finalize(P(a.out))
        man["files"].append({"file": name, "rows": rows, "bytes": os.path.getsize(f), "sha256": sha256(f),
                             "ingest_ts": ingest_ts, "run_id": run_id, "output_files": n,
                             "static_filled": static is not None})
        os.makedirs(os.path.dirname(man_p), exist_ok=True)
        json.dump(man, open(man_p, "w"), indent=2)
        print(f"{name}: {rows:,} rows -> {n} files")
    spark.stop()


if __name__ == "__main__":
    main()
