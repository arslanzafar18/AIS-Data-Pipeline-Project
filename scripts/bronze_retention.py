"""Bronze retention: delete live Bronze files older than `bronze_retention_days` (config), but ONLY files the
Silver stream has already processed (listed in its checkpoint). Dry run by default; --apply deletes.

Why: Bronze grows every 2 minutes and the Silver stream lists every Bronze file on each run, so old files cost
disk space and Silver time. Silver keeps every row, and spark.reenrich applies new row rules to Silver, so old
raw files are only needed to recover fields Silver does not keep (see docs/DESIGN_AND_LIMITS.md).

  APP_ENV=prod PYTHONPATH=. python scripts/bronze_retention.py [--days 30] [--apply]
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))   # run from anywhere
import argparse, json, os
from datetime import date, datetime, timedelta, timezone
from urllib.parse import unquote, urlparse

import pipeline_config


def processed_files(ckpt_sources):
    """Basenames of every file the Silver stream committed (FileStreamSource log: 'v1' then one JSON per line)."""
    names = set()
    root = pathlib.Path(ckpt_sources)
    if not root.exists():
        return names
    for f in root.rglob("*"):
        if not f.is_file() or f.name.startswith("."):
            continue
        for line in f.read_text(errors="replace").splitlines():
            if line.startswith("{"):
                try:
                    p = json.loads(line)["path"]
                except (ValueError, KeyError):
                    continue
                names.add(os.path.basename(unquote(urlparse(p).path)))
    return names


def plan(raw_dir, cutoff, processed):
    """Files in dt=... partitions strictly older than cutoff: (deletable, kept because not yet processed)."""
    delete, keep = [], []
    for d in sorted(pathlib.Path(raw_dir).glob("dt=*")):
        try:
            day = date.fromisoformat(d.name[3:])
        except ValueError:
            continue
        if day >= cutoff:
            continue
        for f in sorted(d.rglob("*.jsonl.gz")):
            (delete if f.name in processed else keep).append(f)
    return delete, keep


def remove_empty_dirs(raw_dir, cutoff):
    for d in sorted(pathlib.Path(raw_dir).glob("dt=*")):
        try:
            if date.fromisoformat(d.name[3:]) >= cutoff:
                continue
        except ValueError:
            continue
        for sub in sorted(d.rglob("*"), reverse=True):
            if sub.is_dir() and not any(sub.iterdir()):
                sub.rmdir()
        if not any(d.iterdir()):
            d.rmdir()


def main(argv=None):
    cfg = pipeline_config.load()
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=int(cfg.get("bronze_retention_days", 30)))
    ap.add_argument("--apply", action="store_true", help="delete (default: only report what would be deleted)")
    a = ap.parse_args(argv)
    if a.days < 7:
        sys.exit("refusing: keep at least 7 days of Bronze")
    raw = pipeline_config.path(cfg, "bronze/raw")
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=a.days)
    processed = processed_files(pipeline_config.path(cfg, "_checkpoints/silver/sources"))
    delete, keep = plan(raw, cutoff, processed)
    size = sum(f.stat().st_size for f in delete)
    if a.apply:
        for f in delete:
            f.unlink()
        remove_empty_dirs(raw, cutoff)
    print(json.dumps({"env": cfg["env"], "mode": "apply" if a.apply else "dry-run", "keep_days": a.days,
                      "deleted_before": cutoff.isoformat(), "files": len(delete), "gb": round(size / 1e9, 2),
                      "kept_not_processed": len(keep), "processed_in_checkpoint": len(processed)}))
    if keep:
        print(f"WARNING: {len(keep)} old files are not in the Silver checkpoint and were kept, e.g. {keep[0]}")


if __name__ == "__main__":
    main()
