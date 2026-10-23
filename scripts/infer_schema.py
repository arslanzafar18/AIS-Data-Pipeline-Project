"""Run once on the first Bronze files to see the REAL field names inside `payload`."""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))   # run from anywhere
import glob, gzip, json, collections
import pipeline_config
cfg = pipeline_config.load()
files = sorted(glob.glob(pipeline_config.path(cfg, "bronze/raw") + "/**/*.jsonl.gz", recursive=True))
if not files: raise SystemExit("No Bronze files yet - start the collector first")
keys, n = collections.Counter(), 0
with gzip.open(files[0], "rt") as fh:
    for line in fh:
        try: keys.update(json.loads(json.loads(line)["payload"]).keys()); n += 1
        except Exception: pass
print(f"{files[0]}: {n} messages parsed")
for k, v in keys.most_common(): print(f"{k:28s}{v}")
