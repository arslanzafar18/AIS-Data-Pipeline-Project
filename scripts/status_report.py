"""Operator view: collector heartbeat, latest Silver quality, reject reasons, Gold readiness. Stdlib only."""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))   # run from anywhere
import json, os
from datetime import datetime, timezone
import pipeline_config

cfg = pipeline_config.load()
P = lambda r: pipeline_config.path(cfg, r)
print(f"== environment: {cfg['env']} ==")
hb = os.path.join(cfg["metrics_dir"], "heartbeat.json")
if os.path.exists(hb):
    h = json.load(open(hb)); age = (datetime.now(timezone.utc) - datetime.fromisoformat(h["ts"])).total_seconds()
    print(f"collector: last file {h['last_file']} ({age:.0f}s ago)")
else:
    print("collector: no heartbeat file")
mp = P("quality/silver_metrics.jsonl")
if os.path.exists(mp):
    M = [json.loads(l) for l in open(mp)]
    tot = lambda k: sum((m.get(k) or 0) for m in M)
    print(f"silver: {len(M)} batches, input {tot('input_rows')}, accepted {tot('accepted_rows')}, "
          f"rejected {tot('rejected_rows')} ({tot('rejected_rows')/max(tot('input_rows'),1):.2%}), "
          f"duplicates {tot('in_batch_duplicates')+tot('cross_batch_duplicates')}")
    print("reject reasons:", {k[4:]: tot(k) for k in M[0] if k.startswith("rej_") and tot(k)})
    print(f"latency p95 (last batch): {M[-1]['latency_p95_s']} s; last batch runtime {M[-1]['runtime_s']} s")
for name in ("_READY.json", "_LAST_FAILED.json"):
    p = P(f"gold/{name}")
    if os.path.exists(p):
        g = json.load(open(p)); print(f"gold {g['status']} at {g['finished_at']}: tables {g['tables']}")
        for c in g["checks"]:
            print(f"   {'PASS' if c['passed'] else 'FAIL'} {c['check']}: {c['detail']}")
