"""
Rolls metrics/events.jsonl up to files / messages / bytes / message rate per hour and
projects days needed to reach a target raw size from live collection alone.
Pure standard library (no pandas).  Run after 24 h:  python scripts/hourly_report.py [target_gb]
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))   # run from anywhere
import json
from collections import defaultdict
import pipeline_config

target_gb = float(sys.argv[1]) if len(sys.argv) > 1 else 5.0
hours = defaultdict(lambda: {"files": 0, "messages": 0, "bytes_raw": 0, "bytes_gz": 0, "seconds": 0.0})
errors = 0
cfg = pipeline_config.load()
with open(f"{cfg['metrics_dir']}/events.jsonl") as f:
    for line in f:
        e = json.loads(line)
        if e["event"] == "reader_error":
            errors += 1
        elif e["event"] == "flush":
            h = hours[e["ts"][:13]]           # 'YYYY-MM-DDTHH'
            h["files"] += 1 if e["file"] else 0
            for k in ("messages", "bytes_raw", "bytes_gz"):
                h[k] += e[k]
            h["seconds"] += e["window_s"]

print(f"{'hour (UTC)':<14}{'files':>6}{'messages':>12}{'msg/s':>8}{'MB gz':>9}{'MB raw':>9}")
for k in sorted(hours):
    h = hours[k]
    print(f"{k:<14}{h['files']:>6}{h['messages']:>12}{h['messages']/max(h['seconds'],1):>8.1f}"
          f"{h['bytes_gz']/1e6:>9.1f}{h['bytes_raw']/1e6:>9.1f}")

n = max(len(hours), 1)
raw_day = sum(h["bytes_raw"] for h in hours.values()) / n * 24 / 1e9
gz_day = sum(h["bytes_gz"] for h in hours.values()) / n * 24 / 1e9
print(f"\nAvg: {raw_day:.2f} GB/day raw JSON, {gz_day:.2f} GB/day gzip; reader errors: {errors}")
if raw_day > 0:
    print(f"Days to {target_gb} GB raw from live only: {target_gb / raw_day:.1f}")
