"""Verifies the pipeline output against the injected ground truth. Exit 1 on any mismatch."""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))   # run from anywhere
import json, sys
import pipeline_config
from spark.session import get_spark

cfg = pipeline_config.load()
P = lambda r: pipeline_config.path(cfg, r)
exp = json.load(open(P("demo_expected.json")))
M = [json.loads(l) for l in open(P("quality/silver_metrics.jsonl"))]
S = lambda k: sum(m[k] for m in M)
spark = get_spark(cfg, "check")
silver_rows = spark.read.parquet(P("silver/positions")).count()
visits = spark.read.parquet(P("gold/vessel_visits"))
results = [
    ("input rows == generated lines", S("input_rows"), exp["total_lines"]),
    ("parse_failed == malformed", S("parse_failed"), exp["malformed"]),
    ("rejected bad_mmsi", S("rej_bad_mmsi"), exp["bad_mmsi"]),
    ("rejected sentinel_position", S("rej_sentinel_position"), exp["sentinel"]),
    ("duplicates removed (in-batch + cross-batch)", S("in_batch_duplicates") + S("cross_batch_duplicates"), exp["duplicates"]),
    ("Silver rows == valid unique", silver_rows, exp["valid_unique"]),
    ("sog corrections", S("corrected_sog"), exp["corrected_sog"]),
    ("rows with unknown fields (schema drift)", S("rows_with_unknown_fields"), exp["unknown_field_rows"]),
    ("Gold visits == visitor vessels", visits.count(), exp["visitors"]),
    ("all visits entered 500 m zone", visits.where("entered_zone = 1").count(), exp["visitors"]),
    ("Gold READY manifest", json.load(open(P("gold/_READY.json")))["status"], "READY"),
]
bad = 0
for name, got, want in results:
    ok = got == want; bad += not ok
    print(f"{'PASS' if ok else 'FAIL'}  {name}: got {got}, expected {want}")
sys.exit(1 if bad else 0)
