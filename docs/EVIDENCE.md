# Run evidence
All numbers below come from runs in this repository's environment (PySpark 4.2, local mode, 1 core). The end-to-end run and benchmarks use **SYNTHETIC data** (scripts/make_demo_data.py) with known injected defects, because they must be verifiable against ground truth. They prove pipeline logic and relative performance effects, not real AIS volumes. Real-data evidence comes from running the collector (README) and repeating the same steps.

## 1. Unit tests: 16 passed
Config completeness and dev/prod separation; Bronze record contract equals Silver's Bronze schema; every Silver reject reason; sentinel correction; schema drift flag; dedupe; nearest-installation enrichment and zone flags; quality-count reconciliation; Gold sessionisation and Silver-to-Gold reconciliation.

## 2. End-to-end run (`scripts/run_demo.sh`): 11 of 11 checks pass
5,060 Bronze lines (45 files, 5 micro-batches) with injected defects. Pipeline measurements match the ground truth exactly:
| Check | Pipeline | Injected |
|---|---|---|
| parse failures | 11 | 11 |
| bad MMSI rejected | 28 | 28 |
| sentinel positions rejected | 54 | 54 |
| duplicates removed (in-batch + cross-batch) | 110 | 110 |
| Silver rows | 4,857 | 4,857 valid unique |
| SOG sentinels corrected | 33 | 33 |
| rows with unknown fields (drift) | 25 | 25 |
| Gold visits / entered 500 m zone | 10 / 10 | 10 visitor vessels |
Quality before vs after: 5,060 input rows → 93 rejected (1.84 %: 11 parse, 28 MMSI, 54 sentinel position) → 110 duplicates removed → 4,857 Silver rows; ingest latency p95 6 s. Full log: `evidence/demo_run.log`, per-batch metrics: `evidence/silver_metrics_sample.jsonl`.

## 3. Gold reconciliation and publish gate
Gold totals equal Silver totals (2,768 near-installation messages in both visits and hourly tables; 1,334 in-zone). `evidence/gold_READY.json` is the manifest consumers read.
Failure case (`scripts/demo_gate_failure.sh`): a Bronze file that is 90 % malformed pushes the reject rate to 34.7 % (limit 20 %); Gold exits with code 1, is **not** published, the previous READY output stays, and `evidence/gold_LAST_FAILED.json` records the failed check (`evidence/gate_failure.log`).

## 4. Performance (`scripts/benchmark_silver.py`, synthetic, 300 installations)
| Positions | A: haversine on all pairs | B: bounding-box prefilter | Speed-up |
|---|---|---|---|
| 51,570 | 1.41 s | 0.60 s | 2.4x |
| 309,420 | 4.92 s | 0.77 s | 6.4x |
Same output rows in A and B. Full enrichment with 4 shuffle partitions vs Spark's default 200: 1.30 s vs 1.67 s (51k) and 2.06 s vs 2.24 s (309k), a modest 10-22 % gain. Plan (`evidence/enrich_plan.txt`) shows a broadcast nested-loop join for the reference data and a sort-merge join for the nearest-installation lookup. Not tested: skew (the demo has few vessels), multi-node behaviour, real data volumes.
