# Live Offshore Oil & Gas Vessel Activity Pipeline
Medallion pipeline (Bronze → Silver → Gold) on Norwegian open AIS data. PySpark DataFrame / Spark SQL, no pandas.
Project definition: `docs/PROJECT_DEFINITION.md` · design decisions and limits: `docs/DESIGN_AND_LIMITS.md` · run evidence: `docs/EVIDENCE.md`

## Try it in 2 minutes (no credentials, synthetic data)
```bash
python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt   # needs Java 17+
scripts/run_demo.sh            # generate Bronze -> Silver -> Gold -> verify against injected defects
scripts/demo_gate_failure.sh   # shows Gold refusing to publish bad data
PYTHONPATH=. pytest -q         # unit tests
```
## Run it for real (always-on host)
1. Register at barentswatch.no → My page → create API client (scope `ais`); `cp .env.example .env` and fill it in (`APP_ENV=prod`).
2. `python reference/load_sodir.py` (installations → `data/<env>/bronze/reference/facilities.csv`; the loader writes both facilities.csv and fields.csv; check printed column names against `facility_name_col`/`facility_kind_col` in `config/*.json`).
3. Start services: `deploy-ais-collector.service` (collector) and `deploy-ais-pipeline-loop.service` (Silver every 120 s, Gold hourly), installed under systemd as `ais-collector` and `ais-pipeline-loop`.
4. After the first files: `python scripts/infer_schema.py` and confirm `PAYLOAD_SCHEMA` in `spark/silver_logic.py`.
5. After 24 h: `python scripts/hourly_report.py 5` (files, messages, bytes per hour, message rate, days to 5 GB). Backfill: order a box at hais.kystverket.no, check it with `scripts/probe_historic.py`.
6. Operate: `python scripts/status_report.py`.

## Layout
| Path | Purpose |
|---|---|
| `collector/` | Bronze collector (stream → 120 s gzip JSONL files, metrics, heartbeat) |
| `spark/silver_logic.py`, `silver_job.py` | Silver rules (testable functions) and incremental job |
| `spark/gold_logic.py`, `gold_job.py` | Gold tables, quality gate, publish + `_READY.json` |
| `config/dev.json`, `config/prod.json` | Environment settings (`APP_ENV`) |
| `.github/workflows/` | `ci.yml` (tests, e2e, deploy), `monitor.yml` (heartbeat + Gold readiness) |
| `tests/`, `scripts/` | Unit tests; demo, benchmark, status and report tools |
Deploy and monitor run on the self-hosted runner on the always-on host, so no repository secrets are needed; the monitor reads the heartbeat and Gold manifest as local files.
Data: Barentswatch/Kystverket open AIS and Sodir FactPages under NLOD — keep source attribution.
