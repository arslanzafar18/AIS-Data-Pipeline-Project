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
Any Linux host with systemd, Java 17+ and Python 3.11; nothing below is tied to a user name or folder.
1. Clone the repository, then create the Python environment: `python3 -m venv ~/spark-env && ~/spark-env/bin/pip install -r requirements.txt`.
2. Register at barentswatch.no → My page → create API client (scope `ais`); `cp .env.example .env` and fill it in (`APP_ENV=prod`; `GFW_TOKEN` optional).
3. Reference data: `APP_ENV=prod PYTHONPATH=. ~/spark-env/bin/python reference/fetch_sodir.py` (then `fetch_pipelines.py`, `fetch_fishing_vessels.py`, `fetch_gfw_sar.py`); the timers refresh them afterwards.
4. Tracing: `scripts/install_jaeger.sh` (Jaeger v2.22.0 with `config/jaeger.yaml`, localhost only).
5. Services: `scripts/install_services.sh` renders every `deploy-*.service` / `deploy-*.timer` template for this machine (user, repo, `VENV`, `JAVA_HOME`, `JAEGER_DIR`; override any as environment variables), installs them under systemd and starts them: `ais-collector`, `ais-pipeline-loop`, `ais-dashboard`, `jaeger` and the four timers.
6. CI deploy and monitor: register a GitHub self-hosted runner on the host; if production is not in `/home/ubuntu/AIS-Data-Pipeline-Project` with `/home/ubuntu/spark-env`, set the repository variables `PROD_DIR` and `PROD_VENV`.
7. After the first files: `python scripts/infer_schema.py` and confirm `PAYLOAD_SCHEMA` in `spark/silver_logic.py`.
8. After 24 h: `python scripts/hourly_report.py 5` (files, messages, bytes per hour, message rate, days to 5 GB). Backfill: order a box at hais.kystverket.no, check it with `scripts/probe_historic.py`.
9. Operate: `python scripts/status_report.py`.

## Operations runbook (production host)
All commands from `~/AIS-Data-Pipeline-Project`. `PY=/home/ubuntu/spark-env/bin/python`. Give prod settings to single commands
(`APP_ENV=prod PYTHONPATH=. $PY ...`); do not `source .env` into a shell you also use for development.

**Services**
| Service | What it does | Logs |
|---|---|---|
| `ais-collector` | Bronze: live stream → one gzip file per 120 s | `metrics/prod/events.jsonl`, `journalctl -u ais-collector` |
| `ais-pipeline-loop` | Silver every 120 s, Gold hourly (gated, incremental) | `logs/silver_prod.log`, `logs/gold_prod.log` |
| `sodir-refresh.timer` | Sodir facilities and pipelines → `bronze/reference` (Mondays 05:00 UTC) | `journalctl -u sodir-refresh` |
| `gfw-refresh.timer` | Global Fishing Watch radar detections not matched to AIS → `bronze/gfw/` (daily 06:15 UTC; needs `GFW_TOKEN` in `.env`; non-commercial use only) | `journalctl -u gfw-refresh` |
| `fishing-refresh.timer` | Norwegian fishing vessel register → `bronze/reference/fishing_vessels.csv` (Mondays 05:30 UTC; owners dropped) | `journalctl -u fishing-refresh` |
| `bronze-retention.timer` | Deletes live Bronze files older than `bronze_retention_days` (30) that Silver has processed (03:30 UTC) | `journalctl -u bronze-retention` |
| `ais-dashboard` | Serves the Gold dashboard on localhost:8050 | `journalctl -u ais-dashboard` |
| `jaeger` | Trace store and UI (localhost:4318 in, localhost:16686 UI) | `journalctl -u jaeger` |

All of them are installed by `scripts/install_services.sh` (after `scripts/install_jaeger.sh`); rerun it after changing a `deploy-*` file.
Preview retention first: `APP_ENV=prod $PY scripts/bronze_retention.py` (dry run: prints how many files and GB would go).

`sudo systemctl status|restart ais-collector ais-pipeline-loop` · `systemctl list-timers sodir-refresh.timer` · health: `APP_ENV=prod $PY scripts/status_report.py`.

**Is it healthy?**
```bash
cat metrics/prod/heartbeat.json                       # ts within the last few minutes
python3 -c "import json;m=json.load(open('data/prod/gold/_READY.json'));print(m['status'],m.get('mode'),m['finished_at'],m.get('warnings'))"
ls data/prod/gold/_LAST_FAILED.json 2>/dev/null       # present = last Gold run was blocked by the gate
```

**When the monitor opens a `pipeline-alert` issue** (checks: heartbeat, Gold READY and age, disk space, message volume) (the workflow comments on it while failing and closes it when checks pass):
1. *Heartbeat stale*: `sudo systemctl status ais-collector`, then `tail metrics/prod/events.jsonl` (look for `reader_error`, e.g. expired
   BarentsWatch credentials or the stream being down). `sudo systemctl restart ais-collector`.
2. *Gold not READY*: read `data/prod/gold/_LAST_FAILED.json` (which gate check failed) and `tail -50 logs/gold_prod.log`.
   The previous Gold stays published, so readers are unaffected; fix the cause, the next hourly run publishes again.
   *Warning "repeated keys"* in `logs/gold_prod.log` or `_READY.json` (`warnings`, `keys`): Gold still published; run
   `APP_ENV=prod PYTHONPATH=. python scripts/check_keys.py` (read-only) to see which table repeats a key.
3. *Gold too old*: `pgrep -af spark.gold_job` (a run still going?) and `free -g` (out of memory → see `docs/DESIGN_AND_LIMITS.md`).
4. *Disk almost full* (under 15% free): `du -sh data/prod/*/* | sort -h | tail`; delete old `silver/positions_old_*` copies,
   or run retention with fewer days (`scripts/bronze_retention.py --days 14 --apply`).
5. *Message volume low* (last full hour under half the usual): the stream is up but thin. Check `reader_error` events and
   the BarentsWatch status; it often recovers by itself, and Gold marks the affected hours as a feed outage.

**Tracing (OpenTelemetry → Jaeger).** Set `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318` in `.env`; unset, tracing is off.
Services in Jaeger: `ais-collector` (`bronze.connect`, `bronze.flush`, `bronze.stream_end`, `bronze.reader_error`), `ais-silver`
(`silver.batch` and its steps), `ais-gold` (`gold.*`, including the gate checks). Jaeger runs as the `jaeger` service from `~/jaeger`, bound to localhost
(it has no login): forward port 16686 in VS Code (PORTS tab) and open http://localhost:16686. Do not open the port publicly.

**Dashboard and vessel profile.** Gold rebuilds `data/prod/gold/dashboard/index.html` after every run (a run the gate blocked rebuilds it from the published Gold). Its **Pipeline health** tab is the visual health check: freshness of each layer, Bronze columns (each Silver batch records Bronze records missing a contract column, new AIS message fields and expected fields no message carried; each page build checks every reference CSV still has the columns the pipeline reads), messages per hour with feed outages, Silver rejects and batch times, and every Gold run from `data/prod/quality/gold_runs.jsonl` (one line per run: published, blocked or crashed). To view it:
the `ais-dashboard` service serves it on localhost:8050; forward port 8050.
One vessel: `APP_ENV=prod PYTHONPATH=. $PY -m spark.vessel_report --name "ESVAGT AURORA" --days 30` (or `--mmsi N`) → `reports/`.

**Maintenance jobs** (stop the loop first, and only when `pgrep -af spark.gold_job` prints nothing; the collector keeps running, so no data is lost):
```bash
sudo systemctl stop ais-pipeline-loop
APP_ENV=prod PYTHONPATH=. $PY -m spark.gold_job --full     # rebuild all of Gold (after a rule or layout change)
APP_ENV=prod PYTHONPATH=. $PY -m spark.reenrich            # apply new row rules to old Silver → silver/positions_reenriched
sudo systemctl start ais-pipeline-loop
```
After `reenrich`: compare row counts of `silver/positions` and `silver/positions_reenriched`; only if equal, rename `positions` to
`positions_old_<date>`, rename `positions_reenriched` to `positions`, then run `gold_job --full`. Keep the old folder until Gold is READY.
Refresh facilities now: `sudo systemctl start sodir-refresh` (the change log shows added, removed and renamed facilities).

**Deploy.** Merging to `main` runs `ci.yml`: static check, unit tests, end-to-end demo, gate demo, then the `deploy` job on the
self-hosted runner pulls and restarts both services. Check it with `gh run list --workflow ci --limit 3`. If deploy did not run,
deploy by hand: `git pull --ff-only && sudo systemctl restart ais-collector ais-pipeline-loop`.
Never merge during a maintenance job: deploy restarts the loop.

## Layout
| Path | Purpose |
|---|---|
| `collector/` | Bronze collector (stream → 120 s gzip JSONL files, metrics, heartbeat, traces) |
| `reference/` | Sodir facility list: `fetch_sodir.py` (weekly API refresh), `load_sodir.py` (first load); `fetch_fishing_vessels.py` (fishing vessel register, weekly) |
| `spark/silver_logic.py`, `silver_job.py` | Silver rules (testable functions) and incremental job |
| `spark/gold_logic.py`, `gold_job.py`, `gold_plan.py` | Gold tables, incremental plan, quality gate, publish + `_READY.json` |
| `spark/dashboard.py`, `vessel_report.py` | Gold dashboard (5 tabs incl. Pipeline health, map, date slider) and single-vessel profile |
| `spark/reenrich.py` | Re-apply Silver row rules to existing Silver without reprocessing Bronze |
| `observability.py` | OpenTelemetry tracing (no-op unless configured) |
| `config/dev.json`, `config/prod.json` | Environment settings (`APP_ENV`) |
| `.github/workflows/` | `ci.yml` (tests, e2e, deploy), `monitor.yml` (heartbeat + Gold readiness) |
| `tests/`, `scripts/` | Unit tests; demo, benchmark, status and report tools |
Deploy and monitor run on the self-hosted runner on the always-on host, so no repository secrets are needed; the monitor reads the heartbeat and Gold manifest as local files.
Data: Barentswatch/Kystverket open AIS and Sodir FactPages under NLOD; Fiskeridirektoratet vessel register (owners dropped); radar detections Powered by Global Fishing Watch (https://globalfishingwatch.org, CC BY-NC 4.0, non-commercial use only). Keep source attribution.
