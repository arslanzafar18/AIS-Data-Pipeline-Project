#!/usr/bin/env bash
# End-to-end run on SYNTHETIC data: Bronze -> Silver -> Gold -> verification. Used locally and in CI.
set -euo pipefail
cd "$(dirname "$0")/.."
export APP_ENV=${APP_ENV:-dev} PYTHONPATH=.
rm -rf "data/$APP_ENV" "metrics/$APP_ENV"
python scripts/make_demo_data.py
python -m spark.silver_job
python -m spark.gold_job
python scripts/status_report.py
python scripts/check_demo.py
