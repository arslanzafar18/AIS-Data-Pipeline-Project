#!/usr/bin/env bash
# Runs on the SAME always-on host as the collector. Silver every 120 s, Gold hourly (gated).
# GitHub Actions cannot do this: its minimum schedule interval is 5 min and runs may be delayed.
cd "$(dirname "$0")/.."
export APP_ENV=${APP_ENV:-prod} PYTHONPATH=.
mkdir -p logs
last_gold=0
while true; do
  start=$(date +%s)
  python -m spark.silver_job >> "logs/silver_$APP_ENV.log" 2>&1 || echo "silver failed $(date -u)" >> "logs/silver_$APP_ENV.log"
  if (( start - last_gold >= 3600 )); then
    python -m spark.gold_job >> "logs/gold_$APP_ENV.log" 2>&1 || echo "gold gate/job failed $(date -u)" >> "logs/gold_$APP_ENV.log"
    last_gold=$start
  fi
  elapsed=$(( $(date +%s) - start )); (( elapsed < 120 )) && sleep $(( 120 - elapsed ))
done
