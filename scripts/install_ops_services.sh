#!/usr/bin/env bash
# Installs the always-on helpers as systemd services (run once on the production host, from the repo folder):
#   ais-dashboard    the Gold dashboard web server (localhost:8050)
#   jaeger           the trace store and UI (localhost:4318 / 16686), using the command you run Jaeger with now
#   bronze-retention daily timer (03:30 UTC) deleting processed live Bronze files older than bronze_retention_days
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f ~/jaeger/jaeger.env ]; then
  # the Jaeger program itself, not a tmux/bash/tee wrapper around it
  pid=$(pgrep -f -n '^[^ ]*/jaeger[^ /]* .*(--config|config=)' || true)
  if [ -z "$pid" ]; then
    echo "Jaeger is not running, so its start command cannot be copied. Start it the usual way, then rerun." >&2
    exit 1
  fi
  cmd=$(tr '\0' ' ' < /proc/$pid/cmdline)
  echo "JAEGER_CMD=$cmd" > ~/jaeger/jaeger.env
  echo "Jaeger command: $cmd"
fi

sudo cp deploy-ais-dashboard.service /etc/systemd/system/ais-dashboard.service
sudo cp deploy-jaeger.service /etc/systemd/system/jaeger.service
sudo cp deploy-bronze-retention.service /etc/systemd/system/bronze-retention.service
sudo cp deploy-bronze-retention.timer /etc/systemd/system/bronze-retention.timer
sudo systemctl daemon-reload

# The hand-started copies hold the ports; stop them just before the services take over.
pkill -f 'http.server 8050' || true
tmux kill-session -t jaeger 2>/dev/null || true
pkill -f '^[^ ]*/jaeger[^ /]* .*(--config|config=)' || true
sleep 2
sudo systemctl enable --now ais-dashboard jaeger bronze-retention.timer
sleep 3
systemctl is-active ais-dashboard jaeger
systemctl list-timers bronze-retention.timer --no-pager | head -2
