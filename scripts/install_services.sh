#!/usr/bin/env bash
# Installs and starts every systemd unit of the pipeline on THIS machine (run from the repo folder; safe to rerun).
# The deploy-*.service / deploy-*.timer files are templates; their @PLACEHOLDERS@ are filled from these settings,
# so nothing is tied to one user or folder:
#   REPO        this repository (default: the folder this script is in, one level up)
#   SVC_USER    the user the services run as (default: you)
#   VENV        Python environment with requirements.txt installed (default: ~/spark-env)
#   JAVA_HOME   Java 17+ for Spark (default: found from the `java` on PATH)
#   JAEGER_DIR  where scripts/install_jaeger.sh put Jaeger (default: ~/jaeger)
# Usage:  scripts/install_services.sh                 render, install, enable and (re)start
#         scripts/install_services.sh --render DIR    only write the rendered units to DIR (no sudo; used by tests)
set -euo pipefail
cd "$(dirname "$0")/.."
REPO=${REPO:-$(pwd)}
SVC_USER=${SVC_USER:-$(id -un)}
VENV=${VENV:-$HOME/spark-env}
JAEGER_DIR=${JAEGER_DIR:-$HOME/jaeger}
if [ -z "${JAVA_HOME:-}" ] && command -v java >/dev/null 2>&1; then
  JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")
fi
JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}
SERVICES="ais-collector ais-pipeline-loop ais-dashboard jaeger"
JOBS="sodir-refresh fishing-refresh gfw-refresh bronze-retention silver-v2"      # oneshot services started by timers

render_all() {   # $1 = output folder
  mkdir -p "$1"
  for f in deploy-*.service deploy-*.timer; do
    sed -e "s|@REPO@|$REPO|g" -e "s|@USER@|$SVC_USER|g" -e "s|@VENV@|$VENV|g" \
        -e "s|@JAVA_HOME@|$JAVA_HOME|g" -e "s|@JAEGER_DIR@|$JAEGER_DIR|g" "$f" > "$1/${f#deploy-}"
  done
}

if [ "${1:-}" = "--render" ]; then
  render_all "${2:?usage: --render DIR}"
  exit 0
fi

[ -x "$VENV/bin/python" ] || { echo "No Python at $VENV/bin/python. Create it: python3 -m venv $VENV && $VENV/bin/pip install -r requirements.txt (or set VENV)." >&2; exit 1; }
[ -f .env ] || { echo "No .env here: cp .env.example .env and fill it in." >&2; exit 1; }
[ -f "$JAEGER_DIR/jaeger.env" ] || { echo "Jaeger is not installed in $JAEGER_DIR: run scripts/install_jaeger.sh first (or set JAEGER_DIR)." >&2; exit 1; }
echo "Installing for user $SVC_USER: repo $REPO, python $VENV/bin/python, java $JAVA_HOME, jaeger $JAEGER_DIR"
tmp=$(mktemp -d)
render_all "$tmp"
sudo cp "$tmp"/*.service "$tmp"/*.timer /etc/systemd/system/
rm -rf "$tmp"
sudo systemctl daemon-reload
pkill -f 'http.server 8050' 2>/dev/null || true             # a hand-started dashboard server holds the port
tmux kill-session -t jaeger 2>/dev/null || true             # so does a hand-started Jaeger
timers=$(for j in $JOBS; do printf '%s.timer ' "$j"; done)
sudo systemctl enable $SERVICES $timers
sudo systemctl restart $SERVICES
sudo systemctl start $timers
sleep 3
systemctl is-active $SERVICES
systemctl list-timers $timers --no-pager | head -6
