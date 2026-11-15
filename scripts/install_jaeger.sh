#!/usr/bin/env bash
# Installs the Jaeger version this project runs (v2.22.0, one binary) with config/jaeger.yaml into JAEGER_DIR
# (default ~/jaeger). Every Jaeger port is bound to localhost: the UI has no login. Safe to rerun; an existing
# download is reused. Next step: scripts/install_services.sh (runs it as the systemd service "jaeger").
#   scripts/install_jaeger.sh            JAEGER_VERSION=... and JAEGER_DIR=... override the defaults
set -euo pipefail
cd "$(dirname "$0")/.."
V=${JAEGER_VERSION:-2.22.0}
DIR=${JAEGER_DIR:-$HOME/jaeger}
case "$(uname -m)" in
  x86_64) A=amd64 ;;
  aarch64|arm64) A=arm64 ;;
  *) echo "Unsupported CPU $(uname -m): download Jaeger $V by hand into $DIR" >&2; exit 1 ;;
esac
NAME="jaeger-$V-linux-$A"
mkdir -p "$DIR"
if [ ! -x "$DIR/$NAME/jaeger" ]; then
  if [ ! -f "$DIR/$NAME.tar.gz" ]; then
    curl -fL --retry 3 -o "$DIR/$NAME.tar.gz" \
      "https://github.com/jaegertracing/jaeger/releases/download/v$V/$NAME.tar.gz"
  fi
  tar -xzf "$DIR/$NAME.tar.gz" -C "$DIR"
fi
cp config/jaeger.yaml "$DIR/config.yaml"
echo "JAEGER_CMD=$DIR/$NAME/jaeger --config $DIR/config.yaml" > "$DIR/jaeger.env"
"$DIR/$NAME/jaeger" --version 2>&1 | tail -1
echo "Jaeger $V ready in $DIR. Next: scripts/install_services.sh"
