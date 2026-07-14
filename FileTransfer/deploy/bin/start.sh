#!/usr/bin/env bash
# start.sh — start a FileTransfer node (master or worker) in the background.
#
#   bin/start.sh master
#   bin/start.sh worker            # start a worker (repeat for more)
#   bin/start.sh worker w2         # named instance -> its own pid/log
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"   # Home dir: bin certs config lib logs tmp
export FT_HOME="$ROOT_DIR"

ROLE="${1:-master}"
INSTANCE="${2:-$ROLE}"      # allows multiple workers: worker w1, worker w2, ...
case "$ROLE" in
    master|worker) ;;
    *) echo "usage: start.sh {master|worker} [instance-name]"; exit 2 ;;
esac

BINARY="$ROOT_DIR/lib/filetransfer"
CONFIG_FILE="${CONFIG_FILE:-$ROOT_DIR/config/config.yml}"
PID_FILE="$ROOT_DIR/logs/$INSTANCE.pid"
OUT_FILE="$ROOT_DIR/logs/$INSTANCE.out"

# Give a worker a STABLE node id derived from the deployment + instance name (e.g.
# app1-w1), so it keeps the same id across restarts instead of a fresh random one.
if [ "$ROLE" = "worker" ] && [ -z "${FT_NODE_ID:-}" ]; then
    export FT_NODE_ID="$(basename "$ROOT_DIR")-$INSTANCE"
fi

if [ ! -f "$BINARY" ]; then
    echo "ERROR: binary not found at $BINARY — run 'make build' first."
    exit 1
fi

if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "$INSTANCE is already running (PID $(cat "$PID_FILE"))"
    exit 1
fi
rm -f "$PID_FILE"
mkdir -p "$ROOT_DIR/logs"

# Run from the project root so relative paths in config.yml resolve correctly.
cd "$ROOT_DIR"
nohup "$BINARY" "$ROLE" --config "$CONFIG_FILE" >> "$OUT_FILE" 2>&1 &
PID=$!
echo "$PID" > "$PID_FILE"

echo "filetransfer $INSTANCE started (PID $PID)"
echo "  Role   : $ROLE"
echo "  Config : $CONFIG_FILE"
echo "  Out    : $OUT_FILE"
echo "  PID    : $PID_FILE"

sleep 1
if ! kill -0 "$PID" 2>/dev/null; then
    echo "ERROR: process exited immediately. Check $OUT_FILE"
    rm -f "$PID_FILE"
    exit 1
fi
echo "$INSTANCE is up."
