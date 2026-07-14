#!/usr/bin/env bash
# stop.sh — gracefully stop a FileTransfer node (SIGTERM).
#
#   bin/stop.sh master
#   bin/stop.sh worker
#   bin/stop.sh worker w2
#   bin/stop.sh all           # stop every node with a pid file
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"   # Home dir: bin certs config lib logs tmp
export FT_HOME="$ROOT_DIR"
LOG_DIR="$ROOT_DIR/logs"

stop_one() {
    local name="$1" pidfile="$LOG_DIR/$1.pid"
    if [ ! -f "$pidfile" ]; then
        echo "$name: not running (no pid file)"
        return 0
    fi
    local pid; pid="$(cat "$pidfile")"
    if ! kill -0 "$pid" 2>/dev/null; then
        echo "$name: not running (stale pid $pid)"; rm -f "$pidfile"; return 0
    fi
    echo -n "Stopping $name (PID $pid) "
    kill -TERM "$pid"
    for _ in $(seq 1 15); do
        if ! kill -0 "$pid" 2>/dev/null; then rm -f "$pidfile"; echo "— stopped."; return 0; fi
        printf "."; sleep 1
    done
    echo " — forcing (SIGKILL)"; kill -9 "$pid" 2>/dev/null || true; rm -f "$pidfile"
}

TARGET="${1:-master}"
if [ "$TARGET" = "all" ]; then
    shopt -s nullglob
    for f in "$LOG_DIR"/*.pid; do stop_one "$(basename "$f" .pid)"; done
else
    stop_one "${2:-$TARGET}"
fi
