#!/usr/bin/env bash
# run-frontend-local.sh — build and run Cerebro frontend locally
# Usage: ./run-frontend-local.sh [kvs-coordinator-host:port]
#   default: starts local KVS (coordinator + 5 workers) + frontend
#   with arg: skips local KVS, points frontend at the given coordinator (e.g. ec2-xx.compute.amazonaws.com:8000)

set -e
cd "$(dirname "$0")"

KVS_COORD_PORT=8000
FRONTEND_PORT=8080
NUM_WORKERS=5
LOG_DIR=logs
JAR=frontend-local.jar
OUT=out-frontend

KVS_COORD="${1:-localhost:$KVS_COORD_PORT}"
USE_LOCAL_KVS=true
if [ -n "$1" ]; then
  USE_LOCAL_KVS=false
fi

# ── cleanup on exit ──────────────────────────────────────────────────────────
PIDS=()
cleanup() {
  echo ""
  echo "Stopping all processes..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null
  echo "Done."
}
trap cleanup EXIT INT TERM

# ── compile & package ────────────────────────────────────────────────────────
echo "Building frontend..."
rm -rf "$OUT" && mkdir -p "$OUT"
javac -cp "lib/webserver.jar:lib/kvs.jar:lib/flame.jar" \
      --source-path src \
      src/cis5550/frontend/Frontend.java \
      src/cis5550/frontend/SeedIndex.java \
      -d "$OUT" 2>&1
jar cf "$JAR" -C "$OUT" . 2>&1
echo "Build OK → $JAR"

# ── logs dir ─────────────────────────────────────────────────────────────────
mkdir -p "$LOG_DIR"

# ── local KVS ────────────────────────────────────────────────────────────────
if [ "$USE_LOCAL_KVS" = true ]; then
  echo ""
  echo "Starting KVS coordinator on port $KVS_COORD_PORT..."
  java -cp "lib/kvs.jar:lib/webserver.jar" \
       cis5550.kvs.Coordinator "$KVS_COORD_PORT" \
       > "$LOG_DIR/kvs-coord.log" 2>&1 &
  PIDS+=($!)
  sleep 1

  for i in $(seq 1 $NUM_WORKERS); do
    PORT=$((KVS_COORD_PORT + i))
    DIR="worker$i"
    mkdir -p "$DIR"
    echo "  Starting KVS worker $i on port $PORT (data: $DIR/)..."
    java -cp "lib/kvs.jar:lib/webserver.jar" \
         cis5550.kvs.Worker "$PORT" "$DIR" "localhost:$KVS_COORD_PORT" \
         > "$LOG_DIR/kvs-worker$i.log" 2>&1 &
    PIDS+=($!)
    sleep 0.3
  done

  echo "KVS ready. Coordinator: localhost:$KVS_COORD_PORT"
  echo "  KVS UI: http://localhost:$KVS_COORD_PORT"
  sleep 1
else
  echo ""
  echo "Using remote KVS coordinator: $KVS_COORD"
fi

# ── frontend ─────────────────────────────────────────────────────────────────
echo ""
echo "Starting Cerebro frontend on port $FRONTEND_PORT..."
java -cp "$JAR:lib/webserver.jar:lib/kvs.jar:lib/flame.jar" \
     cis5550.frontend.Frontend "$FRONTEND_PORT" "$KVS_COORD" \
     > "$LOG_DIR/frontend.log" 2>&1 &
PIDS+=($!)
sleep 1

echo ""
echo "──────────────────────────────────────────────"
echo "  Cerebro is running!"
echo "  Search UI:  http://localhost:$FRONTEND_PORT"
echo "  Debug mode: http://localhost:$FRONTEND_PORT/debug?q=test"
if [ "$USE_LOCAL_KVS" = true ]; then
echo "  KVS UI:     http://localhost:$KVS_COORD_PORT"
fi
echo "  Logs in:    $LOG_DIR/"
echo ""
echo "  Press Ctrl+C to stop everything."
echo "──────────────────────────────────────────────"


# open browser if on Maci am
if command -v open &>/dev/null; then
  sleep 1 && open "http://localhost:$FRONTEND_PORT" &
fi

# keep alive until Ctrl+C
wait "${PIDS[0]}" 2>/dev/null || true