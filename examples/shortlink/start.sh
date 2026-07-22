#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

ETCD="${ETCD_ENDPOINTS:-127.0.0.1:2379}"
DB_PATH="${DB_PATH:-./shortlink.db}"

echo "=========================================="
echo " ReachCache Shortlink Demo"
echo " etcd=$ETCD  db=$DB_PATH"
echo "=========================================="

cleanup() {
    echo ""
    echo "shutting down..."
    kill $PID_A $PID_B $PID_C 2>/dev/null
    wait 2>/dev/null
    echo "done"
}
trap cleanup EXIT INT TERM

go run . \
    -name node-a -addr :50051 -http :8081 \
    -advertise 127.0.0.1:50051 \
    -etcd "$ETCD" -db "$DB_PATH" &
PID_A=$!

sleep 1

go run . \
    -name node-b -addr :50052 -http :8082 \
    -advertise 127.0.0.1:50052 \
    -etcd "$ETCD" -db "$DB_PATH" &
PID_B=$!

sleep 1

go run . \
    -name node-c -addr :50053 -http :8083 \
    -advertise 127.0.0.1:50053 \
    -etcd "$ETCD" -db "$DB_PATH" &
PID_C=$!

echo ""
echo "All nodes started. Demo commands:"
echo ""
echo "  # Create a short link via Node A"
echo "  curl -X POST 'http://localhost:8081/shorten?url=https://github.com/vernmorn/reachcache'"
echo ""
echo "  # Access it via Node B (consistency hash routes to owner)"
echo "  curl -v http://localhost:8082/<code>"
echo ""
echo "  # Check cache stats on Node C"
echo "  curl http://localhost:8083/stats"
echo ""

wait
