#!/usr/bin/env bash
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJ_DIR="$(dirname "$BENCH_DIR")"
OUT_DIR="${BENCH_DIR}/results"
mkdir -p "$OUT_DIR"

BENCHTIME="${BENCHTIME:-3x}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SUMMARY="${OUT_DIR}/summary_${TIMESTAMP}.txt"

echo "=========================================="
echo " ReachCache Benchmark Suite"
echo "=========================================="
echo "benchtime=$BENCHTIME, results=$OUT_DIR"
echo ""

# ──────────────────────────────────
# 1. Local benchmarks (always run)
# ──────────────────────────────────
echo "--- Running local benchmarks ---"
LOCAL_OUT="${OUT_DIR}/local_${TIMESTAMP}.txt"

cd "$PROJ_DIR"
go test -bench="Benchmark(LRU|LRU2|GroupGet|GroupSet|GroupWithExpiration|SingleFlight|Store)" \
    -benchtime="$BENCHTIME" \
    -benchmem \
    -count=1 \
    ./benchmark/ 2>&1 | tee "$LOCAL_OUT"

echo ""
echo "Local results saved to: $LOCAL_OUT"

# ──────────────────────────────────
# 2. Distributed benchmarks (if CLOUD_ADDR set)
# ──────────────────────────────────
if [ -n "${CLOUD_ADDR:-}" ]; then
    echo ""
    echo "--- Running distributed benchmarks (CLOUD_ADDR=$CLOUD_ADDR) ---"
    REMOTE_OUT="${OUT_DIR}/remote_${TIMESTAMP}.txt"

    cd "$PROJ_DIR"
    CLOUD_ADDR="$CLOUD_ADDR" go test -bench="Benchmark(Remote|Distributed)" \
        -benchtime="$BENCHTIME" \
        -benchmem \
        -count=1 \
        ./benchmark/ 2>&1 | tee "$REMOTE_OUT"

    echo ""
    echo "Remote results saved to: $REMOTE_OUT"
else
    echo ""
    echo "--- Skipping distributed benchmarks (CLOUD_ADDR not set) ---"
fi

# ──────────────────────────────────
# 3. LRU vs LRU-2 focused comparison
# ──────────────────────────────────
echo ""
echo "--- Running LRU vs LRU-2 comparison ---"
LRU_OUT="${OUT_DIR}/lru_vs_lru2_${TIMESTAMP}.txt"

cd "$PROJ_DIR"
go test -bench="BenchmarkStore_LRU_vs_LRU2|BenchmarkStore_LRU_Sequential|BenchmarkStore_LRU2_Sequential|BenchmarkStore_LRU_RandomAccess|BenchmarkStore_LRU2_RandomAccess|BenchmarkStore_LRU_ParallelSet|BenchmarkStore_LRU2_ParallelSet" \
    -benchtime="$BENCHTIME" \
    -benchmem \
    -count=1 \
    ./benchmark/ 2>&1 | tee "$LRU_OUT"

echo ""
echo "LRU vs LRU-2 results saved to: $LRU_OUT"

# ──────────────────────────────────
# 4. Summary
# ──────────────────────────────────
{
    echo "ReachCache Benchmark Summary"
    echo "============================"
    echo "Date: $(date)"
    echo "Go version: $(go version)"
    echo "Platform: $(uname -a)"
    echo "CPU: $(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 'unknown')"
    echo ""
    echo "Local results:"
    grep -E "^(Benchmark|---|ok)" "$LOCAL_OUT" 2>/dev/null || echo "  (no data)"
    if [ -n "${CLOUD_ADDR:-}" ]; then
        echo ""
        echo "Remote results (CLOUD_ADDR=$CLOUD_ADDR):"
        grep -E "^(Benchmark|---|ok)" "$REMOTE_OUT" 2>/dev/null || echo "  (no data)"
    fi
    echo ""
    echo "LRU vs LRU-2 results:"
    grep -E "^(Benchmark|---|ok)" "$LRU_OUT" 2>/dev/null || echo "  (no data)"
} > "$SUMMARY"

echo ""
echo "=========================================="
echo " All done! Summary saved to:"
echo "  $SUMMARY"
echo "=========================================="
