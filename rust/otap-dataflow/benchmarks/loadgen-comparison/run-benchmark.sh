#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────
# Loadgen Comparison Benchmark
# ──────────────────────────────────────────────────────────────────────
# Compares Python loadgen vs Rust engine-as-loadgen sending OTLP traffic
# to a df_engine target, measuring:
#   - Load generator CPU & memory (RSS)
#   - Target engine throughput via its telemetry endpoint
#
# Usage:
#   cd rust/otap-dataflow
#   bash benchmarks/loadgen-comparison/run-benchmark.sh [DURATION_SECS]
#
# Prerequisites:
#   - Release build:  cargo build --release
#   - Python deps:    pip install -r ../../tools/pipeline_perf_test/load_generator/requirements.txt
# ──────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOADGEN_DIR="$ENGINE_ROOT/../../tools/pipeline_perf_test/load_generator"
ENGINE_BIN="$ENGINE_ROOT/target/release/df_engine"
TARGET_CFG="$SCRIPT_DIR/target-engine.yaml"
RUST_LG_CFG="$SCRIPT_DIR/rust-loadgen.yaml"
METRICS_URL="http://127.0.0.1:8080/telemetry/metrics?format=json&reset=false"
DURATION="${1:-30}"
SAMPLE_INTERVAL=2  # seconds between CPU/mem samples
RESULTS_DIR="$SCRIPT_DIR/results"

# ── colours ──────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()  { echo -e "${CYAN}▶${NC} $*"; }
ok()    { echo -e "${GREEN}✔${NC} $*"; }
fail()  { echo -e "${RED}✘${NC} $*"; exit 1; }
header(){ echo -e "\n${BOLD}═══ $* ═══${NC}\n"; }

# ── pre-flight checks ───────────────────────────────────────────────
[[ -x "$ENGINE_BIN" ]] || fail "Engine binary not found at $ENGINE_BIN — run 'cargo build --release' first."
command -v python >/dev/null 2>&1 || fail "python not found."
[[ -f "$LOADGEN_DIR/loadgen.py" ]] || fail "Python loadgen not found at $LOADGEN_DIR/loadgen.py"

mkdir -p "$RESULTS_DIR"

# ── helpers ──────────────────────────────────────────────────────────

wait_for_port() {
    local port=$1 timeout=${2:-15}
    local end=$((SECONDS + timeout))
    while ! ss -tlnp 2>/dev/null | grep -q ":${port} " && (( SECONDS < end )); do
        sleep 0.3
    done
    ss -tlnp 2>/dev/null | grep -q ":${port} " || fail "Port $port not ready after ${timeout}s"
}

wait_for_http() {
    local url=$1 timeout=${2:-15}
    local end=$((SECONDS + timeout))
    while ! curl -sf "$url" >/dev/null 2>&1 && (( SECONDS < end )); do
        sleep 0.5
    done
    curl -sf "$url" >/dev/null 2>&1 || fail "HTTP endpoint $url not ready after ${timeout}s"
}

# Sample CPU% and RSS of a PID every SAMPLE_INTERVAL seconds.
# Writes CSV: timestamp,cpu%,rss_kb
sample_process() {
    local pid=$1 outfile=$2
    echo "timestamp,cpu_pct,rss_kb" > "$outfile"
    while kill -0 "$pid" 2>/dev/null; do
        # /proc/PID/stat fields: 14=utime 15=stime (ticks)
        # ps gives a snapshot cpu% and rss
        local line
        line=$(ps -p "$pid" -o pcpu=,rss= 2>/dev/null) || break
        local cpu rss
        cpu=$(echo "$line" | awk '{print $1}')
        rss=$(echo "$line" | awk '{print $2}')
        echo "$(date +%s),$cpu,$rss" >> "$outfile"
        sleep "$SAMPLE_INTERVAL"
    done
}

# Fetch engine metrics JSON snapshot
snapshot_engine_metrics() {
    curl -sf "$METRICS_URL" 2>/dev/null || echo "{}"
}

# Summarise a CSV of cpu_pct,rss_kb samples
summarise_samples() {
    local file=$1 label=$2
    if [[ ! -s "$file" ]] || [[ $(wc -l < "$file") -le 1 ]]; then
        echo "  (no samples collected)"
        return
    fi
    awk -F, 'NR>1 {
        n++; cpu+=$2; rss+=$3
        if(NR==2 || $2>max_cpu) max_cpu=$2
        if(NR==2 || $3>max_rss) max_rss=$3
    } END {
        if(n>0) {
            printf "  %-18s  avg CPU: %5.1f%%   max CPU: %5.1f%%   avg RSS: %6.1f MB   max RSS: %6.1f MB   samples: %d\n",
                "'"$label"':", cpu/n, max_cpu, rss/n/1024, max_rss/1024, n
        }
    }' "$file"
}

# Kill a process and wait for it to exit
stop_proc() {
    local pid=$1 name=$2
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        ok "Stopped $name (PID $pid)"
    fi
}

# ── cleanup on exit ─────────────────────────────────────────────────
TARGET_PID="" LOADGEN_PID="" SAMPLER_PID=""
cleanup() {
    [[ -n "$SAMPLER_PID" ]] && kill "$SAMPLER_PID" 2>/dev/null; wait "$SAMPLER_PID" 2>/dev/null || true
    [[ -n "$LOADGEN_PID" ]] && stop_proc "$LOADGEN_PID" "loadgen"
    [[ -n "$TARGET_PID" ]]  && stop_proc "$TARGET_PID" "target-engine"
}
trap cleanup EXIT

# ══════════════════════════════════════════════════════════════════════
#  PHASE 1 — Python load generator
# ══════════════════════════════════════════════════════════════════════
header "Phase 1: Python loadgen → target engine (${DURATION}s)"

# Start target engine
info "Starting target engine..."
"$ENGINE_BIN" --config "$TARGET_CFG" --http-admin-bind "0.0.0.0:8080" &
TARGET_PID=$!
wait_for_http "$METRICS_URL" 20
ok "Target engine ready (PID $TARGET_PID)"

# Fetch baseline engine metrics
BASELINE_PYTHON=$(snapshot_engine_metrics)

# Start sampling target engine resources
sample_process "$TARGET_PID" "$RESULTS_DIR/target-during-python.csv" &
TARGET_SAMPLER_PID=$!

# Start Python loadgen
info "Starting Python loadgen (threads=4, batch_size=1000, target_rate=100000, duration=${DURATION}s)..."
OTLP_ENDPOINT="localhost:4317" python "$LOADGEN_DIR/loadgen.py" \
    --load-type otlp \
    --threads 4 \
    --batch-size 1000 \
    --target-rate 100000 \
    --duration "$DURATION" &
LOADGEN_PID=$!

# Sample Python loadgen resources
sample_process "$LOADGEN_PID" "$RESULTS_DIR/python-loadgen.csv" &
SAMPLER_PID=$!

# Wait for loadgen to finish
wait "$LOADGEN_PID" 2>/dev/null || true
LOADGEN_PID=""
ok "Python loadgen finished"

# Stop sampler
kill "$SAMPLER_PID" 2>/dev/null; wait "$SAMPLER_PID" 2>/dev/null || true
SAMPLER_PID=""
kill "$TARGET_SAMPLER_PID" 2>/dev/null; wait "$TARGET_SAMPLER_PID" 2>/dev/null || true

# Capture engine metrics after Python run
sleep 2  # let metrics settle
AFTER_PYTHON=$(snapshot_engine_metrics)

# Stop target engine
stop_proc "$TARGET_PID" "target-engine"
TARGET_PID=""
sleep 2  # let port free up

# ══════════════════════════════════════════════════════════════════════
#  PHASE 2 — Rust engine-as-loadgen
# ══════════════════════════════════════════════════════════════════════
header "Phase 2: Rust loadgen → target engine (${DURATION}s)"

# Start target engine
info "Starting target engine..."
"$ENGINE_BIN" --config "$TARGET_CFG" --http-admin-bind "0.0.0.0:8080" &
TARGET_PID=$!
wait_for_http "$METRICS_URL" 20
ok "Target engine ready (PID $TARGET_PID)"

# Fetch baseline engine metrics
BASELINE_RUST=$(snapshot_engine_metrics)

# Start sampling target engine resources
sample_process "$TARGET_PID" "$RESULTS_DIR/target-during-rust.csv" &
TARGET_SAMPLER_PID=$!

# Start Rust loadgen engine
info "Starting Rust loadgen engine (signals_per_second=100000, duration=${DURATION}s)..."
"$ENGINE_BIN" --config "$RUST_LG_CFG" &
LOADGEN_PID=$!
sleep 1  # let it establish connection

# Sample Rust loadgen resources
sample_process "$LOADGEN_PID" "$RESULTS_DIR/rust-loadgen.csv" &
SAMPLER_PID=$!

# Let it run for DURATION
sleep "$DURATION"

# Stop Rust loadgen
stop_proc "$LOADGEN_PID" "rust-loadgen"
LOADGEN_PID=""

# Stop sampler
kill "$SAMPLER_PID" 2>/dev/null; wait "$SAMPLER_PID" 2>/dev/null || true
SAMPLER_PID=""
kill "$TARGET_SAMPLER_PID" 2>/dev/null; wait "$TARGET_SAMPLER_PID" 2>/dev/null || true

# Capture engine metrics after Rust run
sleep 2
AFTER_RUST=$(snapshot_engine_metrics)

# Stop target engine
stop_proc "$TARGET_PID" "target-engine"
TARGET_PID=""

# ══════════════════════════════════════════════════════════════════════
#  RESULTS
# ══════════════════════════════════════════════════════════════════════
header "Results (${DURATION}s test duration, ${SAMPLE_INTERVAL}s sample interval)"

echo -e "${BOLD}Load Generator Resource Usage:${NC}"
summarise_samples "$RESULTS_DIR/python-loadgen.csv" "Python loadgen"
summarise_samples "$RESULTS_DIR/rust-loadgen.csv"   "Rust loadgen"

echo ""
echo -e "${BOLD}Target Engine Resource Usage (while receiving load):${NC}"
summarise_samples "$RESULTS_DIR/target-during-python.csv" "During Python"
summarise_samples "$RESULTS_DIR/target-during-rust.csv"   "During Rust"

echo ""
echo -e "${BOLD}Engine Metrics Snapshots:${NC}"
echo "  Python run (before): $RESULTS_DIR/engine-metrics-before-python.json"
echo "  Python run (after):  $RESULTS_DIR/engine-metrics-after-python.json"
echo "  Rust run (before):   $RESULTS_DIR/engine-metrics-before-rust.json"
echo "  Rust run (after):    $RESULTS_DIR/engine-metrics-after-rust.json"

# Save engine metric snapshots
echo "$BASELINE_PYTHON" > "$RESULTS_DIR/engine-metrics-before-python.json"
echo "$AFTER_PYTHON"    > "$RESULTS_DIR/engine-metrics-after-python.json"
echo "$BASELINE_RUST"   > "$RESULTS_DIR/engine-metrics-before-rust.json"
echo "$AFTER_RUST"      > "$RESULTS_DIR/engine-metrics-after-rust.json"

# Save raw CSV data location
echo ""
echo -e "${BOLD}Raw CSV data:${NC}"
echo "  $RESULTS_DIR/python-loadgen.csv"
echo "  $RESULTS_DIR/rust-loadgen.csv"
echo "  $RESULTS_DIR/target-during-python.csv"
echo "  $RESULTS_DIR/target-during-rust.csv"

ok "Benchmark complete. Results in $RESULTS_DIR/"
