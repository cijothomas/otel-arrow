#!/bin/bash
# Test scaling metrics computation locally
#
# Usage: ./test-scaling-metrics-locally.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
RESULTS_DIR="$REPO_ROOT/tools/pipeline_perf_test/results"

echo "==> Testing scaling metrics computation"
echo "Repository root: $REPO_ROOT"
echo "Results directory: $RESULTS_DIR"
echo ""

cd "$RESULTS_DIR"

# Step 1: Consolidate benchmark data (matching CI exactly)
echo "Step 1: Consolidating saturation benchmark data..."
bash "$SCRIPT_DIR/consolidate-benchmarks.sh" . output-saturation.json
echo ""

# Step 2: Compute scaling metrics (matching CI exactly)
echo "Step 2: Computing scaling efficiency metrics..."
python3 "$SCRIPT_DIR/compute-scaling-metrics.py" \
  output-saturation.json \
  output-saturation.json
echo ""

# Step 3: Generate summary (matching CI exactly)
echo "Step 3: Generating scaling analysis summary..."
python3 "$SCRIPT_DIR/generate-scaling-summary.py" output-saturation.json
echo ""

echo "==> Done! Check the output above for scaling analysis."
