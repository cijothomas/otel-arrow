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

# Step 1: Consolidate benchmark data
echo "Step 1: Consolidating saturation benchmark data..."
cd "$RESULTS_DIR"

# Find and combine all saturation benchmark JSON files
find . -path '*/continuous_saturation_*/gh-actions-benchmark/*.json' -type f -exec cat {} \; | \
  jq -s 'map(.[])' > combined-saturation-benchmarks.json

ENTRY_COUNT=$(jq '. | length' combined-saturation-benchmarks.json)
echo "✓ Consolidated $ENTRY_COUNT benchmark entries"
echo ""

# Step 2: Compute scaling metrics
echo "Step 2: Computing scaling efficiency metrics..."
python3 "$SCRIPT_DIR/compute-scaling-metrics.py" \
  combined-saturation-benchmarks.json \
  combined-saturation-with-metrics.json

echo ""

# Step 3: Generate summary
echo "Step 3: Generating scaling analysis summary..."
python3 "$SCRIPT_DIR/generate-scaling-summary.py" \
  combined-saturation-with-metrics.json \
  scaling-summary.md

echo ""
echo "==> Summary also saved to: $RESULTS_DIR/scaling-summary.md"
echo ""
cat "$RESULTS_DIR/scaling-summary.md"
echo ""
echo "==> Done! Check the output above for scaling analysis."
