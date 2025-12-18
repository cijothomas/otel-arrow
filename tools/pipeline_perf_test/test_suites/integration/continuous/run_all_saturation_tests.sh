#!/bin/bash
#
# Run all core scaling saturation tests and collect benchmark results
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORCHESTRATOR_DIR="$SCRIPT_DIR/../../.."
RESULTS_DIR="$ORCHESTRATOR_DIR/results"

cd "$ORCHESTRATOR_DIR"

echo "=== Running Core Scaling Saturation Tests ==="
echo

# Run each test
for cores in 1 2 4 8; do
    if [ "$cores" -eq 1 ]; then
        test_file="saturation-1core.yaml"
        echo "=== Running saturation test with 1 core ==="
    else
        test_file="saturation-${cores}cores.yaml"
        echo "=== Running saturation test with $cores cores ==="
    fi
    
    python orchestrator/run_orchestrator.py \
        --config "test_suites/integration/continuous/$test_file" \
        "$@"
    
    echo
done

echo "=== All tests completed ==="
echo

# Collect and combine benchmark results
OUTPUT_FILE="$RESULTS_DIR/combined-saturation-benchmarks.json"

echo "=== Collecting benchmark results ==="
echo "Looking for results in: $RESULTS_DIR/continuous_saturation_*core/gh-actions-benchmark/"

# Find all benchmark JSON files and combine them
find "$RESULTS_DIR"/continuous_saturation_*core/gh-actions-benchmark/ \
    -name "*.json" -type f 2>/dev/null | while read file; do
    echo "  Found: $file"
done

# Combine all benchmark JSON files using jq
if command -v jq &> /dev/null; then
    find "$RESULTS_DIR"/continuous_saturation_*core/gh-actions-benchmark/ \
        -name "*.json" -type f 2>/dev/null \
        -exec cat {} \; | jq -s 'map(.[])' > "$OUTPUT_FILE"
    
    echo
    echo "✓ Combined benchmark results written to: $OUTPUT_FILE"
    echo
    echo "Summary:"
    jq 'length' "$OUTPUT_FILE" | xargs echo "  Total benchmark entries:"
else
    echo
    echo "⚠ jq not found - cannot combine JSON files"
    echo "  Install jq to automatically combine benchmark results"
    echo
    echo "Benchmark files are available in:"
    echo "  $RESULTS_DIR/continuous_saturation_*core/gh-actions-benchmark/"
fi
