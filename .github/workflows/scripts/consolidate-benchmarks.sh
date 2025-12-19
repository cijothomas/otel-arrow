#!/bin/bash
# Consolidate multiple benchmark JSON files into a single file
# Usage: consolidate-benchmarks.sh <input_dir> <output_file>

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <input_dir> <output_file>"
    exit 1
fi

INPUT_DIR="$1"
OUTPUT_FILE="$2"

# Find all JSON files in the input directory and consolidate them
find "$INPUT_DIR" -name '*.json' -exec cat {} \; | jq -s 'add' > "$OUTPUT_FILE"

echo "Consolidated benchmarks from $INPUT_DIR to $OUTPUT_FILE"
