#!/usr/bin/env bash
set -euo pipefail

response=$(curl -fsS -X POST \
  "http://127.0.0.1:14319/api/v1/groups/default/pipelines/main/shutdown?wait=true&timeout_secs=30")
state=$(jq -r '.state' <<<"$response")
if [[ "$state" != "succeeded" ]]; then
  echo "pipeline shutdown did not succeed: $response" >&2
  exit 1
fi