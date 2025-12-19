#!/usr/bin/env python3
"""
Compute scaling efficiency metrics from benchmark data.

Reads benchmark JSON data and adds computed metrics:
- scaling_efficiency: How close to ideal linear scaling (100% = perfect)
- per_core_throughput: Throughput divided by number of cores
- speedup: Actual speedup compared to 1-core baseline

Usage:
    python compute-scaling-metrics.py <input-file> <output-file>
    
Example:
    python compute-scaling-metrics.py output-saturation.json output-saturation-with-metrics.json
"""

import json
import sys


def compute_scaling_metrics(input_file, output_file):
    """Compute and add scaling metrics to benchmark data."""
    
    with open(input_file) as f:
        data = json.load(f)
    
    # Group by cores and protocol - collect throughput and CPU
    throughput = {}
    cpu_norm = {}
    for entry in data:
        extra = entry['extra']
        parts = extra.split(' - ')
        if len(parts) < 3:
            continue
        
        cores = parts[2].split('/')[0].split()[0]
        protocol = extra.split('/')[1].split(' - ')[0] if '/' in extra else 'unknown'
        key = f"{cores}core-{protocol}"
        
        if entry['name'] == 'logs_received_rate':
            throughput[key] = entry['value']
        elif entry['name'] == 'cpu_percentage_normalized_avg':
            cpu_norm[key] = entry['value']
    
    # Calculate scaling metrics
    scaling_metrics = []
    for protocol in ['OTLP-ATTR-OTLP', 'OTAP-ATTR-OTLP']:
        baseline = throughput.get(f"1core-{protocol}", 1)
        
        for cores in ['1', '2', '4', '8']:
            key = f"{cores}core-{protocol}"
            if key in throughput:
                cores_int = int(cores)
                actual_speedup = throughput[key] / baseline if baseline > 0 else 0
                ideal_speedup = cores_int
                efficiency = (actual_speedup / ideal_speedup) * 100 if ideal_speedup > 0 else 0
                per_core = throughput[key] / cores_int
                
                # Add efficiency metric (higher is better)
                scaling_metrics.append({
                    "name": "scaling_efficiency",
                    "unit": "%",
                    "value": efficiency,
                    "extra": f"Continuous - Saturation - {cores} Core(s)/{protocol} - Scaling Efficiency"
                })
                
                # Add per-core throughput (should remain constant for linear scaling)
                scaling_metrics.append({
                    "name": "per_core_throughput",
                    "unit": "logs/sec/core",
                    "value": per_core,
                    "extra": f"Continuous - Saturation - {cores} Core(s)/{protocol} - Per-Core Throughput"
                })
                
                # Add speedup metric
                scaling_metrics.append({
                    "name": "speedup",
                    "unit": "x",
                    "value": actual_speedup,
                    "extra": f"Continuous - Saturation - {cores} Core(s)/{protocol} - Speedup vs 1-core"
                })
    
    # Merge with original data
    data.extend(scaling_metrics)
    
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Added {len(scaling_metrics)} scaling metrics")
    print(f"Output written to: {output_file}")


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    compute_scaling_metrics(input_file, output_file)
