#!/usr/bin/env python3
"""
Generate scaling analysis summary table from benchmark data.

Reads benchmark JSON with computed metrics and generates a markdown summary table
showing throughput, speedup, efficiency, CPU usage, and dropped logs for each configuration.

Usage:
    python generate-scaling-summary.py <input-file> [output-file]
    
Examples:
    # Print to console
    python generate-scaling-summary.py output-saturation.json
    
    # Write to file
    python generate-scaling-summary.py output-saturation.json summary.md
    
    # Append to GitHub Step Summary (CI mode)
    GITHUB_STEP_SUMMARY=summary.md python generate-scaling-summary.py output-saturation.json
"""

import json
import sys
import os


def generate_summary(input_file, output_file=None):
    """Generate scaling analysis summary table."""
    
    with open(input_file) as f:
        data = json.load(f)
    
    # Group metrics by configuration
    metrics = {}
    for entry in data:
        extra = entry['extra']
        parts = extra.split(' - ')
        if len(parts) < 3:
            continue
        
        cores = parts[2].split('/')[0].split()[0]
        protocol = extra.split('/')[1].split(' - ')[0] if '/' in extra else 'unknown'
        key = f"{cores}core-{protocol}"
        
        if key not in metrics:
            metrics[key] = {}
        
        name = entry['name']
        if name == 'logs_received_rate':
            metrics[key]['throughput'] = entry['value']
        elif name == 'cpu_percentage_normalized_avg':
            metrics[key]['cpu'] = entry['value']
        elif name == 'speedup':
            metrics[key]['speedup'] = entry['value']
        elif name == 'scaling_efficiency':
            metrics[key]['efficiency'] = entry['value']
        elif name == 'dropped_logs_percentage':
            metrics[key]['dropped'] = entry['value']
    
    # Build markdown output
    lines = []
    lines.append("\n## 🚀 Core Scaling Analysis\n")
    lines.append("### OTLP Protocol\n")
    lines.append("| Cores | Throughput (logs/s) | Speedup | Efficiency | CPU % | Dropped % |\n")
    lines.append("|-------|--------------------:|--------:|-----------:|------:|----------:|\n")
    
    for cores in ['1', '2', '4', '8']:
        key = f"{cores}core-OTLP-ATTR-OTLP"
        if key in metrics and 'throughput' in metrics[key]:
            m = metrics[key]
            speedup = m.get('speedup', 0)
            efficiency = m.get('efficiency', 0)
            cpu = m.get('cpu', 0)
            throughput = m.get('throughput', 0)
            dropped = m.get('dropped', 0)
            
            # Add status emoji
            eff_emoji = "🟢" if efficiency >= 80 else "🟡" if efficiency >= 60 else "🔴"
            cpu_emoji = "✅" if cpu >= 90 else "⚠️" if cpu >= 70 else "❌"
            
            lines.append(f"| {cores} {eff_emoji} | {throughput:>15,.0f} | {speedup:>5.2f}x | {efficiency:>8.1f}% | {cpu:>4.1f}% {cpu_emoji} | {dropped:>6.2f}% |\n")
    
    lines.append("\n### OTAP Protocol\n")
    lines.append("| Cores | Throughput (logs/s) | Speedup | Efficiency | CPU % | Dropped % |\n")
    lines.append("|-------|--------------------:|--------:|-----------:|------:|----------:|\n")
    
    for cores in ['1', '2', '4', '8']:
        key = f"{cores}core-OTAP-ATTR-OTLP"
        if key in metrics and 'throughput' in metrics[key]:
            m = metrics[key]
            speedup = m.get('speedup', 0)
            efficiency = m.get('efficiency', 0)
            cpu = m.get('cpu', 0)
            throughput = m.get('throughput', 0)
            dropped = m.get('dropped', 0)
            
            # Add status emoji
            eff_emoji = "🟢" if efficiency >= 80 else "🟡" if efficiency >= 60 else "🔴"
            cpu_emoji = "✅" if cpu >= 90 else "⚠️" if cpu >= 70 else "❌"
            
            lines.append(f"| {cores} {eff_emoji} | {throughput:>15,.0f} | {speedup:>5.2f}x | {efficiency:>8.1f}% | {cpu:>4.1f}% {cpu_emoji} | {dropped:>6.2f}% |\n")
    
    lines.append("\n**Legend:**\n")
    lines.append("- 🟢 Efficiency ≥80% | 🟡 60-80% | 🔴 <60%\n")
    lines.append("- ✅ CPU ≥90% (saturated) | ⚠️ 70-90% | ❌ <70% (under-utilized)\n")
    lines.append("- **Ideal Linear Scaling:** Efficiency = 100%, Speedup = # of cores\n\n")
    
    markdown_output = ''.join(lines)
    
    # Determine output destination
    # Priority: 1) explicit output_file, 2) GITHUB_STEP_SUMMARY env var, 3) stdout
    if output_file:
        with open(output_file, 'a') as f:
            f.write(markdown_output)
        print(f"Summary appended to: {output_file}")
    elif 'GITHUB_STEP_SUMMARY' in os.environ:
        summary_file = os.environ['GITHUB_STEP_SUMMARY']
        with open(summary_file, 'a') as f:
            f.write(markdown_output)
        print(f"Summary appended to GitHub Step Summary: {summary_file}")
    else:
        print(markdown_output)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    generate_summary(input_file, output_file)
