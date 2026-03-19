# OpenTelemetry Arrow Performance Summary

## Overview

The OpenTelemetry Arrow (OTel Arrow) project is currently in **Phase 2**,
building an end-to-end Arrow-based telemetry pipeline in Rust. Phase 1 focused
on collector-to-collector traffic compression using the OTAP protocol, achieving
significant network bandwidth savings. Phase 2 expands this foundation by
implementing the entire in-process pipeline using Apache Arrow's columnar
format, targeting substantial improvements in data processing efficiency while
maintaining the network efficiency gains from Phase 1.

The OTel Arrow dataflow engine, implemented in Rust, provides predictable
performance characteristics and efficient resource utilization across varying
load conditions. The engine uses a [thread-per-core
architecture](#thread-per-core-design) where resource consumption scales with
the number of configured cores.

This document presents a curated set of key performance metrics across different
load scenarios and test configurations. For the complete set of automated
performance tests (continuous, nightly, saturation, idle state, and binary size
benchmarks), see [Detailed Benchmark
Results](benchmarks.md#current-performance-results).

### Test Environment

All performance tests are executed on bare-metal compute instance with the
following specifications:

- **CPU**: 64 physical cores / 128 logical cores (x86-64 architecture)
- **Memory**: 512 GB RAM
- **Platform**: Oracle Bare Metal Instance
- **OS**: Oracle Linux 8

This consistent, high-performance environment ensures reproducible results and
allows for comprehensive testing across various CPU core configurations (1, 4,
and 8 cores etc.) by constraining the OTel Arrow dataflow engine to specific
core allocations.

### Performance Metrics

#### Idle State Performance

Baseline resource consumption with no active telemetry traffic, measured after
startup stabilization over a 60-second period. These metrics represent
post-initialization idle state and validate minimal resource footprint. Note
that longer-duration soak testing for memory leak detection is outside the scope
of this benchmark summary.

| Configuration | CPU Usage | Memory Usage |
|---------------|-----------|--------------|
| Single Core   | 0.06%     | 27 MB        |
| All Cores (128) | 2.5%    | 600 MB       |

*Note: CPU usage is normalized (percentage of total system capacity). Memory
usage scales with core count due to the [thread-per-core
architecture](#thread-per-core-design).*

These baseline metrics validate that the engine maintains minimal resource
footprint when idle, ensuring efficient operation in environments with variable
telemetry loads.

#### Standard Load Performance (Single Core)

Resource utilization at 100,000 log records per second (100K logs/sec) on a
single CPU core. Tests are conducted with four different batch sizes to
demonstrate the impact of batching on performance.

**Test Parameters:**

- Total input load: 100,000 log records/second
- Average log record size: 1 KB
- Batch sizes tested: 10, 100, 1000, 5000, and 10000 records per request
- Test duration: 60 seconds

This wide range of batch sizes evaluates performance across diverse deployment
scenarios. Small batches (10-100) represent edge collectors or real-time
streaming requirements, while large batches (1000-10000) represent gateway
collectors and high-throughput aggregation points. This approach ensures a fair
assessment, highlighting both the overhead for small batches and the significant
efficiency gains inherent to Arrow's columnar format at larger batch sizes.

<!-- TODO: We need to add BatchingProcessor to tests. -->
<!-- TODO: Batch size influence might be most relevant when we
 do aggregation/transform etc. -->

##### Standard Load - OTAP -> OTAP (Native Protocol)

| Batch Size | CPU Usage | Memory Usage | Network In | Network Out |
|------------|-----------|--------------|------------|-------------|
| 10/batch | TBD | TBD | TBD | TBD |
| 100/batch | TBD | TBD | TBD | TBD |
| 1000/batch | 17% | 46 MB | 718 KB/s | 750 KB/s |
| 5000/batch | 7% | 50 MB | 390 KB/s | 422 KB/s |
| 10000/batch | 5% | 58 MB | 350 KB/s | 383 KB/s |

This represents the optimal scenario where the dataflow engine operates with its
native protocol end-to-end, eliminating protocol conversion overhead.

##### Standard Load - OTLP -> OTLP (Standard Protocol)

| Batch Size | CPU Usage | Memory Usage | Network In | Network Out |
|------------|-----------|--------------|------------|-------------|
| 10/batch | 100%* | 60 MB | 3.9 MB/s | 4.1 MB/s |
| 100/batch | 91% | 67 MB | 4.4 MB/s | 4.5 MB/s |
| 1000/batch | 43% | 52 MB | 2.1 MB/s | 2.2 MB/s |
| 5000/batch | 40% | 79 MB | 1.9 MB/s | 2.0 MB/s |
| 10000/batch | 40% | 80 MB | 1.8 MB/s | 1.9 MB/s |

*\*At batch size 10, the single core saturates at 100% CPU and only achieves
~33K logs/sec — well below the 100K target. This demonstrates the per-request
overhead dominance at very small batch sizes.*

This scenario processes OTLP end-to-end using the standard OpenTelemetry
protocol, providing a baseline for comparison with traditional OTLP-based
pipelines.

#### Saturation Performance (Single Core)

Maximum throughput achievable on a single CPU core at full utilization. This
establishes the baseline "unit of capacity" for capacity planning.

**Test Parameters:**

- Batch size: 500 records per request
- Load: Continuously increased until the CPU core is fully saturated
- Test duration: 60 seconds at maximum load

##### Pass-through Mode

Forwarding without data transformation. Represents the minimum engine overhead
for load balancing and routing use cases.

| Protocol | Max Throughput | CPU Usage | Memory Usage |
|----------|----------------|-----------|--------------|
| OTAP -> OTAP (Native) | TBD | TBD | TBD |
| OTLP -> OTLP (Standard) | ~566K logs/sec | ~98% | ~45 MB |

##### With Processing

Includes an attribute processor to force data materialization. Represents
typical production workloads where collectors perform transformations such as
filtering, attribute enrichment, renaming, or aggregation.

| Protocol | Max Throughput | CPU Usage | Memory Usage |
|----------|----------------|-----------|--------------|
| OTAP -> OTAP (Native) | TBD | TBD | TBD |
| OTLP -> OTLP (Standard) | ~238K logs/sec | ~99% | ~38 MB |

#### Scalability

How throughput scales when adding CPU cores. The [thread-per-core
architecture](#thread-per-core-design) enables near-linear scaling by
eliminating shared-state synchronization overhead.

**Test Parameters:**

- Batch size: 512 records per request
- Protocol: OTLP -> OTLP with attribute processing
- Load: Maximum sustained throughput at each core count

| CPU Cores | Max Throughput | CPU Usage | Scaling Efficiency | Memory Usage |
|-----------|----------------|-----------|-------------------|--------------|
| 1 Core    | ~238K logs/sec | ~99%      | 100% (baseline)   | ~38 MB       |
| 2 Cores   | ~414K logs/sec | ~91%      | 87%               | ~55 MB       |
| 4 Cores   | ~653K logs/sec | ~70%*     | 69%               | ~80 MB       |
| 8 Cores   | ~1.47M logs/sec | ~82%*    | 78%               | ~178 MB      |
| 16 Cores  | ~2.37M logs/sec | ~78%*    | 62%               | ~288 MB      |
| 24 Cores  | ~3.67M logs/sec | ~77%*    | 64%               | ~461 MB      |

TODO: Use more load-generator so saturate CPU at higher cores.

Scaling Efficiency = (Throughput at N cores) / (N * Single-core throughput)

### Architecture

The OTel Arrow dataflow engine is built in Rust, to achieve high throughput and
low latency. The columnar data representation and zero-copy processing
capabilities enable efficient handling of telemetry data at scale.

#### Thread-Per-Core Design

The dataflow engine supports a configurable runtime execution model, using a
**thread-per-core architecture** that eliminates traditional concurrency
overhead. This design allows:

- **CPU Affinity Control**: Pipelines can be pinned to specific CPU cores or
  groups through configuration
- **NUMA Optimization**: Memory and CPU assignments can be coordinated for
  Non-Uniform Memory Access (NUMA) architectures
- **Workload Isolation**: Different telemetry signals or tenants can be assigned
  to dedicated CPU resources, preventing resource contention
- **Reduced Synchronization**: Thread-per-core design minimizes lock contention
  and context switching overhead

For detailed technical documentation, see the [OTAP Dataflow Engine
Documentation](../rust/otap-dataflow/README.md) and [Phase 2
Design](phase2-design.md).

---

## Comparative Analysis: OTel Arrow vs OpenTelemetry Collector

### Methodology

To provide a fair and meaningful comparison between the OTel Arrow dataflow
engine and the OpenTelemetry Collector, we use **Syslog (UDP/TCP)** as the
ingress protocol for both systems.

#### Rationale for Syslog-Based Comparison

Syslog was specifically chosen as the input protocol because:

1. Neutral Ground: Syslog is neither OTLP (OpenTelemetry Protocol) nor OTAP
   (OpenTelemetry Arrow Protocol), ensuring neither system has a native protocol
   advantage
2. Real-World Relevance: Syslog is widely deployed in production environments,
   particularly for log aggregation from network devices, legacy systems, and
   infrastructure components
3. Conversion Overhead: Both systems must perform meaningful work to convert
   incoming Syslog messages into their internal representations:
   - **OTel Collector**: Converts to Go-based `pdata` (protocol data) structures
   - **OTel Arrow**: Converts to Arrow-based columnar memory format
4. Complete Pipeline Test: This approach validates the full pipeline efficiency,
   including parsing, transformation, and serialization stages

Both OTLP and OTAP are used as output protocols to measure egress performance
across different serialization formats.

### Performance Comparison

**Test Parameters:**

- Input protocol: Syslog RFC 3164 (UDP)
- Input load: 5,000 messages/second
- Syslog message size: ~200 bytes (non-CEF) / ~200 bytes (CEF)
- Output protocols: OTLP and OTAP
- Test duration: 60 seconds

<!-- TODO: Increase syslog load to 100K messages/sec to match the standard load
     section. Current test config uses target_rate=5000. -->

#### Syslog (5K Messages/sec) - OTLP Output

| Metric | OTel Collector | OTel Arrow | Improvement |
|--------|---------------|------------|-------------|
| CPU Usage | TBD | 9% | TBD |
| Memory Usage | TBD | 47 MB | TBD |
| Network Egress | TBD | 89 KB/s | TBD |
| Throughput (messages/sec) | TBD | ~5,420 (0% drop) | TBD |

#### Syslog (5K Messages/sec) - OTAP Output

| Metric | OTel Collector | OTel Arrow | Improvement |
|--------|---------------|------------|-------------|
| CPU Usage | TBD | 12% | TBD |
| Memory Usage | TBD | 46 MB | TBD |
| Network Egress | TBD | 58 KB/s | TBD |
| Throughput (messages/sec) | TBD | ~5,418 (0% drop) | TBD |

*Note: OTAP output achieves lower network egress (58 KB/s vs 89 KB/s) due to
Arrow's columnar compression, while CPU usage is slightly higher (12% vs 9%)
due to Arrow encoding overhead at this low throughput.*

#### Syslog CEF Parsing - OTLP Output

| Metric | OTel Arrow (non-CEF) | OTel Arrow (CEF) |
|--------|---------------------|------------------|
| CPU Usage | 9% | 13% |
| Memory Usage | 47 MB | 47 MB |
| Network Egress | 89 KB/s | 128 KB/s |
| Throughput | ~5,420 logs/sec | ~5,420 logs/sec |

*CEF parsing adds ~4% CPU overhead due to structured field extraction, with
larger egress (128 KB/s vs 89 KB/s) reflecting the additional parsed fields.*

### Key Findings

**OTel Arrow (Syslog ingestion, single core):**

- At 5K syslog messages/sec, the engine uses only 9-13% CPU on a single core,
  leaving substantial headroom for higher loads
- OTAP output reduces network egress by ~35% compared to OTLP output (58 KB/s
  vs 89 KB/s) due to Arrow's columnar compression
- CEF parsing adds ~4% CPU overhead but zero impact on throughput at this load
  level
- Memory footprint remains constant at ~46-47 MB regardless of output protocol

**Pending:**

- OTel Collector (Go) comparative numbers — requires new test configuration
  pairing the Go collector with syslog input
- Higher syslog load tests (100K messages/sec) to stress both systems
- Relative efficiency of Arrow-based columnar processing vs traditional
  row-oriented data structures
- Memory allocation patterns and garbage collection impact (Rust vs Go)

---

## Additional Resources

- [Detailed Benchmark Results from phase2](benchmarks.md)
- [Phase 1 Benchmark Results](benchmarks-phase1.md)
- [OTAP Dataflow Engine Documentation](../rust/otap-dataflow/README.md)
- [Project Phases Overview](project-phases.md)
