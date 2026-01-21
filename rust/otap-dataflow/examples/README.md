# Apache Arrow with OpenTelemetry Examples

This directory contains educational examples demonstrating Apache Arrow fundamentals and how OpenTelemetry data is represented in the OTAP (OpenTelemetry Arrow Protocol) format.

## Quick Start

Run any example with:
```bash
cargo run --example <example_name>
```

## Examples

### 1. `arrow_basics` - Introduction to Arrow

**Run:** `cargo run --example arrow_basics`

**What you'll learn:**
- Apache Arrow fundamentals (columnar storage, arrays, RecordBatches)
- Why columnar format is efficient (SIMD, compression, cache-friendly)
- How OTel data maps to Arrow (OTAP structure)
- Real OTLP → Arrow conversion with actual data
- Benefits of Arrow for telemetry processing

**Topics covered:**
- Arrays and RecordBatches
- Columnar vs row-oriented storage
- Nested/structured data in Arrow
- OTAP star schema (4 tables: logs, log_attrs, scope_attrs, resource_attrs)
- Zero-copy operations

### 2. `arrow_operations` - Arrow Operations Deep Dive

**Run:** `cargo run --example arrow_operations`

**What you'll learn:**
- Detailed Arrow operations (slicing, projection, filtering)
- Working with RecordBatches
- Attribute table pattern (OTAP specific)
- Batching and merging operations
- Performance characteristics

**Topics covered:**
- Array access and iteration
- Zero-copy slicing
- Column projection (select subset)
- Vectorized filtering
- Star schema attribute lookups
- RecordBatch concatenation

### 3. `otlp_arrow_conversion` - Conversion Deep Dive

**Run:** `cargo run --example otlp_arrow_conversion`

**What you'll learn:**
- Zero-copy "view" pattern used in the codebase
- Step-by-step conversion from OTLP bytes to Arrow
- How RawLogsData view works
- Why this approach is efficient
- Performance characteristics

**Topics covered:**
- View pattern vs traditional deserialization
- RawLogsData and view traits
- Conversion code flow through the codebase
- Arrow builder usage
- Performance comparison

## Key Concepts

### Apache Arrow Basics

**What is Arrow?**
- Columnar in-memory data format
- Language-agnostic (zero-copy between Rust, Go, Python, etc.)
- Optimized for modern CPUs (SIMD-friendly, cache-efficient)
- Industry standard for analytics

**Core Types:**
- `Array`: Single column of data (e.g., `Int32Array`, `StringArray`)
- `RecordBatch`: Collection of columns with schema (like a table)
- `Schema`: Defines column names and types

### OTAP (OTel Arrow Protocol) Structure

OTAP represents OpenTelemetry data in a "star schema" with separate tables:

```
┌─────────────────────────────────┐
│ LOGS (Main Fact Table)          │
│ - id, time, severity, body      │
│ - resource: struct               │
│ - scope: struct                  │
└──────────────┬──────────────────┘
               │
       ┌───────┴───────┬─────────────┬──────────────┐
       │               │             │              │
       ▼               ▼             ▼              ▼
┌──────────┐  ┌──────────────┐  ┌─────────────┐  ┌────────────────┐
│LOG_ATTRS │  │SCOPE_ATTRS   │  │RESOURCE_    │  │(star schema    │
│(parent_  │  │(parent_id FK)│  │ATTRS        │  │ dimension      │
│ id FK)   │  │              │  │(parent_id   │  │ tables)        │
└──────────┘  └──────────────┘  │ FK)         │  └────────────────┘
                                 └─────────────┘
```

**Why this design?**
- Attributes vary per log (sparse data)
- Separate table = no null columns
- Better compression (similar values grouped)
- O(1) operations (rename attribute = scan one column)
- Efficient filtering without loading all fields

### Zero-Copy View Pattern

The codebase uses a "view" pattern to avoid intermediate allocations:

```
Traditional:     OTLP bytes → Prost objects → Arrow
                            ❌ allocations    ❌ allocations

Zero-copy view:  OTLP bytes → Views → Arrow
                            ✅ parse on-fly  ✅ direct build
```

**Views:**
- `RawLogsData`: View over OTLP bytes
- `LogRecordView`, `ResourceLogsView`, etc.: Trait-based API
- Parse protobuf tags on-demand
- No intermediate message objects

## Code Locations

**Key files to explore:**

1. **Arrow Schema Definitions:**
   - `crates/pdata/src/schema/consts.rs` - Column name constants
   - `crates/pdata/src/encode/record/logs.rs` - Arrow builders

2. **Conversion Code:**
   - `crates/pdata/src/payload.rs` - TryFrom implementations
   - `crates/otap/src/encoder.rs` - OTLP → Arrow conversion
   - `crates/pdata/src/views/` - Zero-copy view traits

3. **Data Types:**
   - `crates/pdata/src/otap.rs` - OtapArrowRecords enum
   - `crates/pdata/src/otlp/mod.rs` - OtlpProtoBytes enum
   - `crates/otap/src/pdata.rs` - OtapPdata (pipeline data)

4. **gRPC Integration:**
   - `crates/otap/src/otap_grpc/otlp/server_new.rs` - OTLP decoder
   - `crates/otap/src/otlp_receiver.rs` - OTLP receiver

## Arrow Benefits for Telemetry

1. **Compression:** 3-10x better than OTLP (similar values grouped)
2. **Processing Speed:** Columnar layout enables SIMD operations
3. **Memory Efficiency:** Zero-copy operations, no intermediate objects
4. **Interoperability:** Same format across languages (Rust, Go, Python)
5. **Analytics:** Direct query with Arrow/DataFusion
6. **Efficient Operations:** O(1) attribute renames, column pruning

## Further Reading

- [Apache Arrow Documentation](https://arrow.apache.org/)
- [OTAP Basics](../docs/otap_basics.md)
- [OTel Arrow Data Model](../docs/data_model.md)
- [Design Principles](../docs/design-principles.md)
- [Crate READMEs](../crates/README.md)

## Next Steps

After running these examples:

1. Explore `crates/pdata/src/views/` for view implementations
2. Look at `crates/pdata/src/encode/record/` for Arrow builders
3. Read `crates/otap/src/encoder.rs` for conversion logic
4. Try modifying the examples to process different data
5. Run benchmarks: `cargo bench` in the `benchmarks/` directory

## Questions?

- Check the main [README](../README.md)
- Review [CONTRIBUTING.md](../CONTRIBUTING.md)
- Join [#otel-arrow](https://slack.cncf.io/) on CNCF Slack
