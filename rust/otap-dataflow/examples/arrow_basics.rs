// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! # Arrow Basics with OpenTelemetry Examples
//!
//! This example demonstrates Apache Arrow fundamentals and how OpenTelemetry
//! data is represented in Arrow format (OTAP - OTel Arrow Protocol).
//!
//! Run with: cargo run --example arrow_basics

use arrow::array::{
    Array, ArrayRef, BinaryArray, Int32Array, RecordBatch, StringArray, StructArray,
    TimestampNanosecondArray, UInt16Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use otap_df_pdata::otap::OtapBatchStore;
use otap_df_pdata::proto::opentelemetry::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
    resource::v1::Resource,
};
use otap_df_pdata::{OtapArrowRecords, OtapPayload, OtlpProtoBytes};
use prost::Message;
use std::sync::Arc;

fn main() {
    println!("\n╔═══════════════════════════════════════════════════════════════════╗");
    println!("║          Apache Arrow Basics with OpenTelemetry                  ║");
    println!("╚═══════════════════════════════════════════════════════════════════╝\n");

    // Part 1: Arrow Basics
    arrow_fundamentals();

    // Part 2: OTel Data in Arrow (OTAP)
    otel_arrow_representation();

    // Part 3: Real Conversion Example
    otlp_to_arrow_conversion();

    // Part 4: Arrow Benefits
    arrow_benefits_demo();

    println!("\n✅ Example completed!\n");
}

/// Part 1: Demonstrate core Arrow concepts
fn arrow_fundamentals() {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📚 Part 1: Apache Arrow Fundamentals");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    println!("🔹 What is Apache Arrow?");
    println!("   • Columnar in-memory data format");
    println!("   • Language-agnostic (zero-copy between systems)");
    println!("   • CPU/SIMD-friendly (vectors in contiguous memory)");
    println!("   • Efficient for analytics and bulk operations\n");

    // Example 1: Simple Array
    println!("📊 Example 1: Simple Arrays (columns)");
    let names = StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana"]);
    let ages = Int32Array::from(vec![25, 30, 35, 28]);
    let active = arrow::array::BooleanArray::from(vec![true, true, false, true]);

    println!("   Names:  {:?}", names.iter().collect::<Vec<_>>());
    println!("   Ages:   {:?}", ages.values());
    println!("   Active: {:?}", active.iter().collect::<Vec<_>>());
    println!("   ➜ Each column is a contiguous array in memory\n");

    // Example 2: RecordBatch (Table)
    println!("📊 Example 2: RecordBatch (like a table)");
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("active", DataType::Boolean, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(names), Arc::new(ages), Arc::new(active)],
    )
    .unwrap();

    println!("   Schema: {}", schema);
    println!("   Rows:   {}", batch.num_rows());
    println!("   Cols:   {}", batch.num_columns());
    println!("   ➜ RecordBatch = Schema + Arrays (columnar storage)\n");

    // Example 3: Why Columnar?
    println!("💡 Example 3: Why Columnar Format?");
    println!("   Row-oriented (traditional):     | Alice | 25 | T | Bob | 30 | T | ...");
    println!("   Column-oriented (Arrow):        | Alice, Bob, Charlie, Diana | (names)");
    println!("                                   | 25, 30, 35, 28 |            (ages)");
    println!("                                   | T, T, F, T |                (active)\n");
    println!("   Benefits:");
    println!("   ✓ Better compression (similar values together)");
    println!("   ✓ SIMD operations (process multiple values at once)");
    println!("   ✓ Skip unused columns (only read what you need)");
    println!("   ✓ Cache-friendly (sequential memory access)\n");

    // Example 4: Nested Data
    println!("📊 Example 4: Nested/Structured Data");
    let address_struct = StructArray::from(vec![
        (
            Arc::new(Field::new("city", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["NYC", "LA"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("zip", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![10001, 90001])) as ArrayRef,
        ),
    ]);

    println!("   Struct column 'address': {{city: Utf8, zip: Int32}}");
    println!("   Row 0: city='{}', zip={}",
             address_struct.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0),
             address_struct.column(1).as_any().downcast_ref::<Int32Array>().unwrap().value(0));
    println!("   ➜ Arrow supports complex nested types (structs, lists, maps)\n");
}

/// Part 2: Show how OTel data maps to Arrow
fn otel_arrow_representation() {
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🎯 Part 2: OpenTelemetry Data in Arrow (OTAP)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    println!("🔹 OTLP (Protocol Buffers) vs OTAP (Arrow)");
    println!("   OTLP: Hierarchical, nested protobuf messages");
    println!("   OTAP: Flattened, columnar \"star schema\"\n");

    println!("📋 OTAP Logs Structure (4 RecordBatches):");
    println!("   ┌─────────────────────────────────────────────┐");
    println!("   │ 1️⃣  LOGS Table (main log records)          │");
    println!("   │    - id: u16                                │");
    println!("   │    - time_unix_nano: timestamp              │");
    println!("   │    - observed_time_unix_nano: timestamp     │");
    println!("   │    - severity_number: i32                   │");
    println!("   │    - severity_text: binary                  │");
    println!("   │    - body: struct (AnyValue)                │");
    println!("   │    - flags: u32                             │");
    println!("   │    - trace_id: fixed_size_binary[16]        │");
    println!("   │    - span_id: fixed_size_binary[8]          │");
    println!("   │    - resource: struct                       │");
    println!("   │    - scope: struct                          │");
    println!("   │    - schema_url: binary                     │");
    println!("   │    - event_name: binary                     │");
    println!("   ├─────────────────────────────────────────────┤");
    println!("   │ 2️⃣  LOG_ATTRS Table (log attributes)       │");
    println!("   │    - parent_id: u16 (FK to logs.id)         │");
    println!("   │    - key: binary                            │");
    println!("   │    - type: i32 (str/int/double/bool/etc)    │");
    println!("   │    - str/int/double/bool/bytes: values      │");
    println!("   ├─────────────────────────────────────────────┤");
    println!("   │ 3️⃣  SCOPE_ATTRS Table (scope attributes)   │");
    println!("   │    - parent_id: u16 (FK to logs.scope.id)   │");
    println!("   │    - key/type/values: similar to above      │");
    println!("   ├─────────────────────────────────────────────┤");
    println!("   │ 4️⃣  RESOURCE_ATTRS Table (resource attrs)  │");
    println!("   │    - parent_id: u16 (FK to logs.resource.id)│");
    println!("   │    - key/type/values: similar to above      │");
    println!("   └─────────────────────────────────────────────┘\n");

    println!("🔗 Star Schema Pattern:");
    println!("   • Main 'fact' table (logs/spans/metrics)");
    println!("   • Separate 'dimension' tables for attributes");
    println!("   • Connected via parent_id (foreign key)");
    println!("   • Enables efficient columnar operations\n");

    println!("💡 Why This Design?");
    println!("   ✓ Attributes vary widely (different keys per log)");
    println!("   ✓ Separate table = no sparse/null columns");
    println!("   ✓ Better compression (attribute keys/types grouped)");
    println!("   ✓ O(1) operations: rename attribute = rename one column");
    println!("   ✓ Filter by attribute without loading all log fields\n");

    // Show example schema
    println!("📐 Example: Logs RecordBatch Schema");
    let logs_schema = create_example_logs_schema();
    for (i, field) in logs_schema.fields().iter().enumerate() {
        println!("   Column {}: {} ({})", i, field.name(), field.data_type());
    }
    println!();
}

/// Helper to create example logs schema
fn create_example_logs_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::UInt16, true),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "observed_time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("severity_number", DataType::Int32, true),
        Field::new("severity_text", DataType::Binary, true),
        Field::new("body", DataType::Utf8, true), // Simplified
        Field::new("flags", DataType::UInt32, true),
        Field::new("trace_id", DataType::FixedSizeBinary(16), true),
        Field::new("span_id", DataType::FixedSizeBinary(8), true),
    ])
}

/// Part 3: Real conversion from OTLP bytes to Arrow
fn otlp_to_arrow_conversion() {
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🔄 Part 3: OTLP → Arrow Conversion (Real Code)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // Create sample OTLP data
    println!("📝 Creating sample OTLP log data...");
    let otlp_request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    KeyValue::new("service.name", AnyValue::new_string("arrow-demo")),
                    KeyValue::new("service.version", AnyValue::new_string("1.0.0")),
                ],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "example-logger".to_string(),
                    version: "1.0".to_string(),
                    ..Default::default()
                }),
                log_records: vec![
                    LogRecord {
                        time_unix_nano: 1704110400000000000,
                        observed_time_unix_nano: 1704110400000000000,
                        severity_number: SeverityNumber::Info as i32,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue::new_string("User logged in")),
                        attributes: vec![
                            KeyValue::new("user.id", AnyValue::new_int(12345)),
                            KeyValue::new("user.name", AnyValue::new_string("alice")),
                        ],
                        ..Default::default()
                    },
                    LogRecord {
                        time_unix_nano: 1704110401000000000,
                        observed_time_unix_nano: 1704110401000000000,
                        severity_number: SeverityNumber::Warn as i32,
                        severity_text: "WARN".to_string(),
                        body: Some(AnyValue::new_string("Slow query")),
                        attributes: vec![
                            KeyValue::new("db.duration_ms", AnyValue::new_int(5000)),
                        ],
                        ..Default::default()
                    },
                    LogRecord {
                        time_unix_nano: 1704110402000000000,
                        observed_time_unix_nano: 1704110402000000000,
                        severity_number: SeverityNumber::Error as i32,
                        severity_text: "ERROR".to_string(),
                        body: Some(AnyValue::new_string("Failed operation")),
                        attributes: vec![
                            KeyValue::new("error.type", AnyValue::new_string("NetworkError")),
                        ],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    println!("   ✓ Created 3 log records with attributes\n");

    // Encode to OTLP bytes
    let mut otlp_bytes = Vec::new();
    otlp_request.encode(&mut otlp_bytes).unwrap();
    println!("📦 OTLP Protobuf size: {} bytes", otlp_bytes.len());

    // Convert to OTAP (Arrow)
    println!("🔄 Converting OTLP bytes → Arrow records...\n");
    let otlp_proto_bytes = OtlpProtoBytes::ExportLogsRequest(Bytes::from(otlp_bytes));
    let payload = OtapPayload::from(otlp_proto_bytes);

    // Convert to Arrow
    let otap_records: OtapArrowRecords = payload.try_into().unwrap();

    // Inspect the Arrow representation
    if let OtapArrowRecords::Logs(logs) = &otap_records {
        println!("✅ Conversion successful! Inspecting Arrow RecordBatches:\n");

        // 1. Logs table
        if let Some(logs_batch) = logs.get(
            otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType::Logs,
        ) {
            println!("━━━ 1️⃣  LOGS RecordBatch ━━━");
            println!("Schema:");
            for field in logs_batch.schema().fields() {
                println!("  • {} : {}", field.name(), field.data_type());
            }
            println!("\nData ({} rows):", logs_batch.num_rows());

            // Show time_unix_nano column
            let time_col = logs_batch
                .column_by_name("time_unix_nano")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            println!("  time_unix_nano: {:?}", &time_col.values()[..3.min(time_col.len())]);

            // Show severity (it's a dictionary-encoded column)
            if let Some(severity_col) = logs_batch.column_by_name("severity_number") {
                println!("  severity_number column type: {}", severity_col.data_type());
            }

            // Show body (it's a struct with dictionary-encoded string)
            if let Some(body_col) = logs_batch.column_by_name("body") {
                println!("  body column type: {}", body_col.data_type());
                println!("  body rows: {}", body_col.len());
            }
            println!();
        }

        // 2. Log attributes table
        if let Some(attrs_batch) = logs.get(
            otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType::LogAttrs,
        ) {
            println!("━━━ 2️⃣  LOG_ATTRS RecordBatch ━━━");
            println!("Schema:");
            for field in attrs_batch.schema().fields() {
                println!("  • {} : {}", field.name(), field.data_type());
            }
            println!("\nData ({} attribute rows):", attrs_batch.num_rows());

            // Show parent_id (links to logs)
            if let Some(parent_col) = attrs_batch.column_by_name("parent_id") {
                let parent_ids = parent_col.as_any().downcast_ref::<UInt16Array>().unwrap();
                println!(
                    "  parent_id: {:?}",
                    &parent_ids.values()[..5.min(parent_ids.len())]
                );
            }

            // Show attribute keys
            if let Some(key_col) = attrs_batch.column_by_name("key") {
                if let Some(key_array) = key_col.as_any().downcast_ref::<BinaryArray>() {
                    print!("  keys (first 5): [");
                    for i in 0..5.min(key_array.len()) {
                        if !key_array.is_null(i) {
                            print!("\"{}\"", String::from_utf8_lossy(key_array.value(i)));
                            if i < 4.min(key_array.len() - 1) {
                                print!(", ");
                            }
                        }
                    }
                    println!("]");
                }
            }
            println!();
        }

        // 3. Scope attributes
        if let Some(scope_attrs) = logs.get(
            otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType::ScopeAttrs,
        ) {
            println!("━━━ 3️⃣  SCOPE_ATTRS RecordBatch ━━━");
            println!("Rows: {}", scope_attrs.num_rows());
            println!("Columns: {}", scope_attrs.num_columns());
            println!();
        }

        // 4. Resource attributes
        if let Some(resource_attrs) = logs.get(
            otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType::ResourceAttrs,
        ) {
            println!("━━━ 4️⃣  RESOURCE_ATTRS RecordBatch ━━━");
            println!("Rows: {}", resource_attrs.num_rows());

            // Show resource attribute keys
            if let Some(key_col) = resource_attrs.column_by_name("key") {
                if let Some(key_array) = key_col.as_any().downcast_ref::<BinaryArray>() {
                    print!("  Resource keys: [");
                    for i in 0..key_array.len() {
                        if !key_array.is_null(i) {
                            print!("\"{}\"", String::from_utf8_lossy(key_array.value(i)));
                            if i < key_array.len() - 1 {
                                print!(", ");
                            }
                        }
                    }
                    println!("]");
                }
            }
            println!();
        }
    }
}

/// Part 4: Demonstrate Arrow's benefits
fn arrow_benefits_demo() {
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("⚡ Part 4: Arrow Benefits in Action");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    println!("🚀 1. Zero-Copy Between Systems");
    println!("   • Arrow format is language-agnostic");
    println!("   • Rust → Go → Python: no serialization overhead");
    println!("   • Same memory layout across languages");
    println!("   • Example: Arrow IPC (Inter-Process Communication) wire format\n");

    println!("🎯 2. Efficient Filtering (Column Pruning)");
    println!("   OTLP (protobuf):  Must parse entire nested structure");
    println!("   OTAP (Arrow):     Read only needed columns");
    println!("   ");
    println!("   Example: Filter logs by severity");
    println!("   → Only load 'severity_number' column (skip body, attributes, etc.)");
    println!("   → Process as vector: [9, 13, 17, 9, ...] (contiguous memory)\n");

    println!("📊 3. SIMD Acceleration");
    println!("   • Modern CPUs process multiple values in single instruction");
    println!("   • Arrow's contiguous layout enables SIMD");
    println!("   ");
    println!("   Example: Count ERROR logs (severity >= 17)");
    println!("   Traditional: for (item in items) {{ if item.severity >= 17 {{ count++ }} }}");
    println!("   SIMD:       Process 8 severities at once with comparison vector\n");

    println!("🔧 4. O(1) Attribute Operations (OTAP Specific)");
    println!("   Attribute Processor: Rename 'old_key' → 'new_key'");
    println!("   ");
    println!("   OTLP approach:");
    println!("   • Parse all protobuf messages");
    println!("   • Traverse nested structures");
    println!("   • Find/replace key in each log record");
    println!("   • Re-serialize → O(n) per message");
    println!("   ");
    println!("   OTAP approach:");
    println!("   • Find 'key' column in LOG_ATTRS table");
    println!("   • Replace matching entries in single array");
    println!("   • No re-parsing/re-serialization → O(1) operation\n");

    println!("💾 5. Compression Efficiency");
    println!("   • Similar values grouped together (columnar)");
    println!("   • Example: severity_number column [9,9,9,13,9,17,17,9]");
    println!("   • Run-length encoding: 9×3, 13×1, 9×1, 17×2, 9×1");
    println!("   • Dictionary encoding for repeated strings");
    println!("   • Better compression ratios than row-oriented formats\n");

    println!("🔗 6. Zero-Copy Views (OTAP Implementation)");
    println!("   • RawLogsData: View over OTLP bytes without deserialization");
    println!("   • Parse protobuf tags on-the-fly");
    println!("   • Build Arrow arrays directly from wire format");
    println!("   • No intermediate Prost message objects");
    println!("   • Saves memory allocations and CPU cycles\n");

    println!("📈 Real-World Impact:");
    println!("   ✓ 3-10x compression improvement over OTLP");
    println!("   ✓ Faster processing (columnar + SIMD)");
    println!("   ✓ Lower memory footprint");
    println!("   ✓ Better cache utilization");
    println!("   ✓ Enables efficient analytics on telemetry data\n");
}
