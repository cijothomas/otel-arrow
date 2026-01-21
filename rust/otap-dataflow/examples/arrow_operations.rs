// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! # Arrow Operations Deep Dive
//!
//! This example shows detailed Arrow operations and how they're used
//! in the OTAP dataflow pipeline.
//!
//! Run with: cargo run --example arrow_operations

use arrow::array::{
    Array, BinaryArray, Int32Array, RecordBatch, StringArray, UInt16Array,
};
use arrow::compute::{self, kernels::cmp};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

fn main() {
    println!("\n╔═══════════════════════════════════════════════════════════════════╗");
    println!("║              Arrow Operations Deep Dive                          ║");
    println!("╚═══════════════════════════════════════════════════════════════════╝\n");

    demo_array_operations();
    demo_record_batch_operations();
    demo_filtering_and_selection();
    demo_attribute_operations();
    demo_batching_operations();

    println!("\n✅ All examples completed!\n");
}

/// Demonstrate basic array operations
fn demo_array_operations() {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🔢 Array Operations");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // Create arrays
    println!("📝 Creating sample arrays...");
    let ids = UInt16Array::from(vec![0, 1, 2, 3, 4]);
    let severities = Int32Array::from(vec![9, 13, 9, 17, 13]); // DEBUG, INFO, DEBUG, ERROR, INFO
    let messages = StringArray::from(vec![
        "Starting service",
        "Configuration loaded",
        "Health check passed",
        "Connection failed",
        "Retrying connection",
    ]);

    println!("   IDs:        {:?}", ids.values());
    println!("   Severities: {:?}", severities.values());
    println!("   Messages:   {:?}", messages.iter().collect::<Vec<_>>());
    println!();

    // Array properties
    println!("📊 Array Properties:");
    println!("   Length:     {}", ids.len());
    println!("   Null count: {}", ids.null_count());
    println!("   Data type:  {:?}", ids.data_type());
    println!();

    // Access elements
    println!("🔍 Accessing Elements:");
    println!("   Message[2]: {}", messages.value(2));
    println!("   Severity[3]: {}", severities.value(3));
    println!();

    // Memory layout
    println!("💾 Memory Layout:");
    println!("   Severities buffer: {:?}", &severities.values()[..]);
    println!("   ➜ Contiguous memory = cache-friendly, SIMD-ready");
    println!();
}

/// Demonstrate RecordBatch operations
fn demo_record_batch_operations() {
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📋 RecordBatch Operations");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // Create a RecordBatch representing a simplified log table
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt16, false),
        Field::new("severity", DataType::Int32, false),
        Field::new("message", DataType::Utf8, false),
        Field::new("duration_ms", DataType::Int32, true), // nullable
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt16Array::from(vec![0, 1, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![9, 13, 9, 17, 13])),
            Arc::new(StringArray::from(vec![
                "Starting service",
                "Configuration loaded",
                "Health check passed",
                "Connection failed",
                "Retrying connection",
            ])),
            Arc::new(Int32Array::from(vec![Some(100), None, Some(50), Some(5000), Some(200)])),
        ],
    )
    .unwrap();

    println!("📊 RecordBatch Info:");
    println!("   Rows:    {}", batch.num_rows());
    println!("   Columns: {}", batch.num_columns());
    println!();

    // Column access by name
    println!("🔍 Access Column by Name:");
    let severity_col = batch.column_by_name("severity").unwrap();
    let severity_array = severity_col.as_any().downcast_ref::<Int32Array>().unwrap();
    println!("   Severity column: {:?}", severity_array.values());
    println!();

    // Slice operation (zero-copy)
    println!("✂️  Slice Operation (Zero-Copy):");
    let slice = batch.slice(1, 3); // From row 1, take 3 rows
    println!("   Original rows: {}", batch.num_rows());
    println!("   Sliced rows:   {}", slice.num_rows());
    println!("   ➜ No data copied, just offset adjustment!");
    println!();

    // Project columns (select subset)
    println!("📐 Project Columns (Select Subset):");
    let projection = batch.project(&[0, 2]).unwrap(); // id and message only
    println!("   Original columns: {}", batch.num_columns());
    println!("   Projected columns: {}", projection.num_columns());
    for field in projection.schema().fields() {
        println!("     • {}", field.name());
    }
    println!("   ➜ Useful for reading only what you need!");
    println!();
}

/// Demonstrate filtering and selection
fn demo_filtering_and_selection() {
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🔎 Filtering and Selection");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    let severities = Int32Array::from(vec![9, 13, 9, 17, 13, 9, 17]);
    let messages = StringArray::from(vec![
        "Debug 1",
        "Info 1",
        "Debug 2",
        "Error 1",
        "Info 2",
        "Debug 3",
        "Error 2",
    ]);

    println!("📊 Original Data:");
    println!("   Severities: {:?}", severities.values());
    println!("   Messages:   {:?}", messages.iter().collect::<Vec<_>>());
    println!();

    // Filter: severity >= 13 (INFO and above)
    println!("🔍 Filter: severity >= 13 (INFO and above)");
    let threshold = Int32Array::new_scalar(13);
    let filter_mask = cmp::gt_eq(&severities, &threshold).unwrap();
    println!("   Filter mask: {:?}", filter_mask);

    let filtered_severities = compute::filter(&severities, &filter_mask).unwrap();
    let filtered_messages = compute::filter(&messages, &filter_mask).unwrap();

    let filtered_sev = filtered_severities
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let filtered_msg = filtered_messages
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    println!("   Filtered severities: {:?}", filtered_sev.values());
    println!(
        "   Filtered messages:   {:?}",
        filtered_msg.iter().collect::<Vec<_>>()
    );
    println!("   ➜ Vectorized comparison + selection");
    println!();

    // Count by severity
    println!("📊 Count by Severity Level:");
    let debug_count = severities.values().iter().filter(|&&s| s == 9).count();
    let info_count = severities.values().iter().filter(|&&s| s == 13).count();
    let error_count = severities.values().iter().filter(|&&s| s == 17).count();
    println!("   DEBUG (9):  {} logs", debug_count);
    println!("   INFO (13):  {} logs", info_count);
    println!("   ERROR (17): {} logs", error_count);
    println!();
}

/// Demonstrate attribute table operations (OTAP pattern)
fn demo_attribute_operations() {
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🏷️  Attribute Table Operations (OTAP Pattern)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    println!("📋 Scenario: 3 logs with different attributes");
    println!("   Log 0: {{user.id: 123, user.name: 'alice'}}");
    println!("   Log 1: {{user.id: 456, request.method: 'GET'}}");
    println!("   Log 2: {{user.name: 'bob', response.status: 200}}");
    println!();

    // Create attribute table (star schema)
    let schema = Arc::new(Schema::new(vec![
        Field::new("parent_id", DataType::UInt16, false), // FK to logs table
        Field::new("key", DataType::Binary, false),
        Field::new("str_value", DataType::Binary, true),
        Field::new("int_value", DataType::Int32, true),
    ]));

    // Flattened attributes (one row per attribute)
    let attr_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt16Array::from(vec![0, 0, 1, 1, 2, 2])), // parent_id
            Arc::new(BinaryArray::from(vec![
                b"user.id".as_slice(),
                b"user.name".as_slice(),
                b"user.id".as_slice(),
                b"request.method".as_slice(),
                b"user.name".as_slice(),
                b"response.status".as_slice(),
            ])),
            Arc::new(BinaryArray::from(vec![
                None,
                Some(b"alice".as_slice()),
                None,
                Some(b"GET".as_slice()),
                Some(b"bob".as_slice()),
                None,
            ])),
            Arc::new(Int32Array::from(vec![
                Some(123),
                None,
                Some(456),
                None,
                None,
                Some(200),
            ])),
        ],
    )
    .unwrap();

    println!("📊 Attribute Table ({} rows):", attr_batch.num_rows());
    let parent_ids = attr_batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .unwrap();
    let keys = attr_batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();

    for i in 0..attr_batch.num_rows() {
        let parent = parent_ids.value(i);
        let key = String::from_utf8_lossy(keys.value(i));
        println!("   Row {}: parent_id={}, key={}", i, parent, key);
    }
    println!();

    // Operation 1: Find all attributes for log 1
    println!("🔍 Operation 1: Get all attributes for log 1");
    let filter = cmp::eq(parent_ids, &UInt16Array::new_scalar(1)).unwrap();
    let log1_attrs = compute::filter(&attr_batch.column(1), &filter).unwrap();
    let log1_keys = log1_attrs.as_any().downcast_ref::<BinaryArray>().unwrap();
    print!("   Keys: [");
    for i in 0..log1_keys.len() {
        print!("\"{}\"", String::from_utf8_lossy(log1_keys.value(i)));
        if i < log1_keys.len() - 1 {
            print!(", ");
        }
    }
    println!("]");
    println!("   ➜ Efficient lookup via parent_id FK");
    println!();

    // Operation 2: Rename attribute key
    println!("🔧 Operation 2: Rename 'user.name' → 'username'");
    println!("   Before: {:?}", keys.iter().map(|k| String::from_utf8_lossy(k.unwrap())).collect::<Vec<_>>());

    // In real code, you'd rebuild the array with new values
    println!("   After:  Replace matching entries in 'key' column");
    println!("   ➜ O(1) operation on columnar data (scan one column)");
    println!("   ➜ No need to parse/modify nested structures");
    println!();

    // Operation 3: Filter logs by attribute value
    println!("🔎 Operation 3: Find logs where user.id exists");
    let key_filter = keys
        .iter()
        .map(|k| k.map(|v| v == b"user.id").unwrap_or(false))
        .collect::<Vec<_>>();
    let matching_parents = parent_ids
        .iter()
        .zip(key_filter.iter())
        .filter(|(_, matches)| **matches)
        .map(|(parent, _)| parent.unwrap())
        .collect::<Vec<_>>();
    println!("   Matching log IDs: {:?}", matching_parents);
    println!("   ➜ Scan attribute table, return parent_ids");
    println!();
}

/// Demonstrate batching operations
fn demo_batching_operations() {
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📦 Batching Operations");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    println!("📝 Scenario: Merge multiple small RecordBatches into one");
    println!();

    // Create 3 small batches
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt16, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt16Array::from(vec![0, 1])),
            Arc::new(Int32Array::from(vec![10, 20])),
        ],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt16Array::from(vec![2, 3])),
            Arc::new(Int32Array::from(vec![30, 40])),
        ],
    )
    .unwrap();

    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt16Array::from(vec![4])),
            Arc::new(Int32Array::from(vec![50])),
        ],
    )
    .unwrap();

    println!("📊 Input Batches:");
    println!("   Batch 1: {} rows", batch1.num_rows());
    println!("   Batch 2: {} rows", batch2.num_rows());
    println!("   Batch 3: {} rows", batch3.num_rows());
    println!();

    // Concatenate batches
    println!("🔗 Concatenating batches...");
    let batches = vec![&batch1, &batch2, &batch3];
    let merged = arrow::compute::concat_batches(&schema, batches.iter().copied()).unwrap();

    println!("   Merged batch: {} rows", merged.num_rows());

    let ids = merged.column(0).as_any().downcast_ref::<UInt16Array>().unwrap();
    let values = merged
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    println!("   IDs:    {:?}", ids.values());
    println!("   Values: {:?}", values.values());
    println!();

    println!("💡 Why Batching Matters in OTAP:");
    println!("   • Reduces network overhead (fewer gRPC calls)");
    println!("   • Better compression (more data to compress)");
    println!("   • Amortizes per-request costs");
    println!("   • Enables efficient columnar operations");
    println!("   • Batch processor merges small requests → larger batches");
    println!();
}
