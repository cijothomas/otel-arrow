// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use bytes::Bytes;
use flate2::Compression;
use flate2::write::GzEncoder;
use std::io::Write;

use super::error::Error;

const ONE_MB: usize = 1024 * 1024; // 1 MB
const GZIP_SAFETY_MARGIN: usize = 30; // Safety margin in bytes

/// A single gzip-compressed JSON array payload ready to send.
pub struct CompressedChunk {
    /// The gzip-compressed bytes (a complete JSON array: `[{...},{...}]`).
    pub compressed_data: Bytes,
    /// Number of log records in this chunk.
    pub row_count: f64,
}

/// Compress `records` into one or more gzip-compressed JSON array payloads,
/// each with a compressed size ≤ 1MB.
///
/// Records are pre-serialized JSON objects (e.g. `{"field":"value"}`). This
/// function wraps them in a JSON array and gzip-compresses the result.
///
/// If a single record exceeds the per-chunk limit even on its own, returns
/// `Err(Error::LogEntryTooLarge)` immediately without producing any output.
///
/// Splitting is rare in practice: it only occurs when a batch of records
/// compresses to more than 1MB, which requires a very large number of
/// records or individually large records. The upstream batch processor is
/// expected to bound batch sizes so that splitting is an infrequent
/// safety-net rather than the common case.
pub fn compress_and_split(records: &[Bytes]) -> Result<Vec<CompressedChunk>, Error> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    let mut chunks: Vec<CompressedChunk> = Vec::new();

    // State for the chunk currently being built.
    let mut encoder = new_encoder();
    let mut first_in_chunk = true;
    let mut row_count: f64 = 0.0;
    // Uncompressed bytes written to the encoder since the last flush (or
    // since the chunk started). Used to cheaply estimate whether a flush
    // is needed before adding the next record.
    let mut uncompressed_size: usize = 0;
    // Estimated remaining compressed capacity in this chunk. Updated after
    // each flush by measuring the actual compressed output so far.
    let mut remaining_size: usize = ONE_MB - GZIP_SAFETY_MARGIN;

    // Write JSON array opening bracket.
    encoder.write_all(b"[").map_err(Error::BatchPushFailed)?;

    for record in records {
        // Reject a single record that could never fit alone in 1MB compressed.
        if record.len() > ONE_MB - GZIP_SAFETY_MARGIN {
            return Err(Error::LogEntryTooLarge);
        }

        let separator_len: usize = usize::from(!first_in_chunk);
        let candidate_size = uncompressed_size + separator_len + record.len();

        if candidate_size > remaining_size {
            // Flush the encoder to measure the actual compressed size and
            // determine whether this record still fits.
            encoder.flush().map_err(Error::BatchPushFailed)?;
            let actual_compressed = encoder.get_ref().len();
            remaining_size = ONE_MB.saturating_sub(actual_compressed + GZIP_SAFETY_MARGIN);
            uncompressed_size = 0;

            if separator_len + record.len() > remaining_size {
                // This record won't fit in the current chunk. Finalize it
                // and start a fresh chunk.
                encoder
                    .write_all(b"]")
                    .map_err(Error::BatchFinalizeFailed)?;
                let finished = encoder.finish().map_err(Error::BatchFinalizeFailed)?;
                chunks.push(CompressedChunk {
                    compressed_data: Bytes::from(finished),
                    row_count,
                });

                encoder = new_encoder();
                encoder.write_all(b"[").map_err(Error::BatchPushFailed)?;
                first_in_chunk = true;
                row_count = 0.0;
                uncompressed_size = 0;
                remaining_size = ONE_MB - GZIP_SAFETY_MARGIN;
            }
        }

        // Write the record into the current chunk.
        if !first_in_chunk {
            encoder.write_all(b",").map_err(Error::BatchPushFailed)?;
            uncompressed_size += 1;
        }
        encoder.write_all(record).map_err(Error::BatchPushFailed)?;
        uncompressed_size += record.len();
        row_count += 1.0;
        first_in_chunk = false;
    }

    // Finalize the last (or only) chunk.
    encoder
        .write_all(b"]")
        .map_err(Error::BatchFinalizeFailed)?;
    let finished = encoder.finish().map_err(Error::BatchFinalizeFailed)?;
    chunks.push(CompressedChunk {
        compressed_data: Bytes::from(finished),
        row_count,
    });

    Ok(chunks)
}

fn new_encoder() -> GzEncoder<Vec<u8>> {
    GzEncoder::new(Vec::with_capacity(ONE_MB), Compression::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use rand::RngExt;
    use std::io::Read;

    // ==================== Test Helpers ====================

    fn generate_data(size: usize) -> Bytes {
        let mut rng = rand::rng();
        let id = rng.random_range(10000..99999);
        let timestamp = rng.random_range(1600000000..1700000000);

        let base = format!(r#"{{"id":{},"ts":{},"msg":""#, id, timestamp);
        let closing = r#""}"#;

        let padding_needed = size.saturating_sub(base.len() + closing.len());
        let padding: String = (0..padding_needed)
            .map(|_| rng.random_range(b'a'..=b'z') as char)
            .collect();

        Bytes::from(format!("{}{}{}", base, padding, closing))
    }

    fn generate_1kb_data() -> Bytes {
        generate_data(1024)
    }

    fn decompress_and_validate(data: &Bytes) -> String {
        let mut decoder = GzDecoder::new(&data[..]);
        let mut decompressed = String::new();
        let _ = decoder
            .read_to_string(&mut decompressed)
            .expect("Should decompress");

        let trimmed = decompressed.trim();
        assert!(trimmed.starts_with('['), "Should start with [");
        assert!(trimmed.ends_with(']'), "Should end with ]");

        // Remove all whitespace to check for structural issues like [, or ,]
        let no_whitespace: String = decompressed
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();

        assert!(
            !no_whitespace.contains("[,") && !no_whitespace.contains(",]"),
            "Invalid comma placement found in JSON: {}",
            decompressed
        );

        decompressed
    }

    // ==================== Basic Functionality Tests ====================

    #[test]
    fn test_empty_input_returns_empty() {
        let result = compress_and_split(&[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_single_record_produces_one_chunk() {
        let records = vec![generate_1kb_data()];
        let chunks = compress_and_split(&records).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].row_count, 1.0);
    }

    #[test]
    fn test_multiple_records_fit_in_one_chunk() {
        let records: Vec<Bytes> = (0..10).map(|_| generate_1kb_data()).collect();
        let chunks = compress_and_split(&records).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].row_count, 10.0);
    }

    // ==================== Output Format Tests ====================

    #[test]
    fn test_output_is_valid_gzip_json_array() {
        let records: Vec<Bytes> = (0..10).map(|_| generate_1kb_data()).collect();
        let chunks = compress_and_split(&records).unwrap();
        assert_eq!(chunks.len(), 1);

        let decompressed = decompress_and_validate(&chunks[0].compressed_data);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&decompressed).unwrap();
        assert_eq!(parsed.len(), 10);
    }

    #[test]
    fn test_no_leading_comma_after_bracket() {
        let records = vec![Bytes::from_static(b"1"), Bytes::from_static(b"2")];
        let chunks = compress_and_split(&records).unwrap();
        let json = decompress_and_validate(&chunks[0].compressed_data);
        assert_eq!(json, "[1,2]");
    }

    #[test]
    fn test_no_trailing_comma_before_bracket() {
        let records = vec![Bytes::from_static(b"1")];
        let chunks = compress_and_split(&records).unwrap();
        let json = decompress_and_validate(&chunks[0].compressed_data);
        assert_eq!(json, "[1]");
    }

    #[test]
    fn test_row_count_accuracy() {
        let records: Vec<Bytes> = (0..42).map(|_| generate_1kb_data()).collect();
        let chunks = compress_and_split(&records).unwrap();
        let total_rows: f64 = chunks.iter().map(|c| c.row_count).sum();
        assert_eq!(total_rows, 42.0);
    }

    // ==================== Error Case Tests ====================

    #[test]
    fn test_too_large_entry_returns_error() {
        let large = Bytes::from(vec![b'x'; ONE_MB]);
        let result = compress_and_split(&[large]);
        assert!(
            matches!(result, Err(Error::LogEntryTooLarge)),
            "Expected LogEntryTooLarge"
        );
    }

    #[test]
    fn test_just_under_limit_does_not_error() {
        // Exactly at the limit (not >) should not trigger LogEntryTooLarge.
        let at_limit = Bytes::from(vec![b'x'; ONE_MB - GZIP_SAFETY_MARGIN]);
        let result = compress_and_split(&[at_limit]);
        assert!(result.is_ok(), "Should not return error at limit");
    }

    #[test]
    fn test_too_large_in_middle_errors_immediately() {
        // Even if valid records come first, a too-large record causes an error.
        let mut records: Vec<Bytes> = (0..5).map(|_| generate_1kb_data()).collect();
        records.push(Bytes::from(vec![b'x'; ONE_MB]));
        let result = compress_and_split(&records);
        assert!(matches!(result, Err(Error::LogEntryTooLarge)));
    }

    // ==================== Size Limit / Splitting Tests ====================

    #[test]
    fn test_chunks_do_not_exceed_1mb_compressed() {
        // Use random (incompressible) bytes in small chunks to fill up to the limit.
        let mut rng = rand::rng();
        let records: Vec<Bytes> = (0..5000)
            .map(|_| {
                let chunk: Vec<u8> = (0..200).map(|_| rng.random()).collect();
                Bytes::from(chunk)
            })
            .collect();

        let chunks = compress_and_split(&records).unwrap();
        for (i, chunk) in chunks.iter().enumerate() {
            assert!(
                chunk.compressed_data.len() <= ONE_MB,
                "Chunk {} compressed size {} exceeds 1MB",
                i,
                chunk.compressed_data.len()
            );
        }
    }

    #[test]
    fn test_split_produces_valid_json_in_each_chunk() {
        // Random bytes force high compressed size → triggers splitting.
        // 8000 records × 201 bytes (200 data + 1 comma separator) = ~1.6 MB uncompressed,
        // well above ONE_MB (1,048,576), guaranteeing at least one split.
        let mut rng = rand::rng();
        let records: Vec<Bytes> = (0..8000)
            .map(|_| {
                let chunk: Vec<u8> = (0..200).map(|_| rng.random()).collect();
                Bytes::from(chunk)
            })
            .collect();

        let chunks = compress_and_split(&records).unwrap();
        // Ensure we actually got a split (otherwise this test doesn't cover the
        // splitting path).
        assert!(
            chunks.len() > 1,
            "Expected at least one split for 8000 incompressible records"
        );

        for chunk in &chunks {
            // Decompress to raw bytes (records are random binary, not valid UTF-8).
            let mut decoder = GzDecoder::new(&chunk.compressed_data[..]);
            let mut decompressed: Vec<u8> = Vec::new();
            let _ = decoder
                .read_to_end(&mut decompressed)
                .expect("Should decompress");
            // Each chunk must be a JSON array: starts with '[', ends with ']'.
            assert_eq!(decompressed.first(), Some(&b'['), "Should start with [");
            assert_eq!(decompressed.last(), Some(&b']'), "Should end with ]");
            assert!(chunk.row_count > 0.0, "Each chunk should have a nonzero row count");
        }
    }

    #[test]
    fn test_row_counts_across_splits_sum_to_total() {
        // 5000 records × 301 bytes (300 data + 1 separator) = ~1.5 MB uncompressed,
        // above ONE_MB, so this exercises the splitting path while verifying counts.
        let mut rng = rand::rng();
        let n = 5000usize;
        let records: Vec<Bytes> = (0..n)
            .map(|_| {
                let chunk: Vec<u8> = (0..300).map(|_| rng.random()).collect();
                Bytes::from(chunk)
            })
            .collect();

        let chunks = compress_and_split(&records).unwrap();
        let total: f64 = chunks.iter().map(|c| c.row_count).sum();
        assert_eq!(total, n as f64);
    }

    #[test]
    fn test_all_records_present_after_split() {
        // Use distinguishable records (simple integers) to verify none are dropped.
        let records: Vec<Bytes> = (0..20)
            .map(|i| Bytes::from(format!("{i}")))
            .collect();
        let chunks = compress_and_split(&records).unwrap();

        let mut all_values: Vec<u64> = Vec::new();
        for chunk in &chunks {
            let mut decoder = GzDecoder::new(&chunk.compressed_data[..]);
            let mut decompressed = String::new();
            let _ = decoder.read_to_string(&mut decompressed).unwrap();
            let parsed: Vec<u64> = serde_json::from_str(&decompressed).unwrap();
            all_values.extend(parsed);
        }

        let expected: Vec<u64> = (0..20).collect();
        assert_eq!(all_values, expected);
    }
}
