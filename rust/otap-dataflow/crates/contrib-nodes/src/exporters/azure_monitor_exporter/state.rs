// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use ahash::AHashMap as HashMap;
use otap_df_otap::pdata::Context;
use otap_df_pdata::OtapPayload;

/// Data retained for a single incoming pipeline message while its HTTP chunk(s)
/// are in flight.
pub struct PendingMessage {
    /// Context needed to ack or nack the message.
    pub context: Context,
    /// Payload to return with the ack/nack.
    pub payload: OtapPayload,
    /// Number of gzip-compressed HTTP chunks that must complete successfully
    /// before this message can be acked. Decremented on each successful chunk
    /// completion; when it reaches zero the message is acked.
    pub pending_chunks: u32,
}

/// Tracks in-flight export state for the Azure Monitor exporter.
///
/// With an upstream batch processor, each incoming pipeline message maps to
/// exactly N compressed HTTP chunks (N ≥ 1). We track a simple counter per
/// message:
///
/// - All N chunks succeed → `on_chunk_success` decrements to 0 → ack.
/// - Any chunk fails → `on_chunk_failure` removes the entry → nack.
///   Subsequent completions for the same message return `None` (no double-ack
///   or double-nack).
pub struct AzureMonitorExporterState {
    pending: HashMap<u64, PendingMessage>,
}

impl AzureMonitorExporterState {
    /// Create state with a small initial capacity that grows on demand.
    pub fn new() -> Self {
        Self {
            pending: HashMap::with_capacity(64),
        }
    }

    /// Register a new incoming message and the number of compressed chunks
    /// that will be dispatched for it.
    pub fn register(
        &mut self,
        msg_id: u64,
        context: Context,
        payload: OtapPayload,
        chunk_count: u32,
    ) {
        let _ = self.pending.insert(
            msg_id,
            PendingMessage {
                context,
                payload,
                pending_chunks: chunk_count,
            },
        );
    }

    /// Called when one HTTP chunk for `msg_id` completes successfully.
    ///
    /// Decrements the pending-chunk counter. Returns `Some((context, payload))`
    /// when the counter reaches zero (all chunks done → ready to ack).
    /// Returns `None` if the message was already nacked (and removed) by a
    /// previously failed chunk.
    pub fn on_chunk_success(&mut self, msg_id: u64) -> Option<(Context, OtapPayload)> {
        let entry = self.pending.get_mut(&msg_id)?;
        entry.pending_chunks = entry.pending_chunks.saturating_sub(1);
        if entry.pending_chunks == 0 {
            let msg = self.pending.remove(&msg_id)?;
            Some((msg.context, msg.payload))
        } else {
            None
        }
    }

    /// Called when one HTTP chunk for `msg_id` fails.
    ///
    /// Removes the entire entry immediately and returns `Some((context, payload))`
    /// for nacking. Returns `None` if the message was already removed (e.g. by a
    /// previous failed chunk), preventing double-nacks.
    pub fn on_chunk_failure(&mut self, msg_id: u64) -> Option<(Context, OtapPayload)> {
        self.pending
            .remove(&msg_id)
            .map(|msg| (msg.context, msg.payload))
    }

    /// Returns the number of messages currently tracked.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Returns true if no messages are currently tracked.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Drain all remaining messages (for shutdown cleanup).
    pub fn drain_all(&mut self) -> Vec<(u64, Context, OtapPayload)> {
        self.pending
            .drain()
            .map(|(msg_id, msg)| (msg_id, msg.context, msg.payload))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use otap_df_otap::pdata::Context;
    use otap_df_pdata::otlp::OtlpProtoBytes;

    fn test_payload(data: &'static [u8]) -> OtapPayload {
        OtapPayload::OtlpBytes(OtlpProtoBytes::ExportLogsRequest(Bytes::from_static(data)))
    }

    fn empty_payload() -> OtapPayload {
        OtapPayload::empty(otap_df_config::SignalType::Logs)
    }

    // ==================== Construction Tests ====================

    #[test]
    fn test_new_is_empty() {
        let state = AzureMonitorExporterState::new();
        assert!(state.is_empty());
        assert_eq!(state.len(), 0);
    }

    // ==================== register Tests ====================

    #[test]
    fn test_register_single_chunk() {
        let mut state = AzureMonitorExporterState::new();
        state.register(1, Context::default(), test_payload(b"a"), 1);
        assert_eq!(state.len(), 1);
    }

    #[test]
    fn test_register_multiple_chunks() {
        let mut state = AzureMonitorExporterState::new();
        state.register(1, Context::default(), test_payload(b"a"), 3);
        assert_eq!(state.len(), 1);
    }

    // ==================== on_chunk_success Tests ====================

    #[test]
    fn test_single_chunk_success_acks() {
        let mut state = AzureMonitorExporterState::new();
        state.register(1, Context::default(), test_payload(b"data"), 1);

        let result = state.on_chunk_success(1);
        assert!(result.is_some());
        assert!(state.is_empty());
    }

    #[test]
    fn test_multi_chunk_success_acks_only_on_last() {
        let mut state = AzureMonitorExporterState::new();
        state.register(1, Context::default(), test_payload(b"data"), 3);

        // First two chunks: no ack yet
        assert!(state.on_chunk_success(1).is_none());
        assert_eq!(state.len(), 1); // still tracked
        assert!(state.on_chunk_success(1).is_none());
        assert_eq!(state.len(), 1);

        // Third chunk: ack
        let result = state.on_chunk_success(1);
        assert!(result.is_some());
        assert!(state.is_empty());
    }

    #[test]
    fn test_success_on_unknown_msg_returns_none() {
        let mut state = AzureMonitorExporterState::new();
        assert!(state.on_chunk_success(999).is_none());
    }

    #[test]
    fn test_success_after_failure_returns_none() {
        let mut state = AzureMonitorExporterState::new();
        state.register(1, Context::default(), test_payload(b"data"), 2);

        // First chunk fails → message removed → nack
        assert!(state.on_chunk_failure(1).is_some());

        // Second chunk succeeds, but message is already gone → no double-ack
        assert!(state.on_chunk_success(1).is_none());
    }

    // ==================== on_chunk_failure Tests ====================

    #[test]
    fn test_failure_nacks_immediately() {
        let mut state = AzureMonitorExporterState::new();
        state.register(1, Context::default(), test_payload(b"data"), 3);

        let result = state.on_chunk_failure(1);
        assert!(result.is_some());
        assert!(state.is_empty()); // removed entirely
    }

    #[test]
    fn test_failure_on_unknown_msg_returns_none() {
        let mut state = AzureMonitorExporterState::new();
        assert!(state.on_chunk_failure(999).is_none());
    }

    #[test]
    fn test_double_failure_returns_none_second_time() {
        let mut state = AzureMonitorExporterState::new();
        state.register(1, Context::default(), test_payload(b"data"), 2);

        // First failure: nack
        assert!(state.on_chunk_failure(1).is_some());
        // Second failure: already removed → no double-nack
        assert!(state.on_chunk_failure(1).is_none());
    }

    #[test]
    fn test_failure_after_partial_success_returns_nack() {
        let mut state = AzureMonitorExporterState::new();
        state.register(1, Context::default(), test_payload(b"data"), 3);

        // Chunk 1 succeeds (counter: 3 → 2)
        assert!(state.on_chunk_success(1).is_none());

        // Chunk 2 fails → nack entire message
        let result = state.on_chunk_failure(1);
        assert!(result.is_some());
        assert!(state.is_empty());

        // Chunk 3 arrives after message is gone → no double-nack
        assert!(state.on_chunk_success(1).is_none());
    }

    // ==================== Multiple Messages Tests ====================

    #[test]
    fn test_independent_message_tracking() {
        let mut state = AzureMonitorExporterState::new();
        state.register(1, Context::default(), test_payload(b"msg1"), 2);
        state.register(2, Context::default(), test_payload(b"msg2"), 1);

        // Message 2 completes first
        assert!(state.on_chunk_success(2).is_some());
        assert_eq!(state.len(), 1); // msg1 still pending

        // Message 1 chunk 1 succeeds (counter: 2 → 1)
        assert!(state.on_chunk_success(1).is_none());

        // Message 1 chunk 2 succeeds → ack
        assert!(state.on_chunk_success(1).is_some());
        assert!(state.is_empty());
    }

    #[test]
    fn test_failure_of_one_does_not_affect_other() {
        let mut state = AzureMonitorExporterState::new();
        state.register(1, Context::default(), test_payload(b"msg1"), 1);
        state.register(2, Context::default(), test_payload(b"msg2"), 1);

        // Message 1 fails
        assert!(state.on_chunk_failure(1).is_some());
        assert_eq!(state.len(), 1); // msg2 still tracked

        // Message 2 succeeds independently
        assert!(state.on_chunk_success(2).is_some());
        assert!(state.is_empty());
    }

    // ==================== drain_all Tests ====================

    #[test]
    fn test_drain_all_empty() {
        let mut state = AzureMonitorExporterState::new();
        let drained = state.drain_all();
        assert!(drained.is_empty());
    }

    #[test]
    fn test_drain_all_returns_all_messages() {
        let mut state = AzureMonitorExporterState::new();
        state.register(1, Context::default(), test_payload(b"1"), 1);
        state.register(2, Context::default(), empty_payload(), 2);

        let drained = state.drain_all();
        assert_eq!(drained.len(), 2);

        let ids: std::collections::HashSet<u64> = drained.iter().map(|(id, _, _)| *id).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
        assert!(state.is_empty());
    }

    #[test]
    fn test_drain_all_clears_state() {
        let mut state = AzureMonitorExporterState::new();
        state.register(1, Context::default(), test_payload(b"1"), 3);

        let _ = state.drain_all();
        assert!(state.is_empty());
        // Operations after drain return None
        assert!(state.on_chunk_success(1).is_none());
        assert!(state.on_chunk_failure(1).is_none());
    }
}
