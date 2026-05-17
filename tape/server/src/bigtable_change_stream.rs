//! Bigtable change-stream wake-ups for the matcher.
//!
//! Design (design-principles/tape-event-bus.md §6.3): the matcher polls the
//! `jg#` journal index every ~1 s by default; this module pushes a wake-up via
//! `tokio::sync::Notify` whenever a DataChange lands on a `jg#` row, so the
//! matcher drains the new batch in milliseconds instead of waiting for the
//! next poll tick.
//!
//! Best-effort: change-streams are an opt-in table feature. Enable with::
//!
//!   cbt -project P -instance I updatetable tape changeStreamRetention=1d
//!
//! If the table doesn't have a change-stream configured (or the backend is the
//! Bigtable emulator, which does not implement `ReadChangeStream` in any
//! published release), `start` logs once and returns — the matcher continues
//! polling. Either way, correctness is preserved; only latency differs.
//!
//! Implementation notes:
//! * We discover partitions via `GenerateInitialChangeStreamPartitions` and
//!   open one `ReadChangeStream` RPC per partition. When a `CloseStream` arrives
//!   (a partition split or merge), we simply restart the discovery loop —
//!   tracking continuation tokens for surgical resumption isn't worth the
//!   complexity at this layer since we only use the stream as a wake-up.
//! * We don't decode the mutation payload; the matcher reads the journal rows
//!   itself once it wakes. The change-stream is purely a *pulse*.
//! * If the `read_change_stream` RPC errors (PERMISSION_DENIED, UNIMPLEMENTED,
//!   FAILED_PRECONDITION = "change stream not enabled"), we exit the partition
//!   loop and log once. The polling path keeps the system live.

use std::sync::Arc;
use std::time::Duration;

use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    read_change_stream_response::StreamRecord, GenerateInitialChangeStreamPartitionsRequest,
    ReadChangeStreamRequest, StreamPartition,
};

use crate::store::bigtable::BigtableRunStore;

const JOURNAL_GS_PREFIX: &[u8] = b"jg#";

/// Try to start the change-stream wake-up loop. Returns immediately after
/// spawning the background task. On any setup error (RPC failure, no partitions,
/// permission denied, emulator without change-stream support), logs a single
/// `warn` and returns — the caller's polling path stays intact.
///
/// If `BIGTABLE_EMULATOR_HOST` is set, we skip the watcher entirely: the
/// in-tree `cbtemulator` (cloud.google.com/go/bigtable v1.47 and earlier)
/// crashes with a nil-pointer panic when `GenerateInitialChangeStreamPartitions`
/// is called against it, taking the emulator process down with it. Polling
/// against the emulator is the documented fallback.
pub fn spawn(store: Arc<BigtableRunStore>, notify: Arc<tokio::sync::Notify>) {
    if std::env::var("BIGTABLE_EMULATOR_HOST").is_ok() {
        tracing::info!(
            "bigtable change-stream: emulator detected (BIGTABLE_EMULATOR_HOST set); \
             skipping the watcher — the cbtemulator panics on \
             GenerateInitialChangeStreamPartitions. Matcher falls back to polling."
        );
        return;
    }
    tokio::spawn(async move {
        if let Err(err) = run(store, notify).await {
            tracing::warn!(
                %err,
                "bigtable change-stream wake-up unavailable; matcher will poll. \
                 Enable with `cbt updatetable tape changeStreamRetention=1d` on a real \
                 Bigtable instance."
            );
        }
    });
}

async fn run(store: Arc<BigtableRunStore>, notify: Arc<tokio::sync::Notify>) -> Result<(), String> {
    // Outer loop: re-discover partitions on CloseStream / on partition rebalance.
    loop {
        let partitions = match list_partitions(&store).await {
            Ok(p) => p,
            Err(err) => return Err(err),
        };
        if partitions.is_empty() {
            return Err("GenerateInitialChangeStreamPartitions returned no partitions".into());
        }
        tracing::info!(count = partitions.len(), "bigtable change-stream: watching partitions");

        // One task per partition; each pulses `notify` on every DataChange that
        // hits a jg# row. We join on the first one to exit (rebalance) and then
        // restart discovery.
        let handles: Vec<_> = partitions
            .into_iter()
            .map(|p| {
                let store = store.clone();
                let notify = notify.clone();
                tokio::spawn(async move { watch_partition(store, p, notify).await })
            })
            .collect();

        // If any partition errors / closes, abort the rest and re-discover.
        let mut early_exit = false;
        for h in handles {
            match h.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    tracing::warn!(%err, "bigtable change-stream: partition watcher exited");
                    early_exit = true;
                }
                Err(err) => {
                    tracing::warn!(%err, "bigtable change-stream: partition watcher panicked");
                    early_exit = true;
                }
            }
            if early_exit { break; }
        }
        // Backoff before re-discovery to avoid tight crash loops on a broken stream.
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn list_partitions(store: &BigtableRunStore) -> Result<Vec<StreamPartition>, String> {
    let mut bt = store.bt();
    let req = GenerateInitialChangeStreamPartitionsRequest {
        table_name: store.table_name().to_string(),
        ..Default::default()
    };
    let resp = bt
        .get_client()
        .generate_initial_change_stream_partitions(req)
        .await
        .map_err(|e| format!("GenerateInitialChangeStreamPartitions: {e}"))?;
    let mut stream = resp.into_inner();
    let mut out = Vec::new();
    while let Some(msg) = stream
        .message()
        .await
        .map_err(|e| format!("partition stream recv: {e}"))?
    {
        if let Some(p) = msg.partition {
            out.push(p);
        }
    }
    Ok(out)
}

async fn watch_partition(
    store: Arc<BigtableRunStore>,
    partition: StreamPartition,
    notify: Arc<tokio::sync::Notify>,
) -> Result<(), String> {
    let mut bt = store.bt();
    let req = ReadChangeStreamRequest {
        table_name: store.table_name().to_string(),
        partition: Some(partition),
        ..Default::default()
    };
    let mut stream = bt
        .get_client()
        .read_change_stream(req)
        .await
        .map_err(|e| format!("ReadChangeStream: {e}"))?
        .into_inner();

    while let Some(msg) = stream
        .message()
        .await
        .map_err(|e| format!("change-stream recv: {e}"))?
    {
        match msg.stream_record {
            Some(StreamRecord::DataChange(dc)) => {
                if dc.row_key.starts_with(JOURNAL_GS_PREFIX) {
                    notify.notify_waiters();
                }
            }
            Some(StreamRecord::Heartbeat(_)) => {
                // Just a keepalive; nothing to do.
            }
            Some(StreamRecord::CloseStream(_)) => {
                // Partition rebalance — exit so the outer loop re-discovers.
                return Ok(());
            }
            None => {}
        }
    }
    Ok(())
}
