use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use log::{error, info, warn};
use tokio::sync::mpsc;
use tokio::time::sleep;
use uuid::Uuid;

use lw_config::chain_config::get_rpc_client;
use lw_config::config::get_config;
use lw_config::types::{IndexerCommand, ObservableContract};
use lw_domain::activity_model::Activity;
use lw_storage::op_repository::PgOperationStore;

use crate::event_source::{
    BackfillSource, EventSource, RpcEventSource, parse_ledger_range,
};
use crate::log_handlers::handle_event;

/// Which source raised a fetch error, and whether an unused fallback remains.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Attempt {
    /// Live RPC; no usable backfill endpoint is configured.
    LiveOnly,
    /// Live RPC; a backfill endpoint can still serve history below its floor.
    LiveWithBackfill,
    /// The backfill endpoint — the last resort for a gap.
    Backfill,
}

/// How the event loop must react to a failed fetch.
#[derive(PartialEq, Eq, Debug)]
enum Recovery {
    /// Cursor fell below the live-RPC floor; replay the gap from backfill.
    ReplayGap { floor: i32 },
    /// Cursor fell below the floor of the last source that could have served
    /// it: skip the gap to `floor` so the loop keeps moving.
    SkipGap { floor: i32 },
    /// Not a retention rejection; nothing to do but report it.
    Report,
}

/// Classify a fetch error at `cursor`. A below-retention rejection is permanent
/// for that request, so it MUST leave the cursor somewhere the next poll can
/// serve: only `LiveWithBackfill` holds the cursor, every other attempt skips.
/// Holding it against a source that already rejected it spins the loop forever.
fn classify_fetch_error(err: &str, cursor: i32, attempt: Attempt) -> Recovery {
    match parse_ledger_range(err) {
        Some((low, _)) if cursor < low => match attempt {
            Attempt::LiveWithBackfill => Recovery::ReplayGap { floor: low },
            Attempt::LiveOnly | Attempt::Backfill => {
                Recovery::SkipGap { floor: low }
            }
        },
        _ => Recovery::Report,
    }
}

/// Drive the indexer: poll the event source from the cursor ledger, decode each
/// event against its observed contract, and forward activities. A single task —
/// no per-chain fan-out. Re-subscribes when the command channel delivers an
/// updated contract set (dynamic OpLend discovery on `OperationCreated`).
pub async fn event_loop(
    mut cmd_rx: mpsc::Receiver<IndexerCommand>,
    tx_events: mpsc::Sender<Vec<Activity>>,
) {
    let client = match get_rpc_client() {
        Ok(c) => c,
        Err(e) => {
            error!("[event_loop] RPC client init failed: {e:?}");
            return;
        }
    };
    let source: Arc<dyn EventSource> = Arc::new(RpcEventSource::new(client));

    // History older than live-RPC retention is replayed from the backfill
    // endpoint. It MAY be the same URL as the live RPC — that endpoint still
    // serves a window the tail has scrolled past, and the span cap keeps the
    // replay chunked. Whether it reaches far enough for a given gap is decided
    // at runtime by its own out-of-range rejection, not by comparing URLs; an
    // unset URL is the only static "no backfill".
    let cfg = get_config();
    let backfill: Option<Arc<dyn EventSource>> =
        match cfg.backfill_source_url.as_str() {
            "" => None,
            url => match BackfillSource::new(url, cfg.backfill_max_span) {
                Ok(b) => Some(Arc::new(b) as Arc<dyn EventSource>),
                Err(e) => {
                    error!("[event_loop] backfill source init failed: {e:?}");
                    None
                }
            },
        };
    // Oldest ledger the live RPC still retains; learned from its out-of-range
    // rejection. `None` until we hit it. Cursors below this use the backfill
    // source until they catch up.
    let mut retention_floor: Option<i32> = None;

    let operations = PgOperationStore::from_global();
    let poll = Duration::from_millis(cfg.poll_interval_ms);

    let mut contracts: HashMap<String, ObservableContract> = HashMap::new();
    let mut fopid_to_opid: HashMap<i32, Uuid> = HashMap::new();
    let mut cursor: i32 = get_config().start_ledger;

    loop {
        // Drain pending contract-set updates without blocking the poll.
        while let Ok(cmd) = cmd_rx.try_recv() {
            let IndexerCommand::UpdateContracts(list, map) = cmd;
            for c in list {
                cursor = cursor
                    .max(c.latest_block + 1)
                    .max(get_config().start_ledger);
                contracts.insert(c.address.clone(), c);
            }
            fopid_to_opid = map;
            info!(
                "[event_loop] tracking {} contracts from ledger {cursor}",
                contracts.len()
            );
        }

        if contracts.is_empty() {
            sleep(poll).await;
            continue;
        }

        let ids: Vec<String> = contracts.keys().cloned().collect();
        // Below the known retention floor, replay from the backfill endpoint;
        // otherwise tail the live RPC.
        let (active, attempt) = match (retention_floor, &backfill) {
            (Some(floor), Some(b)) if cursor < floor => (b, Attempt::Backfill),
            (_, Some(_)) => (&source, Attempt::LiveWithBackfill),
            (_, None) => (&source, Attempt::LiveOnly),
        };
        match active.fetch(cursor, &ids).await {
            Ok((events, next_cursor)) => {
                for raw in &events {
                    let Some(contract) = contracts.get(&raw.contract_id) else {
                        continue;
                    };
                    if let Ok(Some(activities)) =
                        handle_event(contract, &fopid_to_opid, raw, &operations)
                            .await
                        && tx_events.send(activities).await.is_err()
                    {
                        error!("[event_loop] event channel closed; stopping");
                        return;
                    }
                }
                cursor = next_cursor;
            }
            Err(e) => {
                match classify_fetch_error(&e.to_string(), cursor, attempt) {
                    Recovery::ReplayGap { floor } => {
                        retention_floor = Some(floor);
                        warn!(
                            "[event_loop] cursor {cursor} below RPC retention \
                             floor {floor}; backfilling gap"
                        );
                    }
                    Recovery::SkipGap { floor } => {
                        if attempt == Attempt::Backfill {
                            error!(
                                "[event_loop] backfill source retains only \
                                 from {floor}; ledgers {cursor}-{} are \
                                 unreachable and will not be indexed",
                                floor - 1
                            );
                        } else {
                            retention_floor = Some(floor);
                            warn!(
                                "[event_loop] cursor {cursor} below RPC \
                                 retention floor {floor} and no backfill \
                                 source configured; skipping gap to {floor}"
                            );
                        }
                        cursor = floor;
                    }
                    Recovery::Report => error!(
                        "[event_loop] fetch error at ledger {cursor}: {e:?}"
                    ),
                }
            }
        }

        sleep(poll).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rejection observed in production at cursor 63888609.
    const BELOW_FLOOR: &str = "getEvents failed: ErrorObject { code: \
                               InvalidRequest, message: \"startLedger must be \
                               within the ledger range: 64010315 - 64131020\", \
                               data: None }";

    #[test]
    fn holds_cursor_only_while_backfill_is_still_untried() {
        assert_eq!(
            classify_fetch_error(
                BELOW_FLOOR,
                63_888_609,
                Attempt::LiveWithBackfill
            ),
            Recovery::ReplayGap { floor: 64_010_315 }
        );
    }

    #[test]
    fn skips_gap_when_the_backfill_source_cannot_serve_it_either() {
        // Production loop: the backfill endpoint repeated the live RPC's
        // out-of-range rejection, so holding the cursor re-issued the same
        // doomed request every poll. Nothing else can serve the gap: advance.
        assert_eq!(
            classify_fetch_error(BELOW_FLOOR, 63_888_609, Attempt::Backfill),
            Recovery::SkipGap { floor: 64_010_315 }
        );
    }

    #[test]
    fn skips_gap_when_no_backfill_is_configured() {
        assert_eq!(
            classify_fetch_error(BELOW_FLOOR, 63_888_609, Attempt::LiveOnly),
            Recovery::SkipGap { floor: 64_010_315 }
        );
    }

    #[test]
    fn shared_endpoint_backfill_settles_after_one_probe() {
        // BACKFILL_SOURCE_URL == SOROBAN_RPC_URL. The replay attempt is still
        // worth making — the endpoint retains a window the tail scrolled past —
        // but a gap outrunning that window must settle in one probe, not spin.
        let mut cursor = 63_888_609;
        assert_eq!(
            classify_fetch_error(
                BELOW_FLOOR,
                cursor,
                Attempt::LiveWithBackfill
            ),
            Recovery::ReplayGap { floor: 64_010_315 }
        );
        // Next poll hits the same endpoint and gets the same rejection.
        let Recovery::SkipGap { floor } =
            classify_fetch_error(BELOW_FLOOR, cursor, Attempt::Backfill)
        else {
            panic!("a rejected backfill must advance the cursor");
        };
        cursor = floor;
        // Cursor is back inside the window: the gap no longer re-triggers.
        assert_eq!(
            classify_fetch_error(
                BELOW_FLOOR,
                cursor,
                Attempt::LiveWithBackfill
            ),
            Recovery::Report
        );
    }

    #[test]
    fn leaves_non_retention_rejections_to_the_error_log() {
        // Cursor inside/above the advertised range is not a retention gap:
        // above-tip holds are resolved inside the event source.
        assert_eq!(
            classify_fetch_error(BELOW_FLOOR, 64_131_021, Attempt::Backfill),
            Recovery::Report
        );
        assert_eq!(
            classify_fetch_error(
                "connection reset",
                63_888_609,
                Attempt::Backfill
            ),
            Recovery::Report
        );
    }
}
