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
use crate::hubble::HubbleSource;
use crate::log_handlers::handle_event;

/// Which source raised a fetch error, and what can still be done about it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Attempt {
    /// Live RPC; no replay configured, so a gap below retention is lost.
    LiveOnly,
    /// Live RPC; a replay source can cover anything below the floor.
    LiveWithBackfill,
    /// RPC-backed replay. Bounded by the endpoint's own retention, so an
    /// out-of-range rejection means the gap is unreachable from any source.
    BackfillBounded,
    /// Hubble-backed replay. Holds history from genesis, so a failure is
    /// transport or query trouble, never an unreachable gap.
    BackfillArchival,
}

/// How the event loop must react to a failed fetch.
#[derive(PartialEq, Eq, Debug)]
enum Recovery {
    /// Cursor fell below the live-RPC floor; replay the gap.
    ReplayGap { floor: i32 },
    /// Cursor fell below the floor and nothing can replay it: skip to `floor`
    /// so the loop keeps moving, accepting the loss.
    SkipGap { floor: i32 },
    /// Report and hold the cursor.
    Report,
}

/// Classify a fetch error at `cursor`.
///
/// A below-retention rejection is permanent for that request, so it must never
/// leave the cursor pointed at a source that just refused it — that spins the
/// loop forever, which is exactly how the original incident presented.
///
/// The two replay sources fail differently and must not be conflated: an
/// RPC replay can genuinely run out of history, so its rejection means skip;
/// Hubble cannot, so its failure means hold and report, because skipping would
/// discard ledgers that are still recoverable.
fn classify_fetch_error(err: &str, cursor: i32, attempt: Attempt) -> Recovery {
    if attempt == Attempt::BackfillArchival {
        return Recovery::Report;
    }
    match parse_ledger_range(err) {
        Some((low, _)) if cursor < low => match attempt {
            Attempt::LiveWithBackfill => Recovery::ReplayGap { floor: low },
            _ => Recovery::SkipGap { floor: low },
        },
        _ => Recovery::Report,
    }
}

/// A configured replay source plus the failure semantics it demands.
pub(crate) struct Replay {
    source: Arc<dyn EventSource>,
    attempt: Attempt,
}

/// Build the replay source: Hubble when it is configured, reachable and proven
/// to hold the same network; otherwise the RPC replay; otherwise nothing.
///
/// `network_tip` is the live RPC's latest ledger. `None` means it could not be
/// read, and Hubble is then refused rather than trusted: SDF publishes Hubble
/// for pubnet only, and an unverified dataset would answer testnet queries with
/// unrelated pubnet ledgers and silently advance the cursor over them.
pub(crate) async fn build_replay(
    cfg: &lw_config::config::LocalEnv,
    network_tip: Option<i32>,
) -> Option<Replay> {
    match (cfg.hubble_billing_project.as_str(), network_tip) {
        ("", _) => {}
        (_, None) => warn!(
            "[event_loop] cannot read the RPC tip, so Hubble's network cannot \
             be confirmed; falling back to RPC replay"
        ),
        (project, Some(tip)) => {
            match HubbleSource::connect(
                project,
                &cfg.hubble_dataset,
                cfg.hubble_max_span,
                tip,
            )
            .await
            {
                Ok(h) => {
                    return Some(Replay {
                        source: Arc::new(h),
                        attempt: Attempt::BackfillArchival,
                    });
                }
                Err(e) => warn!(
                    "[event_loop] hubble unavailable ({e:#}); falling back to \
                     RPC replay, which cannot reach past its retention window"
                ),
            }
        }
    }
    match cfg.backfill_source_url.as_str() {
        "" => {
            warn!(
                "[event_loop] no replay source configured; gaps below live \
                 retention will be skipped"
            );
            None
        }
        url => match BackfillSource::new(url, cfg.backfill_max_span) {
            Ok(b) => Some(Replay {
                source: Arc::new(b),
                attempt: Attempt::BackfillBounded,
            }),
            Err(e) => {
                error!("[event_loop] RPC replay init failed: {e:?}");
                None
            }
        },
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
    // Read the chain tip before handing the client over: it identifies which
    // network this worker is on, which is what proves a configured Hubble
    // dataset belongs to the same one.
    let network_tip = match client.get_latest_ledger().await {
        Ok(l) => Some(l.sequence as i32),
        Err(e) => {
            warn!("[event_loop] could not read the chain tip: {e:?}");
            None
        }
    };
    let source: Arc<dyn EventSource> = Arc::new(RpcEventSource::new(client));

    // History older than live-RPC retention is replayed by `build_replay`:
    // Hubble when it is usable, the RPC endpoint otherwise. Either way it is
    // reached only while the cursor sits below the retention floor, so it
    // covers `[cursor, floor)` and the live tail resumes the moment the RPC can
    // serve the range itself.
    let cfg = get_config();
    let backfill = build_replay(&cfg, network_tip).await;
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
        // Below the known retention floor, replay; otherwise tail the live RPC.
        let (active, attempt) = match (retention_floor, &backfill) {
            (Some(floor), Some(r)) if cursor < floor => (&r.source, r.attempt),
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
                        retention_floor = Some(floor);
                        warn!(
                            "[event_loop] cursor {cursor} below RPC retention \
                             floor {floor} and no backfill configured; \
                             skipping gap to {floor}, ledgers {cursor}-{} \
                             will not be indexed",
                            floor - 1
                        );
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
    fn never_skips_a_gap_hubble_could_still_replay() {
        // Hubble holds history from genesis, so a failure from it is transport
        // or query trouble — hold and report. Skipping here would silently
        // discard ledgers that are still recoverable.
        assert_eq!(
            classify_fetch_error(
                BELOW_FLOOR,
                63_888_609,
                Attempt::BackfillArchival
            ),
            Recovery::Report
        );
    }

    #[test]
    fn skips_gap_when_the_rpc_fallback_runs_out_of_history() {
        // The original incident, and the reason the fallback needs different
        // semantics: the RPC replay echoed the live RPC's out-of-range
        // rejection every poll and the cursor never moved. Nothing can serve
        // the gap, so it must advance.
        assert_eq!(
            classify_fetch_error(
                BELOW_FLOOR,
                63_888_609,
                Attempt::BackfillBounded
            ),
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
    fn live_tail_hands_the_gap_over_exactly_once() {
        // Poll 1: the RPC drops the cursor, the floor is learned, the cursor
        // holds. Hubble then replays [cursor, floor) and the loop routes back
        // to the live tail the moment the cursor reaches the floor.
        let cursor = 63_888_609;
        assert_eq!(
            classify_fetch_error(
                BELOW_FLOOR,
                cursor,
                Attempt::LiveWithBackfill
            ),
            Recovery::ReplayGap { floor: 64_010_315 }
        );
        // Once caught up to the floor the rejection no longer applies, so the
        // handoff cannot re-trigger the gap.
        assert_eq!(
            classify_fetch_error(
                BELOW_FLOOR,
                64_010_315,
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
            classify_fetch_error(
                BELOW_FLOOR,
                64_131_021,
                Attempt::BackfillBounded
            ),
            Recovery::Report
        );
        assert_eq!(
            classify_fetch_error(
                "connection reset",
                63_888_609,
                Attempt::BackfillBounded
            ),
            Recovery::Report
        );
        // A transport failure against Hubble must not be mistaken for a
        // retention gap either.
        assert_eq!(
            classify_fetch_error(
                "bigquery query: connection reset",
                63_888_609,
                Attempt::BackfillArchival
            ),
            Recovery::Report
        );
    }

    fn env_with(hubble: &str, rpc_replay: &str) -> lw_config::config::LocalEnv {
        lw_config::config::LocalEnv {
            hubble_billing_project: hubble.to_string(),
            backfill_source_url: rpc_replay.to_string(),
            ..lw_config::config::LocalEnv::new()
        }
    }

    /// Pubnet's tip, i.e. a worker whose RPC agrees with Hubble's network.
    const PUBNET_TIP: i32 = 64_131_020;

    #[tokio::test]
    async fn falls_back_to_rpc_replay_when_hubble_has_no_credentials() {
        // Point ADC at a file that cannot exist so the outcome does not depend
        // on whether the host happens to be authenticated to GCP. No other
        // test reads this variable.
        unsafe {
            std::env::set_var(
                "GOOGLE_APPLICATION_CREDENTIALS",
                "/nonexistent/lw-chain-test-credentials.json",
            );
        }
        // The Hubble probe must fail and hand over to the RPC replay rather
        // than leaving the worker with no replay source at all.
        let replay = build_replay(
            &env_with(
                "some-billing-project",
                "https://soroban-testnet.stellar.org",
            ),
            Some(PUBNET_TIP),
        )
        .await
        .expect("must fall back rather than give up");
        assert_eq!(replay.attempt, Attempt::BackfillBounded);
    }

    #[tokio::test]
    async fn refuses_hubble_when_the_network_cannot_be_confirmed() {
        // Without the chain tip there is no way to tell a pubnet dataset from
        // the network this worker actually tails, and a mismatch would advance
        // the cursor over history it never read. Use the RPC replay instead.
        let replay = build_replay(
            &env_with(
                "some-billing-project",
                "https://soroban-testnet.stellar.org",
            ),
            None,
        )
        .await
        .expect("must still provide a replay source");
        assert_eq!(replay.attempt, Attempt::BackfillBounded);
    }

    #[tokio::test]
    async fn no_replay_source_when_neither_is_configured() {
        assert!(
            build_replay(&env_with("", ""), Some(PUBNET_TIP))
                .await
                .is_none()
        );
    }
}
