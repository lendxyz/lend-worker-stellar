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

    // History older than live-RPC retention is served by the backfill endpoint.
    // Only usable when a distinct extended-retention URL is configured; without
    // it `BackfillSource` would just re-hit the live RPC and loop on the same
    // out-of-range error, so we fall back to skipping the gap in that case.
    let cfg = get_config();
    let backfill: Option<Arc<dyn EventSource>> =
        if cfg.backfill_source_url.is_empty() {
            None
        } else {
            match BackfillSource::from_config(cfg.backfill_max_span) {
                Ok(b) => Some(Arc::new(b)),
                Err(e) => {
                    error!("[event_loop] backfill source init failed: {e:?}");
                    None
                }
            }
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
        let below_floor = retention_floor.is_some_and(|f| cursor < f);
        let active = match (below_floor, &backfill) {
            (true, Some(b)) => b,
            _ => &source,
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
                // Cursor fell behind live-RPC retention: learn the floor and let
                // the next iteration serve the gap from backfill (or skip it if
                // no backfill endpoint is configured).
                match parse_ledger_range(&e.to_string()) {
                    Some((low, _)) if cursor < low => {
                        retention_floor = Some(low);
                        if backfill.is_some() {
                            warn!(
                                "[event_loop] cursor {cursor} below RPC \
                                 retention floor {low}; backfilling gap"
                            );
                        } else {
                            warn!(
                                "[event_loop] cursor {cursor} below RPC \
                                 retention floor {low} and no backfill source \
                                 configured; skipping gap to {low}"
                            );
                            cursor = low;
                        }
                    }
                    _ => error!(
                        "[event_loop] fetch error at ledger {cursor}: {e:?}"
                    ),
                }
            }
        }

        sleep(poll).await;
    }
}
