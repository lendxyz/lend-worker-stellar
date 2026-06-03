use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use log::{error, info};
use tokio::sync::mpsc;
use tokio::time::sleep;
use uuid::Uuid;

use lw_config::chain_config::get_rpc_client;
use lw_config::config::get_config;
use lw_config::types::{IndexerCommand, ObservableContract};
use lw_domain::activity_model::Activity;
use lw_storage::op_repository::PgOperationStore;

use crate::event_source::{EventSource, RpcEventSource};
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
    let operations = PgOperationStore::from_global();
    let poll = Duration::from_millis(get_config().poll_interval_ms);

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
        match source.fetch(cursor, &ids).await {
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
                error!("[event_loop] fetch error at ledger {cursor}: {e:?}")
            }
        }

        sleep(poll).await;
    }
}
