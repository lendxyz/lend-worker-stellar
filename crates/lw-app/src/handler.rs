use itertools::Itertools;
use log::{error, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use lw_chain::event_loop::event_loop;
use lw_config::chain_config::StellarNetwork;
use lw_config::config::{STELLAR_CHAIN_ID, get_config};
use lw_config::types::{
    ContractType, IndexerCommand, ObservableContract, OpMapping,
};
use lw_domain::activity_model::{
    Activity, ActivityEventType, InvestedFiatData, factory_needs_refresh,
};
use lw_domain::fiat_holdings::FiatHolding;
use lw_domain::op_model::{Operation, net_funded_shares};
use lw_storage::activity_repository::{ActivityStore, PgActivityStore};
use lw_storage::fiat_holdings_repository::{
    FiatHoldingStore, PgFiatHoldingStore,
};
use lw_storage::op_repository::{
    OperationProgressUpdate, OperationStore, PgOperationStore,
};

/// Whether `addr` is a Soroban StrKey address: a contract (`C...`) or
/// account (`G...`) key, 56 chars of base32 (`A-Z`, `2-7`). Anything else
/// (e.g. an EVM EIP-55 `0x...` address) is rejected.
fn is_soroban_address(addr: &str) -> bool {
    addr.len() == 56
        && matches!(addr.as_bytes()[0], b'C' | b'G')
        && addr
            .bytes()
            .all(|b| b.is_ascii_uppercase() || (b'2'..=b'7').contains(&b))
}

#[derive(Clone)]
pub struct Handler {
    pub fopid_to_dbopid: OpMapping,
    pub op_data: HashMap<Uuid, Operation>,
    pub contracts: Vec<ObservableContract>,
    pub tx_cmd: Option<mpsc::Sender<IndexerCommand>>,
    activity: Arc<dyn ActivityStore>,
    operations: Arc<dyn OperationStore>,
    fiat_holdings: Arc<dyn FiatHoldingStore>,
}

impl Default for Handler {
    fn default() -> Self {
        Self::new()
    }
}

impl Handler {
    /// Production constructor: wires the Postgres-backed stores from the
    /// process-wide pool (requires `setup_db()` to have run).
    pub fn new() -> Self {
        info!("[setup] setting up stores");
        Self::with_stores(
            Arc::new(PgActivityStore::from_global()),
            Arc::new(PgOperationStore::from_global()),
            Arc::new(PgFiatHoldingStore::from_global()),
        )
    }

    pub fn with_stores(
        activity: Arc<dyn ActivityStore>,
        operations: Arc<dyn OperationStore>,
        fiat_holdings: Arc<dyn FiatHoldingStore>,
    ) -> Self {
        Self {
            fopid_to_dbopid: HashMap::new(),
            op_data: HashMap::new(),
            contracts: Vec::new(),
            tx_cmd: None,
            activity,
            operations,
            fiat_holdings,
        }
    }

    pub async fn run(&mut self) -> eyre::Result<()> {
        info!("[init] Fetching OP data...");
        self.get_op_data().await;

        info!("[init] Fetching factory data...");
        self.get_latest_factory_activity().await;

        info!("[init] Fetching rewards data...");
        self.get_latest_rewards_activity().await;

        info!("[init] Fetching contracts starting blocks...");
        self.get_latest_oplend_activity().await;

        info!("[init] Starting indexer...");

        let (tx_events, mut rx_events) = mpsc::channel::<Vec<Activity>>(1000);
        let (tx_cmd, rx_cmd) = mpsc::channel::<IndexerCommand>(10);

        self.tx_cmd = Some(tx_cmd.clone());

        tokio::spawn(event_loop(rx_cmd, tx_events));

        tx_cmd
            .send(IndexerCommand::UpdateContracts(
                self.contracts.clone(),
                self.fopid_to_dbopid.clone(),
            ))
            .await?;

        let mut handler = self.clone();
        while let Some(activity) = rx_events.recv().await {
            let _ = handler.process_events(activity).await;
        }

        Ok(())
    }

    pub async fn process_events(
        &mut self,
        events: Vec<Activity>,
    ) -> eyre::Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let unique_types: HashSet<_> =
            events.iter().map(|e| &e.event_type).collect();
        let event_list = unique_types.iter().map(|t| t.to_string()).join(", ");
        info!("[Handler::process_events] Activity found: {event_list}");

        match self.activity.insert_many(&events).await {
            Ok(_) => {
                if factory_needs_refresh(&events) {
                    self.sync_op_progress().await;
                    self.sync_op_status(&events).await;
                }
            }
            Err(e) => {
                warn!("[Handler::process_events] Failed to save events: {e:?}")
            }
        };

        Ok(())
    }

    async fn refresh_data(
        &mut self,
        tx_cmd: Option<mpsc::Sender<IndexerCommand>>,
    ) -> eyre::Result<()> {
        if let Some(tx) = tx_cmd {
            self.contracts = Vec::new();
            self.get_op_data().await;
            self.get_latest_factory_activity().await;
            self.get_latest_rewards_activity().await;
            self.get_latest_oplend_activity().await;

            tx.send(IndexerCommand::UpdateContracts(
                self.contracts.clone(),
                self.fopid_to_dbopid.clone(),
            ))
            .await?;
        }
        Ok(())
    }

    async fn sync_op_progress(&mut self) {
        let active_ops = self.operations.get_ongoing_operations().await;

        if let Ok(ops) = active_ops
            && !ops.is_empty()
        {
            let tp_query =
                self.activity.get_operation_participants(ops.clone()).await;
            let ti_query =
                self.activity.get_total_invested_amounts(ops.clone()).await;
            let tr_query =
                self.activity.get_total_refunded_amounts(ops.clone()).await;
            let ti_stlr_query = self
                .activity
                .get_total_stellar_invested_amounts(ops.clone())
                .await;
            let tr_stlr_query = self
                .activity
                .get_total_stellar_refunded_amounts(ops.clone())
                .await;

            if let (
                Ok(total_participants),
                Ok(total_invested),
                Ok(total_refunded),
                Ok(total_invested_stlr),
                Ok(total_refunded_stlr),
            ) = (tp_query, ti_query, tr_query, ti_stlr_query, tr_stlr_query)
            {
                let mut res: HashMap<i32, OperationProgressUpdate> =
                    HashMap::new();

                for op_id in ops {
                    let participants =
                        total_participants.get(&op_id).cloned().unwrap_or(0);

                    let invested = total_invested
                        .get(&op_id)
                        .and_then(|s| {
                            s.total_shares_bought.parse::<u128>().ok()
                        })
                        .unwrap_or(0);

                    let refunded = total_refunded
                        .get(&op_id)
                        .and_then(|s| {
                            s.total_shares_refunded.parse::<u128>().ok()
                        })
                        .unwrap_or(0);

                    let invested_stlr = total_invested_stlr
                        .get(&op_id)
                        .and_then(|s| {
                            s.total_shares_bought.parse::<u128>().ok()
                        })
                        .unwrap_or(0);

                    let refunded_stlr = total_refunded_stlr
                        .get(&op_id)
                        .and_then(|s| {
                            s.total_shares_refunded.parse::<u128>().ok()
                        })
                        .unwrap_or(0);

                    let funded_amount = net_funded_shares(invested, refunded);
                    let stellar_funded_amount =
                        net_funded_shares(invested_stlr, refunded_stlr);

                    res.insert(
                        op_id,
                        OperationProgressUpdate {
                            participants,
                            funded_amount: funded_amount.to_string(),
                            stellar_funded_amount: stellar_funded_amount
                                .to_string(),
                        },
                    );
                }

                if let Err(err) =
                    self.operations.update_operation_progress(&res).await
                {
                    error!(
                        "[Handler::sync_op_progress] Failed to update op progress: {err:?}"
                    );
                }
            }
        }
    }

    fn stellar_chain_is_primary(&self, fop_id: i32) -> bool {
        self.fopid_to_dbopid
            .get(&fop_id)
            .and_then(|op_id| self.op_data.get(op_id))
            .map(|op| {
                op.supported_chains
                    .0
                    .iter()
                    .any(|c| c.chain_id == STELLAR_CHAIN_ID && c.primary)
            })
            .unwrap_or(false)
    }

    async fn sync_op_status(&mut self, events: &Vec<Activity>) {
        let unfinished_ops = self.operations.get_unfinished_operations().await;

        if let Ok(ops) = unfinished_ops
            && !ops.is_empty()
        {
            if let Ok(statuses) =
                self.activity.get_operation_status_history(ops).await
            {
                // Only sync status for ops whose Stellar chain (chain_id 0) is
                // the primary one. Skip the call entirely if none qualify, so
                // the repository stays free of this filtering logic.
                let statuses: HashMap<i32, ActivityEventType> = statuses
                    .into_iter()
                    .filter(|(fop_id, _)| {
                        self.stellar_chain_is_primary(*fop_id)
                    })
                    .collect();

                if !statuses.is_empty()
                    && let Err(err) =
                        self.operations.update_operation_status(&statuses).await
                {
                    error!(
                        "[Handler::sync_op_status] Failed to update op status: {err:?}"
                    );
                }
            }

            for event in events {
                if let ActivityEventType::OpCreated = event.event_type {
                    if let Err(err) = self
                        .operations
                        .update_operation_total_shares(
                            event.factory_op_id,
                            event.data.clone(),
                        )
                        .await
                    {
                        error!(
                            "[Handler::sync_op_status] Failed to update op total shares: {err:?}"
                        );
                    }

                    if let Err(err) =
                        self.refresh_data(self.tx_cmd.clone()).await
                    {
                        error!(
                            "[Handler::sync_op_status] Failed to refresh data: {err:?}"
                        );
                    }
                }

                if let ActivityEventType::InvestedFiat = event.event_type {
                    match serde_json::from_value::<InvestedFiatData>(
                        event.data.clone(),
                    ) {
                        Ok(edata) => {
                            let fiat_safe = get_config().fiat_safe_address;
                            let user =
                                event.user_address.clone().unwrap_or_default();
                            if !fiat_safe.is_empty()
                                && edata.op_lend_holder == fiat_safe
                                && user != fiat_safe
                                && !user.is_empty()
                            {
                                let fiat_holding = FiatHolding::new(
                                    event.factory_op_id,
                                    user,
                                    edata.shares_bought,
                                )
                                .set_created_at(event.created_at);

                                if let Err(err) = self
                                    .fiat_holdings
                                    .insert(&fiat_holding)
                                    .await
                                {
                                    error!(
                                        "[Handler::sync_op_status] Failed to create fiat_holdings: {err:?}"
                                    );
                                }
                            }
                        }
                        Err(err) => {
                            error!(
                                "[Handler::sync_op_status] Failed to decode invested_fiat data: {err:?}"
                            );
                        }
                    };
                }
            }
        }
    }

    async fn get_op_data(&mut self) {
        match self.operations.get_all().await {
            Ok(ops) => {
                self.fopid_to_dbopid = ops
                    .clone()
                    .into_iter()
                    .filter_map(|o| o.factory_op_id.map(|fid| (fid, o.id)))
                    .collect();
                self.op_data = ops.into_iter().map(|o| (o.id, o)).collect();
            }
            Err(e) => {
                error!("[Handler::get_op_data] Failed to fetch op data: {e:?}")
            }
        }
    }

    async fn get_latest_factory_activity(&mut self) {
        let net = StellarNetwork::default();
        let mut latest_block = net.factory_start_ledger;
        if let Ok(flb_db) = self.activity.get_factory_latest_block().await
            && flb_db > 0
        {
            latest_block = flb_db;
        }

        self.contracts.push(ObservableContract {
            contract_type: ContractType::Factory,
            address: net.factory_contract_id,
            op_id: None,
            fop_id: None,
            latest_block,
        });
    }

    async fn get_latest_rewards_activity(&mut self) {
        let net = StellarNetwork::default();
        if let Some(rewards_address) = net.rewards_address {
            let latest_block = self
                .activity
                .get_rewards_latest_blocks(STELLAR_CHAIN_ID)
                .await
                .unwrap_or(0);
            self.contracts.push(ObservableContract {
                contract_type: ContractType::Rewards,
                op_id: None,
                fop_id: None,
                address: rewards_address,
                latest_block,
            });
        }
    }

    async fn get_latest_oplend_activity(&mut self) {
        for (_, op_data) in self.op_data.clone() {
            for supp_chain in op_data.supported_chains.0.iter() {
                // op_token can hold a contract address for any supported
                // chain. This worker only indexes Stellar, so skip EVM
                // EIP-55 addresses and keep Soroban StrKey addresses.
                if !is_soroban_address(&supp_chain.op_token) {
                    continue;
                }
                let latest_block = self
                    .activity
                    .get_oplend_latest_blocks(STELLAR_CHAIN_ID, op_data.id)
                    .await
                    .unwrap_or(0);
                self.contracts.push(ObservableContract {
                    contract_type: ContractType::OpLend,
                    op_id: Some(op_data.id),
                    fop_id: op_data.factory_op_id,
                    address: supp_chain.op_token.clone(),
                    latest_block,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::is_soroban_address;

    /// Build a 56-char StrKey from a prefix, padding with base32 `A`s.
    fn strkey(prefix: char) -> String {
        format!("{prefix}{}", "A".repeat(55))
    }

    #[test]
    fn accepts_soroban_contract_and_account() {
        assert!(is_soroban_address(&strkey('C')));
        assert!(is_soroban_address(&strkey('G')));
        // Real-world style contract address (56 base32 chars).
        assert!(is_soroban_address(
            "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
        ));
    }

    #[test]
    fn rejects_evm_eip55_address() {
        assert!(!is_soroban_address(
            "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed"
        ));
    }

    #[test]
    fn rejects_wrong_length() {
        assert!(!is_soroban_address("CAAA"));
        assert!(!is_soroban_address(&format!("C{}", "A".repeat(56))));
        assert!(!is_soroban_address(""));
    }

    #[test]
    fn rejects_bad_prefix() {
        // Right shape, wrong leading char.
        assert!(!is_soroban_address(&strkey('A')));
        assert!(!is_soroban_address(&strkey('M')));
    }

    #[test]
    fn rejects_non_base32_chars() {
        // `1`, `0`, `8`, `9` are outside the base32 alphabet.
        assert!(!is_soroban_address(&format!("C{}1", "A".repeat(54))));
        assert!(!is_soroban_address(&format!("C{}0", "A".repeat(54))));
        assert!(!is_soroban_address(&format!("C{}", "a".repeat(55))));
    }
}
