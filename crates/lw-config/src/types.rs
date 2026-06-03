use std::collections::HashMap;
use uuid::Uuid;

/// Decoded, native representation of each Soroban event we index. Amounts are
/// kept as `i128` (Soroban native); addresses/ids as their StrKey strings.
#[derive(Debug, Clone, PartialEq)]
pub enum ContractEvent {
    OpCreated {
        op_token: String,
        operation_id: u32,
        total_shares: i128,
    },
    OpStarted {
        operation_id: u32,
    },
    OpCanceled {
        operation_id: u32,
    },
    OpPaused {
        operation_id: u32,
    },
    OpResumed {
        operation_id: u32,
    },
    OpFinished {
        operation_id: u32,
        amount_raised_euro: i128,
    },
    OpPredepositsOpen {
        operation_id: u32,
    },
    OpPredepositsClosed {
        operation_id: u32,
    },
    Invested {
        investor: String,
        operation_id: u32,
        usdc_amount: i128,
        shares_bought: i128,
    },
    InvestedFiat {
        investor: String,
        oplend_destination: String,
        operation_id: u32,
        shares_bought: i128,
    },
    ClaimedTokens {
        investor: String,
        operation_id: u32,
        amount: i128,
    },
    Refunded {
        investor: String,
        operation_id: u32,
        usdc_amount: i128,
        shares_refunded: i128,
    },
    OpLendTransfered {
        from: String,
        to: String,
        amount: i128,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractType {
    Factory,
    OpLend,
    /// Dormant: no Soroban Rewards contract observed yet (kept for parity).
    Rewards,
}

/// A contract we poll events for. Flattened from the old per-chain map: the
/// service is single-chain, so a plain list suffices.
#[derive(Debug, Clone)]
pub struct ObservableContract {
    pub contract_type: ContractType,
    pub op_id: Option<Uuid>,
    pub fop_id: Option<i32>,
    /// Soroban contract id (StrKey `C...`).
    pub address: String,
    /// Last indexed ledger sequence (resume cursor floor).
    pub latest_block: i32,
}

pub type OpMapping = HashMap<i32, Uuid>;

/// Command sent to the indexer task. Single variant today (re-subscribe with an
/// updated contract set after dynamic OpLend discovery).
pub enum IndexerCommand {
    UpdateContracts(Vec<ObservableContract>, OpMapping),
}
