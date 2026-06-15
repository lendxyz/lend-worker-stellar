//! Shared test harness for the Soroban integration suite: XDR `ScVal` event
//! builders + recording fake stores. Used by `golden.rs` and `handler.rs`.

#![allow(dead_code)]

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use sqlx::Error;
use sqlx::postgres::PgQueryResult;
use uuid::Uuid;

use stellar_xdr::{Int128Parts, ScAddress, ScMap, ScMapEntry, ScSymbol, ScVal};

use lend_worker_stellar::chain::event_source::RawSorobanEvent;
use lend_worker_stellar::models::activity_model::{
    Activity, ActivityEventType,
};
use lend_worker_stellar::models::fiat_holdings::FiatHolding;
use lend_worker_stellar::models::op_model::Operation;
use lend_worker_stellar::repositories::activity_repository::{
    ActivityStore, InvestedTotals, RefundedTotals,
};
use lend_worker_stellar::repositories::fiat_holdings_repository::FiatHoldingStore;
use lend_worker_stellar::repositories::op_repository::{
    OperationProgressUpdate, OperationStore,
};

// Valid testnet StrKeys (checksum-verified; shared with the lw-chain unit tests).
pub const FACTORY: &str =
    "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC";
pub const ACCOUNT_A: &str =
    "GBZXN7PIRZGNMHGA7MUUUF4GWPY5AYPV6LY4UV2GL6VJGIQRXFDNMADI";
pub const ACCOUNT_B: &str =
    "GADQOBYHA4DQOBYHA4DQOBYHA4DQOBYHA4DQOBYHA4DQOBYHA4DQOZPI";

/// A fixed ledger close time so golden snapshots are deterministic.
pub fn fixed_ts() -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000, 0).unwrap()
}

/// Process-wide serial guard for DB tests: they share one schema and each
/// re-applies it (DROP/CREATE), so they must not run concurrently.
pub fn db_serial() -> &'static tokio::sync::Mutex<()> {
    static L: std::sync::OnceLock<tokio::sync::Mutex<()>> =
        std::sync::OnceLock::new();
    L.get_or_init(|| tokio::sync::Mutex::new(()))
}

// ---- XDR ScVal builders -------------------------------------------------

pub fn sym(s: &str) -> ScVal {
    ScVal::Symbol(ScSymbol(s.try_into().unwrap()))
}

pub fn u32v(n: u32) -> ScVal {
    ScVal::U32(n)
}

pub fn i128v(n: i128) -> ScVal {
    ScVal::I128(Int128Parts {
        hi: (n >> 64) as i64,
        lo: n as u64,
    })
}

pub fn addr(strkey: &str) -> ScVal {
    ScVal::Address(ScAddress::from_str(strkey).unwrap())
}

/// Build a `data_format = "map"` event payload from `(name, value)` pairs.
pub fn data_map(pairs: &[(&str, ScVal)]) -> ScVal {
    let entries: Vec<ScMapEntry> = pairs
        .iter()
        .map(|(k, v)| ScMapEntry {
            key: sym(k),
            val: v.clone(),
        })
        .collect();
    ScVal::Map(Some(ScMap(entries.try_into().unwrap())))
}

/// Assemble a `RawSorobanEvent` with a fixed ledger/timestamp/tx for snapshots.
pub fn raw_event(
    topics: Vec<ScVal>,
    value: ScVal,
    contract_id: &str,
) -> RawSorobanEvent {
    RawSorobanEvent {
        tx_hash: "deadbeef".into(),
        event_index: 0,
        contract_id: contract_id.into(),
        topics,
        value,
        ledger_seq: 12_345,
        ledger_closed_at: fixed_ts(),
    }
}

// ---- Recording fake stores ----------------------------------------------

/// Fake `OperationStore`. Resolves fop->uuid from a fixed map and records the
/// mutating calls the handler makes so tests can assert on them.
#[derive(Default)]
pub struct FakeOperationStore {
    pub fopid_to_uuid: HashMap<i32, Uuid>,
    pub all: Vec<Operation>,
    pub ongoing: Vec<i32>,
    pub unfinished: Vec<i32>,
    pub total_shares_calls: Mutex<Vec<(i32, serde_json::Value)>>,
    pub status_history: HashMap<i32, ActivityEventType>,
}

#[async_trait]
impl OperationStore for FakeOperationStore {
    async fn get_all(&self) -> Result<Vec<Operation>, Error> {
        Ok(self.all.clone())
    }
    async fn get_op_id_from_fop_id(&self, fopid: i32) -> Result<Uuid, Error> {
        Ok(self.fopid_to_uuid.get(&fopid).copied().unwrap_or_default())
    }
    async fn get_ongoing_operations(&self) -> Result<Vec<i32>, Error> {
        Ok(self.ongoing.clone())
    }
    async fn get_unfinished_operations(&self) -> Result<Vec<i32>, Error> {
        Ok(self.unfinished.clone())
    }
    async fn update_operation_progress(
        &self,
        _u: &HashMap<i32, OperationProgressUpdate>,
    ) -> Result<PgQueryResult, Error> {
        Ok(PgQueryResult::default())
    }
    async fn update_operation_status(
        &self,
        _u: &HashMap<i32, ActivityEventType>,
    ) -> Result<PgQueryResult, Error> {
        Ok(PgQueryResult::default())
    }
    async fn update_operation_total_shares(
        &self,
        op_id: i32,
        data: serde_json::Value,
    ) -> Result<PgQueryResult, Error> {
        self.total_shares_calls.lock().unwrap().push((op_id, data));
        Ok(PgQueryResult::default())
    }
    async fn add_supported_chain(
        &self,
        _op_id: i32,
        _d: serde_json::Value,
    ) -> Result<PgQueryResult, Error> {
        Ok(PgQueryResult::default())
    }
}

/// Fake `ActivityStore` recording every inserted activity.
#[derive(Default)]
pub struct FakeActivityStore {
    pub inserted: Mutex<Vec<Activity>>,
}

#[async_trait]
impl ActivityStore for FakeActivityStore {
    async fn insert(
        &self,
        activity: &Activity,
    ) -> Result<PgQueryResult, Error> {
        self.inserted.lock().unwrap().push(activity.clone());
        Ok(PgQueryResult::default())
    }
    async fn insert_many(
        &self,
        activities: &[Activity],
    ) -> Result<PgQueryResult, Error> {
        self.inserted.lock().unwrap().extend_from_slice(activities);
        Ok(PgQueryResult::default())
    }
    async fn get_oplend_latest_blocks(&self, _o: Uuid) -> Result<i32, Error> {
        Ok(0)
    }
    async fn get_rewards_latest_blocks(&self) -> Result<i32, Error> {
        Ok(0)
    }
    async fn get_factory_latest_block(&self) -> Result<i32, Error> {
        Ok(0)
    }
    async fn get_total_invested_amounts(
        &self,
        _o: Vec<i32>,
    ) -> Result<HashMap<i32, InvestedTotals>, Error> {
        Ok(HashMap::new())
    }
    async fn get_total_refunded_amounts(
        &self,
        _o: Vec<i32>,
    ) -> Result<HashMap<i32, RefundedTotals>, Error> {
        Ok(HashMap::new())
    }
    async fn get_total_stellar_invested_amounts(
        &self,
        _o: Vec<i32>,
    ) -> Result<HashMap<i32, InvestedTotals>, Error> {
        Ok(HashMap::new())
    }
    async fn get_total_stellar_refunded_amounts(
        &self,
        _o: Vec<i32>,
    ) -> Result<HashMap<i32, RefundedTotals>, Error> {
        Ok(HashMap::new())
    }
    async fn get_operation_participants(
        &self,
        _o: Vec<i32>,
    ) -> Result<HashMap<i32, i64>, Error> {
        Ok(HashMap::new())
    }
    async fn get_operation_status_history(
        &self,
        _o: Vec<i32>,
    ) -> Result<HashMap<i32, ActivityEventType>, Error> {
        Ok(HashMap::new())
    }
}

/// Fake `FiatHoldingStore` recording inserts.
#[derive(Default)]
pub struct FakeFiatHoldingStore {
    pub inserted: Mutex<Vec<FiatHolding>>,
}

#[async_trait]
impl FiatHoldingStore for FakeFiatHoldingStore {
    async fn insert(
        &self,
        holding: &FiatHolding,
    ) -> Result<PgQueryResult, Error> {
        self.inserted.lock().unwrap().push(holding.clone());
        Ok(PgQueryResult::default())
    }
}
