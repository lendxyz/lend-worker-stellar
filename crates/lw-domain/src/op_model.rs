use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Type, types::Json};
use uuid::Uuid;

/// Net funded shares: invested minus refunded, saturating at zero.
pub fn net_funded_shares(invested_shares: u128, refunded_shares: u128) -> u128 {
    invested_shares.saturating_sub(refunded_shares)
}

#[derive(Debug, Clone, Type, Serialize, Deserialize, PartialEq, Eq)]
#[sqlx(type_name = "funding_status", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum FundingStatus {
    Open,
    Finished,
    Paused,
    Predeposit,
    Upcoming,
    Canceled,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct SupportedChains {
    pub op_token: String,
    pub chain_id: i32,
    /// Retained for DB-schema parity with the EVM model; always 0 on Stellar
    /// (no LayerZero).
    pub lz_endpoint_id: i32,
    pub primary: bool,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Operation {
    pub id: Uuid,
    pub funding_status: FundingStatus,
    pub funding_goal: Option<String>,
    pub shares_sold: Option<String>,
    pub funding_participants: Option<i32>,
    pub total_shares: Option<String>,
    pub supported_chains: Json<Vec<SupportedChains>>,
    pub factory_op_id: Option<i32>,
}
