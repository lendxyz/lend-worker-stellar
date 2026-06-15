use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::Type;
use std::fmt;
use uuid::Uuid;

use lw_config::config::STELLAR_CHAIN_ID;

use super::op_model::FundingStatus;

#[derive(Debug, Clone, Type, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[sqlx(type_name = "activity_event_type", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum ActivityEventType {
    NotSpecified,
    Invested,
    InvestedFiat,
    Refunded,
    ClaimedRewards,
    ClaimedRefRewards,
    ClaimedOpToken,
    RewardsDistributed,
    RefRewardsDistributed,
    OpLendBridged,
    OpLendTransfered, // singular "r" at transferred because of typo in DB defs :(
    OpLendPeerAdded,
    OpPredepositsOpen,
    OpPredepositsClosed,
    OpCreated,
    OpPaused,
    OpResumed,
    OpCanceled,
    OpStarted,
    OpFinished,
    OrderFilled,
    OrderCancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct OpCreatedEventData {
    pub tx_hash: String,
    pub op_token: String,
    pub total_shares: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct InvestedFiatData {
    pub tx_hash: String,
    pub op_lend_holder: String,
    pub shares_bought: String,
}

pub fn activity_event_priority(event: &ActivityEventType) -> i32 {
    match event {
        ActivityEventType::OpCreated => 1,
        ActivityEventType::OpPredepositsOpen => 2,
        ActivityEventType::OpPredepositsClosed => 3,
        ActivityEventType::OpStarted => 4,
        ActivityEventType::OpPaused => 5,
        ActivityEventType::OpResumed => 6,
        ActivityEventType::OpFinished => 7,
        ActivityEventType::OpCanceled => 8,
        _ => 0,
    }
}

pub fn activity_event_to_funding_status(
    event: &ActivityEventType,
) -> Option<FundingStatus> {
    match event {
        ActivityEventType::OpCreated => Some(FundingStatus::Upcoming),
        ActivityEventType::OpStarted => Some(FundingStatus::Open),
        ActivityEventType::OpPredepositsOpen => Some(FundingStatus::Predeposit),
        ActivityEventType::OpPredepositsClosed => Some(FundingStatus::Upcoming),
        ActivityEventType::OpPaused => Some(FundingStatus::Paused),
        ActivityEventType::OpResumed => Some(FundingStatus::Open),
        ActivityEventType::OpFinished => Some(FundingStatus::Finished),
        ActivityEventType::OpCanceled => Some(FundingStatus::Canceled),
        _ => None,
    }
}

pub fn factory_needs_refresh(events: &Vec<Activity>) -> bool {
    for event in events {
        if matches!(
            event.event_type,
            ActivityEventType::Invested
                | ActivityEventType::InvestedFiat
                | ActivityEventType::Refunded
                | ActivityEventType::OpCreated
                | ActivityEventType::OpStarted
                | ActivityEventType::OpPredepositsOpen
                | ActivityEventType::OpPaused
                | ActivityEventType::OpResumed
                | ActivityEventType::OpFinished
                | ActivityEventType::OpCanceled
                | ActivityEventType::OpLendPeerAdded
        ) {
            return true;
        }
    }

    false
}

pub fn orders_needs_refresh(events: &Vec<Activity>) -> bool {
    for event in events {
        if matches!(
            event.event_type,
            ActivityEventType::OrderFilled | ActivityEventType::OrderCancelled
        ) {
            return true;
        };
    }

    false
}

impl fmt::Display for ActivityEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_value(self).unwrap().as_str().unwrap()
        )
    }
}

pub struct ActivityBuilder {
    op_id: Uuid,
    factory_op_id: i32,
    event_hash: String,
    chain_id: i32,
    event_type: ActivityEventType,
    user_address: Option<String>,
    block_number: i32,
    data: serde_json::Value,
    created_at: DateTime<Utc>,
}

impl ActivityBuilder {
    pub fn new(event_type: ActivityEventType, block_number: i32) -> Self {
        Self {
            op_id: Uuid::default(),
            factory_op_id: 0,
            event_hash: "".to_string(),
            chain_id: STELLAR_CHAIN_ID,
            event_type,
            user_address: None,
            block_number,
            created_at: Utc::now(),
            data: json!({}),
        }
    }

    pub fn block_timestamp(mut self, bts: DateTime<Utc>) -> Self {
        self.created_at = bts;
        self
    }

    pub fn event_hash(mut self, event_hash: String) -> Self {
        self.event_hash = event_hash;
        self
    }

    pub fn op_id(mut self, op_id: Uuid) -> Self {
        self.op_id = op_id;
        self
    }

    pub fn factory_op_id(mut self, factory_op_id: i32) -> Self {
        self.factory_op_id = factory_op_id;
        self
    }

    pub fn user_address(mut self, user_address: Option<String>) -> Self {
        self.user_address = user_address;
        self
    }

    pub fn data(mut self, data: serde_json::Value) -> Self {
        self.data = data;
        self
    }

    pub fn build(self) -> Activity {
        Activity {
            op_id: self.op_id,
            factory_op_id: self.factory_op_id,
            event_hash: self.event_hash,
            chain_id: self.chain_id,
            event_type: self.event_type,
            user_address: self.user_address,
            block_number: self.block_number,
            data: self.data,
            created_at: self.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct Activity {
    pub op_id: Uuid,
    pub factory_op_id: i32,
    pub event_hash: String,
    pub chain_id: i32,
    pub event_type: ActivityEventType,
    pub user_address: Option<String>,
    pub block_number: i32,
    pub data: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn builder_defaults_chain_id_zero() {
        let a = ActivityBuilder::new(ActivityEventType::OpStarted, 10).build();
        assert_eq!(a.chain_id, 0);
        assert_eq!(a.user_address, None);
    }
    #[test]
    fn builder_sets_string_user_address() {
        let a = ActivityBuilder::new(ActivityEventType::Invested, 10)
            .user_address(Some("GINVESTOR".to_string()))
            .build();
        assert_eq!(a.user_address.as_deref(), Some("GINVESTOR"));
    }
}
