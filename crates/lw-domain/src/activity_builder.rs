//! Pure mapping from a decoded `ContractEvent` to the `Activity` rows it
//! produces. The I/O variants (`OpCreated`, `OpLendTransfered`) need DB/contract
//! context and are handled in `lw_chain::log_handlers::handle_event`; they reach
//! `_ => None` here.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::json;
use uuid::Uuid;

use lw_config::config::{AppEnv, get_config};
use lw_config::types::ContractEvent;

use crate::activity_model::{Activity, ActivityBuilder, ActivityEventType};

/// Outside production, factory events for reserved low op ids (1..=10) are
/// test/staging noise and dropped.
pub fn is_filtered_test_op(operation_id: u32) -> bool {
    let cfg = get_config();
    cfg.env != AppEnv::Production && operation_id <= 10
}

fn op_uuid(fopid_to_opid: &HashMap<i32, Uuid>, op_id: u32) -> Option<Uuid> {
    fopid_to_opid.get(&(op_id as i32)).copied()
}

/// Build activities for an I/O-free event. `None` = no activity (filtered test
/// op, unknown op) or an I/O variant handled elsewhere.
pub fn build_activity(
    fopid_to_opid: &HashMap<i32, Uuid>,
    event: ContractEvent,
    tx_hash: &str,
    block_number: i32,
    block_timestamp: DateTime<Utc>,
) -> Option<Vec<Activity>> {
    match event {
        ContractEvent::ClaimedTokens {
            investor,
            operation_id,
            amount,
        } => {
            let op_id = op_uuid(fopid_to_opid, operation_id)?;
            Some(vec![
                ActivityBuilder::new(ActivityEventType::ClaimedOpToken, block_number)
                    .block_timestamp(block_timestamp)
                    .event_hash(format!("{tx_hash}#lend_op_tokens_claimed"))
                    .op_id(op_id)
                    .factory_op_id(operation_id as i32)
                    .user_address(Some(investor))
                    .data(json!({ "tx_hash": tx_hash, "token_amount": amount.to_string() }))
                    .build(),
            ])
        }
        ContractEvent::OpPredepositsOpen { operation_id } => {
            if is_filtered_test_op(operation_id) {
                return None;
            }
            let op_id = op_uuid(fopid_to_opid, operation_id)?;
            Some(vec![
                ActivityBuilder::new(
                    ActivityEventType::OpPredepositsOpen,
                    block_number,
                )
                .block_timestamp(block_timestamp)
                .event_hash(format!("{tx_hash}#lend_op_predeposits_open"))
                .op_id(op_id)
                .factory_op_id(operation_id as i32)
                .data(json!({ "tx_hash": tx_hash }))
                .build(),
            ])
        }
        ContractEvent::OpPredepositsClosed { operation_id } => {
            if is_filtered_test_op(operation_id) {
                return None;
            }
            let op_id = op_uuid(fopid_to_opid, operation_id)?;
            Some(vec![
                ActivityBuilder::new(
                    ActivityEventType::OpPredepositsClosed,
                    block_number,
                )
                .block_timestamp(block_timestamp)
                .event_hash(format!("{tx_hash}#lend_op_predeposits_closed"))
                .op_id(op_id)
                .factory_op_id(operation_id as i32)
                .data(json!({ "tx_hash": tx_hash }))
                .build(),
            ])
        }
        ContractEvent::Invested {
            investor,
            operation_id,
            usdc_amount,
            shares_bought,
        } => {
            if is_filtered_test_op(operation_id) {
                return None;
            }
            let op_id = op_uuid(fopid_to_opid, operation_id)?;
            Some(vec![
                ActivityBuilder::new(ActivityEventType::Invested, block_number)
                    .block_timestamp(block_timestamp)
                    .event_hash(format!("{tx_hash}#lend_invested"))
                    .op_id(op_id)
                    .factory_op_id(operation_id as i32)
                    .user_address(Some(investor))
                    .data(json!({
                        "tx_hash": tx_hash,
                        "usdc_amount": usdc_amount.to_string(),
                        "shares_bought": shares_bought.to_string()
                    }))
                    .build(),
            ])
        }
        ContractEvent::InvestedFiat {
            investor,
            oplend_destination,
            operation_id,
            shares_bought,
        } => {
            if is_filtered_test_op(operation_id) {
                return None;
            }
            let op_id = op_uuid(fopid_to_opid, operation_id)?;
            Some(vec![
                ActivityBuilder::new(
                    ActivityEventType::InvestedFiat,
                    block_number,
                )
                .block_timestamp(block_timestamp)
                .event_hash(format!("{tx_hash}#lend_invested_fiat"))
                .op_id(op_id)
                .factory_op_id(operation_id as i32)
                .user_address(Some(investor))
                .data(json!({
                    "tx_hash": tx_hash,
                    "op_lend_holder": oplend_destination,
                    "shares_bought": shares_bought.to_string()
                }))
                .build(),
            ])
        }
        ContractEvent::Refunded {
            investor,
            operation_id,
            usdc_amount,
            shares_refunded,
        } => {
            if is_filtered_test_op(operation_id) {
                return None;
            }
            let op_id = op_uuid(fopid_to_opid, operation_id)?;
            Some(vec![
                ActivityBuilder::new(ActivityEventType::Refunded, block_number)
                    .block_timestamp(block_timestamp)
                    .event_hash(format!("{tx_hash}#lend_refunded"))
                    .op_id(op_id)
                    .factory_op_id(operation_id as i32)
                    .user_address(Some(investor))
                    .data(json!({
                        "tx_hash": tx_hash,
                        "usdc_amount": usdc_amount.to_string(),
                        "shares_refunded": shares_refunded.to_string()
                    }))
                    .build(),
            ])
        }
        ContractEvent::OpStarted { operation_id } => simple_lifecycle(
            fopid_to_opid,
            operation_id,
            ActivityEventType::OpStarted,
            "lend_op_started",
            tx_hash,
            block_number,
            block_timestamp,
        ),
        ContractEvent::OpFinished {
            operation_id,
            amount_raised_euro,
        } => {
            if is_filtered_test_op(operation_id) {
                return None;
            }
            let op_id = op_uuid(fopid_to_opid, operation_id)?;
            Some(vec![
                ActivityBuilder::new(ActivityEventType::OpFinished, block_number)
                    .block_timestamp(block_timestamp)
                    .event_hash(format!("{tx_hash}#lend_op_finished"))
                    .op_id(op_id)
                    .factory_op_id(operation_id as i32)
                    .data(json!({ "tx_hash": tx_hash, "amount_raised": amount_raised_euro.to_string() }))
                    .build(),
            ])
        }
        ContractEvent::OpCanceled { operation_id } => simple_lifecycle(
            fopid_to_opid,
            operation_id,
            ActivityEventType::OpCanceled,
            "lend_op_canceled",
            tx_hash,
            block_number,
            block_timestamp,
        ),
        ContractEvent::OpPaused { operation_id } => simple_lifecycle(
            fopid_to_opid,
            operation_id,
            ActivityEventType::OpPaused,
            "lend_op_paused",
            tx_hash,
            block_number,
            block_timestamp,
        ),
        ContractEvent::OpResumed { operation_id } => simple_lifecycle(
            fopid_to_opid,
            operation_id,
            ActivityEventType::OpResumed,
            "lend_op_resumed",
            tx_hash,
            block_number,
            block_timestamp,
        ),
        ContractEvent::ClaimedRewards {
            op_id,
            user,
            balance,
        } => {
            let op_uuid = op_uuid(fopid_to_opid, op_id)?;
            Some(vec![
                ActivityBuilder::new(
                    ActivityEventType::ClaimedRewards,
                    block_number,
                )
                .block_timestamp(block_timestamp)
                .event_hash(format!("{tx_hash}#lend_rewards_claimed"))
                .op_id(op_uuid)
                .factory_op_id(op_id as i32)
                .user_address(Some(user))
                .data(json!({
                    "tx_hash": tx_hash,
                    "usdc_amount": balance.to_string(),
                }))
                .build(),
            ])
        }
        // Referral rewards are not tied to an operation.
        ContractEvent::ClaimedRefRewards { user, balance } => Some(vec![
            ActivityBuilder::new(
                ActivityEventType::ClaimedRefRewards,
                block_number,
            )
            .block_timestamp(block_timestamp)
            .event_hash(format!("{tx_hash}#lend_ref_rewards_claimed"))
            .user_address(Some(user))
            .data(json!({
                "tx_hash": tx_hash,
                "usdc_amount": balance.to_string(),
            }))
            .build(),
        ]),
        ContractEvent::RewardsDistributed {
            op_id,
            epoch,
            amount,
        } => {
            let op_uuid = op_uuid(fopid_to_opid, op_id)?;
            Some(vec![
                ActivityBuilder::new(
                    ActivityEventType::RewardsDistributed,
                    block_number,
                )
                .block_timestamp(block_timestamp)
                .event_hash(format!("{tx_hash}#lend_rewards_distributed"))
                .op_id(op_uuid)
                .factory_op_id(op_id as i32)
                .data(json!({
                    "tx_hash": tx_hash,
                    "usdc_amount": amount.to_string(),
                    "epoch": epoch.to_string(),
                    "op_id": op_id as i32,
                }))
                .build(),
            ])
        }
        ContractEvent::RefRewardsDistributed { epoch, amount } => Some(vec![
            ActivityBuilder::new(
                ActivityEventType::RefRewardsDistributed,
                block_number,
            )
            .block_timestamp(block_timestamp)
            .event_hash(format!("{tx_hash}#lend_ref_rewards_distributed"))
            .data(json!({
                "tx_hash": tx_hash,
                "usdc_amount": amount.to_string(),
                "epoch": epoch.to_string(),
            }))
            .build(),
        ]),
        // I/O variants handled in lw_chain::log_handlers::handle_event.
        _ => None,
    }
}

/// Shared body for the no-data lifecycle events (started/paused/resumed/canceled).
fn simple_lifecycle(
    fopid_to_opid: &HashMap<i32, Uuid>,
    operation_id: u32,
    event_type: ActivityEventType,
    suffix: &str,
    tx_hash: &str,
    block_number: i32,
    block_timestamp: DateTime<Utc>,
) -> Option<Vec<Activity>> {
    if is_filtered_test_op(operation_id) {
        return None;
    }
    let op_id = op_uuid(fopid_to_opid, operation_id)?;
    Some(vec![
        ActivityBuilder::new(event_type, block_number)
            .block_timestamp(block_timestamp)
            .event_hash(format!("{tx_hash}#{suffix}"))
            .op_id(op_id)
            .factory_op_id(operation_id as i32)
            .data(json!({ "tx_hash": tx_hash }))
            .build(),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use lw_config::types::ContractEvent;
    use std::collections::HashMap;
    use uuid::Uuid;

    fn map(fop: i32) -> HashMap<i32, Uuid> {
        let mut m = HashMap::new();
        m.insert(fop, Uuid::from_u128(7));
        m
    }

    #[test]
    fn invested_maps_to_one_activity() {
        let ev = ContractEvent::Invested {
            investor: "GINV".into(),
            operation_id: 3,
            usdc_amount: 1_000,
            shares_bought: 50,
        };
        let out = build_activity(&map(3), ev, "tx#0", 12, chrono::Utc::now())
            .expect("some");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].event_type, ActivityEventType::Invested);
        assert_eq!(out[0].factory_op_id, 3);
        assert_eq!(out[0].user_address.as_deref(), Some("GINV"));
        assert_eq!(out[0].data["usdc_amount"], "1000");
    }

    #[test]
    fn unknown_op_returns_none() {
        let ev = ContractEvent::OpStarted { operation_id: 99 };
        assert!(
            build_activity(&map(3), ev, "tx#0", 12, chrono::Utc::now())
                .is_none()
        );
    }

    #[test]
    fn op_created_is_deferred_to_handler() {
        let ev = ContractEvent::OpCreated {
            op_token: "C".into(),
            operation_id: 3,
            total_shares: 1,
        };
        assert!(
            build_activity(&map(3), ev, "tx#0", 12, chrono::Utc::now())
                .is_none()
        );
    }
}
