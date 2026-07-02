use eyre::eyre;
use serde_json::json;
use std::collections::HashMap;
use uuid::Uuid;

use lw_config::config::get_ignored_addresses;
use lw_config::types::{ContractEvent, ObservableContract};
use lw_domain::activity_builder::build_activity;
use lw_domain::activity_model::{Activity, ActivityBuilder, ActivityEventType};
use lw_storage::op_repository::OperationStore;

use crate::event_source::RawSorobanEvent;
use crate::scval::{
    as_address, as_i128, as_map, as_u32, map_field, symbol_name,
};

/// Decode a raw Soroban event into a native `ContractEvent`. Matches on the
/// event-name symbol in `topics[0]`, then reads `#[topic]` fields positionally
/// (declaration order) and data fields by name from the value map. Returns
/// `Err` for events we do not index (deferred/dropped) and malformed payloads.
pub fn decode_event(raw: &RawSorobanEvent) -> eyre::Result<ContractEvent> {
    let name =
        symbol_name(raw.topics.first().ok_or_else(|| eyre!("no topics"))?)?;
    let t = &raw.topics;
    // Checked topic access: a malformed event (too few topics) becomes an `Err`
    // the caller skips, never an out-of-bounds panic that would kill the loop.
    let tg = |i: usize| {
        t.get(i)
            .ok_or_else(|| eyre!("missing topic {i} for `{name}`"))
    };

    match name.as_str() {
        "operation_created" => Ok(ContractEvent::OpCreated {
            op_token: as_address(tg(1)?)?,
            operation_id: as_u32(tg(2)?)?,
            total_shares: as_i128(map_field(
                as_map(&raw.value)?,
                "total_shares",
            )?)?,
        }),
        "operation_started" => Ok(ContractEvent::OpStarted {
            operation_id: as_u32(tg(1)?)?,
        }),
        "operation_canceled" => Ok(ContractEvent::OpCanceled {
            operation_id: as_u32(tg(1)?)?,
        }),
        "operation_paused" => Ok(ContractEvent::OpPaused {
            operation_id: as_u32(tg(1)?)?,
        }),
        "operation_resumed" => Ok(ContractEvent::OpResumed {
            operation_id: as_u32(tg(1)?)?,
        }),
        "operation_finished" => Ok(ContractEvent::OpFinished {
            operation_id: as_u32(tg(1)?)?,
            amount_raised_euro: as_i128(map_field(
                as_map(&raw.value)?,
                "amount_raised_euro",
            )?)?,
        }),
        "predeposits_open" => Ok(ContractEvent::OpPredepositsOpen {
            operation_id: as_u32(tg(1)?)?,
        }),
        "predeposits_closed" => Ok(ContractEvent::OpPredepositsClosed {
            operation_id: as_u32(tg(1)?)?,
        }),
        "invested" => {
            let m = as_map(&raw.value)?;
            Ok(ContractEvent::Invested {
                investor: as_address(tg(1)?)?,
                operation_id: as_u32(tg(2)?)?,
                usdc_amount: as_i128(map_field(m, "usdc_amount")?)?,
                shares_bought: as_i128(map_field(m, "shares_bought")?)?,
            })
        }
        "invested_fiat" => Ok(ContractEvent::InvestedFiat {
            investor: as_address(tg(1)?)?,
            oplend_destination: as_address(tg(2)?)?,
            operation_id: as_u32(tg(3)?)?,
            shares_bought: as_i128(map_field(
                as_map(&raw.value)?,
                "shares_bought",
            )?)?,
        }),
        "claimed_op_token" => Ok(ContractEvent::ClaimedTokens {
            investor: as_address(tg(1)?)?,
            operation_id: as_u32(tg(2)?)?,
            amount: as_i128(map_field(as_map(&raw.value)?, "amount")?)?,
        }),
        "refunded" => {
            let m = as_map(&raw.value)?;
            Ok(ContractEvent::Refunded {
                investor: as_address(tg(1)?)?,
                operation_id: as_u32(tg(2)?)?,
                usdc_amount: as_i128(map_field(m, "usdc_amount")?)?,
                shares_refunded: as_i128(map_field(m, "shares_refunded")?)?,
            })
        }
        // soroban-token-sdk `Transfer`: topics [symbol, from, to], data is a
        // Map { to_muxed_id: Option<u64>, amount: i128 } (data_format = "map").
        "transfer" => {
            let m = as_map(&raw.value)?;
            Ok(ContractEvent::OpLendTransfered {
                from: as_address(tg(1)?)?,
                to: as_address(tg(2)?)?,
                amount: as_i128(map_field(m, "amount")?)?,
            })
        }
        // Rewards contract events.
        "claimed" => Ok(ContractEvent::ClaimedRewards {
            op_id: as_u32(tg(1)?)?,
            user: as_address(tg(2)?)?,
            balance: as_i128(map_field(as_map(&raw.value)?, "balance")?)?,
        }),
        "rewards_distributed" => Ok(ContractEvent::RewardsDistributed {
            op_id: as_u32(tg(1)?)?,
            epoch: as_u32(tg(2)?)?,
            amount: as_i128(map_field(as_map(&raw.value)?, "amount")?)?,
        }),
        other => Err(eyre!("unindexed event `{other}`")),
    }
}

/// Decode a raw Soroban event into the `Activity` rows it implies. I/O-free
/// variants go through the pure `build_activity`; `OpCreated` needs a DB op-id
/// lookup and `OpLendTransfered` needs the observed contract's op context.
pub async fn handle_event(
    contract: &ObservableContract,
    fopid_to_opid: &HashMap<i32, Uuid>,
    raw: &RawSorobanEvent,
    operations: &dyn OperationStore,
) -> eyre::Result<Option<Vec<Activity>>> {
    let Ok(event) = decode_event(raw) else {
        return Ok(None);
    };
    let tx_hash = format!("{}#{}", raw.tx_hash, raw.event_index);
    let block_number = raw.ledger_seq;
    let ts = raw.ledger_closed_at;

    match event {
        ContractEvent::OpCreated {
            op_token,
            operation_id,
            total_shares,
        } => {
            let fopid = operation_id as i32;
            let op_id = match fopid_to_opid.get(&fopid) {
                Some(id) => *id,
                None => operations
                    .get_op_id_from_fop_id(fopid)
                    .await
                    .unwrap_or_default(),
            };
            if op_id == Uuid::default() {
                return Ok(None);
            }
            Ok(Some(vec![
                ActivityBuilder::new(
                    ActivityEventType::OpCreated,
                    block_number,
                )
                .block_timestamp(ts)
                .event_hash(format!("{tx_hash}#lend_op_created"))
                .op_id(op_id)
                .factory_op_id(fopid)
                .data(json!({
                    "tx_hash": tx_hash,
                    "op_token": op_token,
                    "total_shares": total_shares.to_string()
                }))
                .build(),
            ]))
        }
        ContractEvent::OpLendTransfered { from, to, amount } => {
            let ignored = get_ignored_addresses();
            if ignored.contains(&from)
                || ignored.contains(&to)
                || contract.op_id.is_none()
                || contract.fop_id.is_none()
            {
                return Ok(None);
            }
            let op_id = contract.op_id.unwrap();
            let fopid = contract.fop_id.unwrap();
            Ok(Some(vec![
                ActivityBuilder::new(
                    ActivityEventType::OpLendTransfered,
                    block_number,
                )
                .event_hash(format!("{tx_hash}#lend_oplend_transferred_from"))
                .block_timestamp(ts)
                .op_id(op_id)
                .factory_op_id(fopid)
                .user_address(Some(from.clone()))
                .data(json!({
                    "tx_hash": tx_hash, "from": from, "to": to,
                    "amount": amount.to_string(), "user_is_sender": true,
                }))
                .build(),
                ActivityBuilder::new(
                    ActivityEventType::OpLendTransfered,
                    block_number,
                )
                .event_hash(format!("{tx_hash}#lend_oplend_transferred_to"))
                .block_timestamp(ts)
                .op_id(op_id)
                .factory_op_id(fopid)
                .user_address(Some(to.clone()))
                .data(json!({
                    "tx_hash": tx_hash, "from": from, "to": to,
                    "amount": amount.to_string(), "user_is_sender": false,
                }))
                .build(),
            ]))
        }
        other => Ok(build_activity(
            fopid_to_opid,
            other,
            &tx_hash,
            block_number,
            ts,
        )),
    }
}
