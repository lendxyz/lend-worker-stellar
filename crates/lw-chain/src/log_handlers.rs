use eyre::eyre;
use serde_json::json;
use std::collections::HashMap;
use uuid::Uuid;

use lw_config::config::{AppEnv, get_config, get_ignored_addresses};
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
        "OperationCreated" => Ok(ContractEvent::OpCreated {
            op_token: as_address(tg(1)?)?,
            operation_id: as_u32(tg(2)?)?,
            total_shares: as_i128(map_field(
                as_map(&raw.value)?,
                "total_shares",
            )?)?,
        }),
        "OperationStarted" => Ok(ContractEvent::OpStarted {
            operation_id: as_u32(tg(1)?)?,
        }),
        "OperationCanceled" => Ok(ContractEvent::OpCanceled {
            operation_id: as_u32(tg(1)?)?,
        }),
        "OperationPaused" => Ok(ContractEvent::OpPaused {
            operation_id: as_u32(tg(1)?)?,
        }),
        "OperationResumed" => Ok(ContractEvent::OpResumed {
            operation_id: as_u32(tg(1)?)?,
        }),
        "OperationFinished" => Ok(ContractEvent::OpFinished {
            operation_id: as_u32(tg(1)?)?,
            amount_raised_euro: as_i128(map_field(
                as_map(&raw.value)?,
                "amount_raised_euro",
            )?)?,
        }),
        "PredepositsOpen" => Ok(ContractEvent::OpPredepositsOpen {
            operation_id: as_u32(tg(1)?)?,
        }),
        "PredepositsClosed" => Ok(ContractEvent::OpPredepositsClosed {
            operation_id: as_u32(tg(1)?)?,
        }),
        "Invested" => {
            let m = as_map(&raw.value)?;
            Ok(ContractEvent::Invested {
                investor: as_address(tg(1)?)?,
                operation_id: as_u32(tg(2)?)?,
                usdc_amount: as_i128(map_field(m, "usdc_amount")?)?,
                shares_bought: as_i128(map_field(m, "shares_bought")?)?,
            })
        }
        "InvestedFiat" => Ok(ContractEvent::InvestedFiat {
            investor: as_address(tg(1)?)?,
            oplend_destination: as_address(tg(2)?)?,
            operation_id: as_u32(tg(3)?)?,
            shares_bought: as_i128(map_field(
                as_map(&raw.value)?,
                "shares_bought",
            )?)?,
        }),
        "ClaimedOpToken" => Ok(ContractEvent::ClaimedTokens {
            investor: as_address(tg(1)?)?,
            operation_id: as_u32(tg(2)?)?,
            amount: as_i128(map_field(as_map(&raw.value)?, "amount")?)?,
        }),
        "Refunded" => {
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
            if get_config().env != AppEnv::Production
                && operation_id <= 10
                && raw.contract_id != get_config().factory_contract_id
            {
                return Ok(None);
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::Utc;
    use std::str::FromStr;
    use stellar_xdr::{
        Int128Parts, ScAddress, ScMap, ScMapEntry, ScSymbol, ScVal,
    };

    fn sym(s: &str) -> ScVal {
        ScVal::Symbol(ScSymbol(s.try_into().unwrap()))
    }
    fn i128v(n: i128) -> ScVal {
        ScVal::I128(Int128Parts {
            hi: (n >> 64) as i64,
            lo: n as u64,
        })
    }
    fn addr(strkey: &str) -> ScVal {
        ScVal::Address(ScAddress::from_str(strkey).unwrap())
    }
    fn data_map(pairs: &[(&str, ScVal)]) -> ScVal {
        let entries: Vec<ScMapEntry> = pairs
            .iter()
            .map(|(k, v)| ScMapEntry {
                key: sym(k),
                val: v.clone(),
            })
            .collect();
        ScVal::Map(Some(ScMap(entries.try_into().unwrap())))
    }

    const CONTRACT: &str =
        "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC";
    const ACCOUNT_A: &str =
        "GBZXN7PIRZGNMHGA7MUUUF4GWPY5AYPV6LY4UV2GL6VJGIQRXFDNMADI";
    // NB: the originally-supplied ACCOUNT_B strkey had an invalid checksum and
    // was rejected by `ScAddress::from_str`; replaced with a valid G-address.
    const ACCOUNT_B: &str =
        "GADQOBYHA4DQOBYHA4DQOBYHA4DQOBYHA4DQOBYHA4DQOBYHA4DQOZPI";

    fn raw(
        topics: Vec<ScVal>,
        value: ScVal,
        contract_id: &str,
    ) -> RawSorobanEvent {
        RawSorobanEvent {
            tx_hash: "tx".into(),
            event_index: 0,
            contract_id: contract_id.into(),
            topics,
            value,
            ledger_seq: 100,
            ledger_closed_at: Utc::now(),
        }
    }

    #[test]
    fn decodes_operation_finished() {
        let r = raw(
            vec![sym("OperationFinished"), ScVal::U32(42)],
            data_map(&[("amount_raised_euro", i128v(5_000_000))]),
            CONTRACT,
        );
        assert_eq!(
            decode_event(&r).unwrap(),
            ContractEvent::OpFinished {
                operation_id: 42,
                amount_raised_euro: 5_000_000
            }
        );
    }

    #[test]
    fn decodes_invested() {
        let r = raw(
            vec![sym("Invested"), addr(ACCOUNT_A), ScVal::U32(3)],
            data_map(&[
                ("usdc_amount", i128v(1000)),
                ("shares_bought", i128v(50)),
            ]),
            CONTRACT,
        );
        match decode_event(&r).unwrap() {
            ContractEvent::Invested {
                operation_id,
                usdc_amount,
                shares_bought,
                ..
            } => {
                assert_eq!(
                    (operation_id, usdc_amount, shares_bought),
                    (3, 1000, 50)
                );
            }
            o => panic!("unexpected {o:?}"),
        }
    }

    #[test]
    fn decodes_transfer_map_data() {
        // soroban-token-sdk Transfer: data is a Map { to_muxed_id, amount }.
        let value =
            data_map(&[("amount", i128v(250)), ("to_muxed_id", ScVal::Void)]);
        let r = raw(
            vec![sym("transfer"), addr(ACCOUNT_A), addr(ACCOUNT_B)],
            value,
            CONTRACT,
        );
        match decode_event(&r).unwrap() {
            ContractEvent::OpLendTransfered { amount, .. } => {
                assert_eq!(amount, 250)
            }
            o => panic!("unexpected {o:?}"),
        }
    }

    #[test]
    fn unindexed_event_errors() {
        let r = raw(
            vec![sym("Gifted"), addr(ACCOUNT_A), ScVal::U32(1)],
            i128v(1),
            CONTRACT,
        );
        assert!(decode_event(&r).is_err());
    }

    // Minimal fake: only get_op_id_from_fop_id is exercised (by OpCreated); the
    // transfer path calls no store methods.
    #[derive(Default)]
    struct NoopOps;
    #[async_trait]
    impl OperationStore for NoopOps {
        async fn get_all(
            &self,
        ) -> Result<Vec<lw_domain::op_model::Operation>, sqlx::Error> {
            unimplemented!()
        }
        async fn get_op_id_from_fop_id(
            &self,
            _f: i32,
        ) -> Result<Uuid, sqlx::Error> {
            Ok(Uuid::default())
        }
        async fn get_ongoing_operations(
            &self,
        ) -> Result<Vec<i32>, sqlx::Error> {
            unimplemented!()
        }
        async fn get_unfinished_operations(
            &self,
        ) -> Result<Vec<i32>, sqlx::Error> {
            unimplemented!()
        }
        async fn update_operation_progress(
            &self,
            _u: &std::collections::HashMap<
                i32,
                lw_storage::op_repository::OperationProgressUpdate,
            >,
        ) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
            unimplemented!()
        }
        async fn update_operation_status(
            &self,
            _u: &std::collections::HashMap<i32, ActivityEventType>,
        ) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
            unimplemented!()
        }
        async fn update_operation_total_shares(
            &self,
            _o: i32,
            _d: serde_json::Value,
        ) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
            unimplemented!()
        }
        async fn add_supported_chain(
            &self,
            _o: i32,
            _d: serde_json::Value,
        ) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn transfer_yields_two_activities() {
        let contract = ObservableContract {
            contract_type: lw_config::types::ContractType::OpLend,
            op_id: Some(Uuid::from_u128(1)),
            fop_id: Some(5),
            address: CONTRACT.into(),
            latest_block: 0,
        };
        let value =
            data_map(&[("amount", i128v(99)), ("to_muxed_id", ScVal::Void)]);
        let r = raw(
            vec![sym("transfer"), addr(ACCOUNT_A), addr(ACCOUNT_B)],
            value,
            CONTRACT,
        );
        let out = handle_event(&contract, &HashMap::new(), &r, &NoopOps)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out.len(), 2);
        assert!(out[0].data["user_is_sender"].as_bool().unwrap());
        assert!(!out[1].data["user_is_sender"].as_bool().unwrap());
    }
}
