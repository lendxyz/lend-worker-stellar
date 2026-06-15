//! Golden suite: decode a representative `RawSorobanEvent` for every indexed
//! event type and snapshot the `Activity` rows `handle_event` produces. Locks in
//! the decode + mapping contract (incl. the map-shaped OpLend `transfer`, the
//! regression that previously dropped every transfer).

mod common;

use common::*;
use uuid::Uuid;

use lend_worker_stellar::chain::event_source::RawSorobanEvent;
use lend_worker_stellar::chain::log_handlers::handle_event;
use lend_worker_stellar::models::activity_model::Activity;
use lend_worker_stellar::utils::types::{ContractType, ObservableContract};

const FOP: i32 = 7;

fn op_uuid() -> Uuid {
    Uuid::from_u128(7)
}

fn factory_contract() -> ObservableContract {
    ObservableContract {
        contract_type: ContractType::Factory,
        op_id: None,
        fop_id: None,
        address: FACTORY.into(),
        latest_block: 0,
    }
}

fn oplend_contract() -> ObservableContract {
    ObservableContract {
        contract_type: ContractType::OpLend,
        op_id: Some(op_uuid()),
        fop_id: Some(FOP),
        address: FACTORY.into(),
        latest_block: 0,
    }
}

fn store() -> FakeOperationStore {
    let mut s = FakeOperationStore::default();
    s.fopid_to_uuid.insert(FOP, op_uuid());
    s
}

/// Run `handle_event` and return the activities (panics on Err).
async fn run(
    contract: &ObservableContract,
    raw: &RawSorobanEvent,
) -> Option<Vec<Activity>> {
    let ops = store();
    let mut map = std::collections::HashMap::new();
    map.insert(FOP, op_uuid());
    handle_event(contract, &map, raw, &ops).await.unwrap()
}

macro_rules! golden {
    ($name:ident, $contract:expr, $raw:expr) => {
        #[tokio::test]
        async fn $name() {
            let out = run(&$contract, &$raw).await;
            insta::assert_json_snapshot!(stringify!($name), out);
        }
    };
}

golden!(
    op_created,
    factory_contract(),
    raw_event(
        vec![sym("operation_created"), addr(FACTORY), u32v(FOP as u32)],
        data_map(&[("total_shares", i128v(1_000_000))]),
        FACTORY
    )
);

golden!(
    op_started,
    factory_contract(),
    raw_event(
        vec![sym("operation_started"), u32v(FOP as u32)],
        data_map(&[]),
        FACTORY
    )
);

golden!(
    op_paused,
    factory_contract(),
    raw_event(
        vec![sym("operation_paused"), u32v(FOP as u32)],
        data_map(&[]),
        FACTORY
    )
);

golden!(
    op_resumed,
    factory_contract(),
    raw_event(
        vec![sym("operation_resumed"), u32v(FOP as u32)],
        data_map(&[]),
        FACTORY
    )
);

golden!(
    op_canceled,
    factory_contract(),
    raw_event(
        vec![sym("operation_canceled"), u32v(FOP as u32)],
        data_map(&[]),
        FACTORY
    )
);

golden!(
    op_finished,
    factory_contract(),
    raw_event(
        vec![sym("operation_finished"), u32v(FOP as u32)],
        data_map(&[("amount_raised_euro", i128v(5_000_000))]),
        FACTORY
    )
);

golden!(
    predeposits_open,
    factory_contract(),
    raw_event(
        vec![sym("predeposits_open"), u32v(FOP as u32)],
        data_map(&[]),
        FACTORY
    )
);

golden!(
    predeposits_closed,
    factory_contract(),
    raw_event(
        vec![sym("predeposits_closed"), u32v(FOP as u32)],
        data_map(&[]),
        FACTORY
    )
);

golden!(
    invested,
    factory_contract(),
    raw_event(
        vec![sym("invested"), addr(ACCOUNT_A), u32v(FOP as u32)],
        data_map(&[
            ("usdc_amount", i128v(1_000)),
            ("shares_bought", i128v(50))
        ]),
        FACTORY
    )
);

golden!(
    invested_fiat,
    factory_contract(),
    raw_event(
        vec![
            sym("invested_fiat"),
            addr(ACCOUNT_A),
            addr(ACCOUNT_B),
            u32v(FOP as u32)
        ],
        data_map(&[("shares_bought", i128v(50))]),
        FACTORY
    )
);

golden!(
    claimed_op_token,
    factory_contract(),
    raw_event(
        vec![sym("claimed_op_token"), addr(ACCOUNT_A), u32v(FOP as u32)],
        data_map(&[("amount", i128v(50))]),
        FACTORY
    )
);

golden!(
    refunded,
    factory_contract(),
    raw_event(
        vec![sym("refunded"), addr(ACCOUNT_A), u32v(FOP as u32)],
        data_map(&[
            ("usdc_amount", i128v(1_000)),
            ("shares_refunded", i128v(50))
        ]),
        FACTORY
    )
);

// The map-shaped token transfer: data is Map { to_muxed_id, amount }. Produces
// two activities (sender + receiver).
golden!(
    oplend_transfer,
    oplend_contract(),
    raw_event(
        vec![sym("transfer"), addr(ACCOUNT_A), addr(ACCOUNT_B)],
        data_map(&[
            ("to_muxed_id", stellar_xdr::ScVal::Void),
            ("amount", i128v(99))
        ]),
        FACTORY
    )
);

// ---- Rewards contract events --------------------------------------------

golden!(
    claimed_rewards,
    factory_contract(),
    raw_event(
        vec![sym("claimed"), u32v(FOP as u32), addr(ACCOUNT_A)],
        data_map(&[("balance", i128v(500))]),
        FACTORY
    )
);

golden!(
    claimed_ref_rewards,
    factory_contract(),
    raw_event(
        vec![sym("claimed_ref"), addr(ACCOUNT_A)],
        data_map(&[("balance", i128v(250))]),
        FACTORY
    )
);

golden!(
    rewards_distributed,
    factory_contract(),
    raw_event(
        vec![sym("rewards_distributed"), u32v(FOP as u32), u32v(3)],
        data_map(&[("amount", i128v(1_000))]),
        FACTORY
    )
);

golden!(
    ref_rewards_distributed,
    factory_contract(),
    raw_event(
        vec![sym("ref_rewards_distributed"), u32v(3)],
        data_map(&[("amount", i128v(800))]),
        FACTORY
    )
);

/// A deferred/unindexed event (`Gifted`) decodes to nothing.
#[tokio::test]
async fn gifted_is_unindexed() {
    let raw = raw_event(
        vec![sym("gifted"), addr(ACCOUNT_A), u32v(FOP as u32)],
        data_map(&[("usdc_amount", i128v(1)), ("shares_bought", i128v(1))]),
        FACTORY,
    );
    let out = run(&factory_contract(), &raw).await;
    assert!(
        out.is_none(),
        "Gifted must not produce activities, got {out:?}"
    );
}
