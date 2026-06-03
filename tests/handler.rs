//! Handler orchestration: `process_events` persists activities and, for an
//! `OperationCreated`, drives `update_operation_total_shares` — the path that
//! seeds `operations.supported_chains` with the new OpLend token so the indexer
//! discovers and starts observing it.

mod common;

use std::sync::Arc;

use common::{FakeActivityStore, FakeFiatHoldingStore, FakeOperationStore};
use serde_json::json;
use uuid::Uuid;

use lend_worker_stellar::handler::Handler;
use lend_worker_stellar::models::activity_model::{
    ActivityBuilder, ActivityEventType,
};

const FOP: i32 = 7;

fn op_created_activity() -> lend_worker_stellar::models::activity_model::Activity
{
    ActivityBuilder::new(ActivityEventType::OpCreated, 12_345)
        .event_hash("deadbeef#0#lend_op_created".into())
        .op_id(Uuid::from_u128(7))
        .factory_op_id(FOP)
        .data(json!({
            "tx_hash": "deadbeef#0",
            "op_token": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
            "total_shares": "1000000"
        }))
        .build()
}

#[tokio::test]
async fn op_created_persists_activity_and_seeds_total_shares() {
    let activity = Arc::new(FakeActivityStore::default());
    // unfinished must be non-empty for the OpCreated branch in sync_op_status to run.
    let operations = Arc::new(FakeOperationStore {
        unfinished: vec![FOP],
        ..Default::default()
    });
    let fiat = Arc::new(FakeFiatHoldingStore::default());

    let mut handler = Handler::with_stores(
        activity.clone(),
        operations.clone(),
        fiat.clone(),
    );

    handler
        .process_events(vec![op_created_activity()])
        .await
        .expect("process_events");

    // The activity was persisted.
    let inserted = activity.inserted.lock().unwrap();
    assert_eq!(inserted.len(), 1);
    assert_eq!(inserted[0].event_type, ActivityEventType::OpCreated);
    assert_eq!(inserted[0].chain_id, 0);

    // OperationCreated seeded total_shares + supported_chains (the OpLend
    // discovery path): update_operation_total_shares was called with the token.
    let calls = operations.total_shares_calls.lock().unwrap();
    assert_eq!(
        calls.len(),
        1,
        "expected one update_operation_total_shares call"
    );
    let (fop, data) = &calls[0];
    assert_eq!(*fop, FOP);
    assert_eq!(
        data["op_token"],
        "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
    );
    assert_eq!(data["total_shares"], "1000000");
}

#[tokio::test]
async fn empty_events_are_a_noop() {
    let activity = Arc::new(FakeActivityStore::default());
    let operations = Arc::new(FakeOperationStore::default());
    let fiat = Arc::new(FakeFiatHoldingStore::default());
    let mut handler = Handler::with_stores(activity.clone(), operations, fiat);

    handler.process_events(vec![]).await.expect("noop");

    assert!(activity.inserted.lock().unwrap().is_empty());
}
