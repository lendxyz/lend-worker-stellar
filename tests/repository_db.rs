//! Postgres round-trip tests for the repositories against the real schema.
//! Gated on `TEST_DATABASE_URL`: when unset (e.g. local runs without a DB) the
//! tests no-op so the suite stays green; CI provides a throwaway Postgres.

mod common;

use uuid::Uuid;

use lend_worker_stellar::models::activity_model::{
    ActivityBuilder, ActivityEventType,
};
use lend_worker_stellar::repositories::activity_repository::{
    ActivityStore, PgActivityStore,
};
use lend_worker_stellar::repositories::helpers::Database;
use lend_worker_stellar::repositories::op_repository::{
    OperationStore, PgOperationStore,
};

use serde_json::json;

const SCHEMA: &str = include_str!("sql/schema.sql");
const FOP: i32 = 7;
const OP_TOKEN: &str =
    "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC";

fn test_db_url() -> Option<String> {
    match std::env::var("TEST_DATABASE_URL") {
        Ok(u) if !u.is_empty() => Some(u),
        _ => None,
    }
}

/// Fresh schema + one published operation (id = `op_id`, factory_op_id = FOP).
async fn setup(db: &Database, op_id: Uuid) {
    sqlx::raw_sql(SCHEMA)
        .execute(db.pool())
        .await
        .expect("apply schema");

    sqlx::query(
        "INSERT INTO operations (id, slug, title, published, factory_op_id)
         VALUES ($1, $2, $3, true, $4)",
    )
    .bind(op_id)
    .bind(format!("op-{op_id}"))
    .bind("Test Operation")
    .bind(FOP)
    .execute(db.pool())
    .await
    .expect("insert operation");
}

#[tokio::test]
async fn activity_round_trip_uses_chain_id_zero() {
    let Some(url) = test_db_url() else {
        eprintln!(
            "TEST_DATABASE_URL unset — skipping repository_db round-trip"
        );
        return;
    };
    let _guard = common::db_serial().lock().await;
    let db = Database::connect(&url).await.expect("connect");
    let op_id = Uuid::from_u128(0xA11CE);
    setup(&db, op_id).await;

    let store = PgActivityStore::with_db(db.clone());

    let activity = ActivityBuilder::new(ActivityEventType::Invested, 12_345)
        .event_hash("tx_round_trip#0#lend_invested".into())
        .op_id(op_id)
        .factory_op_id(FOP)
        .user_address(Some(
            "GBZXN7PIRZGNMHGA7MUUUF4GWPY5AYPV6LY4UV2GL6VJGIQRXFDNMADI".into(),
        ))
        .data(json!({ "tx_hash": "tx_round_trip#0", "usdc_amount": "1000", "shares_bought": "50" }))
        .build();

    store.insert_many(&[activity]).await.expect("insert_many");

    // get_factory_latest_block binds chain_id = 0 (the Stellar sentinel); it must
    // see the ledger sequence we just wrote.
    let latest = store
        .get_factory_latest_block()
        .await
        .expect("latest block");
    assert_eq!(
        latest, 12_345,
        "factory latest block should be the ledger seq"
    );
}

#[tokio::test]
async fn operation_total_shares_seeds_deserializable_supported_chains() {
    let Some(url) = test_db_url() else {
        eprintln!(
            "TEST_DATABASE_URL unset — skipping repository_db supported_chains"
        );
        return;
    };
    let _guard = common::db_serial().lock().await;
    let db = Database::connect(&url).await.expect("connect");
    let op_id = Uuid::from_u128(0xB0B);
    setup(&db, op_id).await;

    let ops = PgOperationStore::with_db(db.clone());

    // Resolve fop -> uuid.
    assert_eq!(ops.get_op_id_from_fop_id(FOP).await.unwrap(), op_id);

    // Seed total_shares + supported_chains from an OperationCreated payload.
    ops.update_operation_total_shares(
        FOP,
        json!({ "tx_hash": "tx#0", "op_token": OP_TOKEN, "total_shares": "1000000" }),
    )
    .await
    .expect("update_operation_total_shares");

    // get_all must deserialize supported_chains (incl. lz_endpoint_id=0) — this
    // is the round-trip that the SupportedChains struct/JSON must agree on.
    let all = ops.get_all().await.expect("get_all");
    let op = all
        .iter()
        .find(|o| o.id == op_id)
        .expect("operation present");
    assert_eq!(op.total_shares.as_deref(), Some("1000000"));
    // Stellar-primary seed sets stellar_shares alongside total_shares.
    assert_eq!(op.stellar_shares.as_deref(), Some("1000000"));
    assert_eq!(op.supported_chains.0.len(), 1);
    let sc = &op.supported_chains.0[0];
    assert_eq!(sc.op_token, OP_TOKEN);
    assert_eq!(sc.chain_id, 0);
    assert_eq!(sc.lz_endpoint_id, 0);
    assert!(sc.primary);
}

#[tokio::test]
async fn operation_total_shares_appends_non_primary_when_primary_exists() {
    let Some(url) = test_db_url() else {
        eprintln!(
            "TEST_DATABASE_URL unset — skipping repository_db append path"
        );
        return;
    };
    let _guard = common::db_serial().lock().await;
    let db = Database::connect(&url).await.expect("connect");
    let op_id = Uuid::from_u128(0xCAFE);
    setup(&db, op_id).await;

    let ops = PgOperationStore::with_db(db.clone());

    // First OperationCreated seeds the primary Stellar chain + total_shares.
    ops.update_operation_total_shares(
        FOP,
        json!({ "tx_hash": "tx#0", "op_token": OP_TOKEN, "total_shares": "1000000" }),
    )
    .await
    .expect("seed primary");

    // Second OperationCreated for an op that already has a primary chain:
    // total_shares must NOT change, and the new chain is appended as
    // non-primary instead of overwriting the array.
    const OTHER_TOKEN: &str =
        "CCREATEDSECONDTOKENXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
    ops.update_operation_total_shares(
        FOP,
        json!({ "tx_hash": "tx#1", "op_token": OTHER_TOKEN, "total_shares": "9999999" }),
    )
    .await
    .expect("append non-primary");

    let all = ops.get_all().await.expect("get_all");
    let op = all
        .iter()
        .find(|o| o.id == op_id)
        .expect("operation present");

    // total_shares accumulates across chains (1000000 + 9999999); stellar_shares
    // tracks only the shares created on the appended Stellar chain.
    assert_eq!(op.total_shares.as_deref(), Some("10999999"));
    assert_eq!(op.stellar_shares.as_deref(), Some("9999999"));

    // Original primary entry preserved, new entry appended as non-primary.
    assert_eq!(op.supported_chains.0.len(), 2);

    let primary = &op.supported_chains.0[0];
    assert_eq!(primary.op_token, OP_TOKEN);
    assert!(primary.primary);

    let appended = &op.supported_chains.0[1];
    assert_eq!(appended.op_token, OTHER_TOKEN);
    assert_eq!(appended.chain_id, 0);
    assert_eq!(appended.lz_endpoint_id, 0);
    assert!(!appended.primary);
}
