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
use sqlx::Row;

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

/// Raw `(supported_chains, total_shares, stellar_shares)` straight from
/// Postgres: reading through `SupportedChains` would hide JSON keys this worker
/// does not model.
async fn raw_shares(db: &Database) -> (serde_json::Value, String, String) {
    let row = sqlx::query(
        "SELECT supported_chains, total_shares, stellar_shares
         FROM operations
         WHERE factory_op_id = $1",
    )
    .bind(FOP)
    .fetch_one(db.pool())
    .await
    .expect("fetch operation");

    (
        row.get::<serde_json::Value, _>("supported_chains"),
        row.get::<Option<String>, _>("total_shares")
            .unwrap_or_default(),
        row.get::<Option<String>, _>("stellar_shares")
            .unwrap_or_default(),
    )
}

/// Another writer (the backend, the EVM worker) owns the primary chain entry:
/// the Stellar OpLend token is appended without rewriting it — unknown keys
/// included — and a replayed `OperationCreated` must neither append twice nor
/// count its shares twice.
#[tokio::test]
async fn operation_total_shares_preserves_foreign_primary_and_is_idempotent() {
    let Some(url) = test_db_url() else {
        eprintln!(
            "TEST_DATABASE_URL unset — skipping repository_db idempotency path"
        );
        return;
    };
    let _guard = common::db_serial().lock().await;
    let db = Database::connect(&url).await.expect("connect");
    let op_id = Uuid::from_u128(0xBEEF);
    setup(&db, op_id).await;

    let evm_primary = json!({
        "op_token": "0x2222222222222222222222222222222222222222",
        "chain_id": 137,
        "lz_endpoint_id": 30109,
        "primary": true,
        // Key `SupportedChains` does not model, so a round trip through it drops it.
        "token_decimals": 18,
    });

    sqlx::query(
        "UPDATE operations
         SET supported_chains = $1, total_shares = $2
         WHERE factory_op_id = $3",
    )
    .bind(json!([evm_primary]))
    .bind("500")
    .bind(FOP)
    .execute(db.pool())
    .await
    .expect("seed foreign primary");

    let ops = PgOperationStore::with_db(db.clone());
    let event = json!({
        "tx_hash": "tx#0",
        "op_token": OP_TOKEN,
        "total_shares": "1000000",
    });

    // The same event twice: worker restart / ledger re-scan replays it.
    for _ in 0..2 {
        ops.update_operation_total_shares(FOP, event.clone())
            .await
            .expect("record stellar chain");
    }

    let (chains, total_shares, stellar_shares) = raw_shares(&db).await;
    let chains = chains.as_array().expect("supported_chains array").clone();

    assert_eq!(chains.len(), 2, "replay must not append twice: {chains:?}");
    assert_eq!(
        chains[0], evm_primary,
        "foreign primary entry was rewritten"
    );

    assert_eq!(chains[1]["op_token"], json!(OP_TOKEN));
    assert_eq!(chains[1]["chain_id"], json!(0));
    assert_eq!(chains[1]["lz_endpoint_id"], json!(0));
    assert_eq!(chains[1]["primary"], json!(false));

    // 500 on the other chain + 1000000 on Stellar, counted once (a replayed
    // event would land on 2000500).
    assert_eq!(total_shares, "1000500");
    assert_eq!(stellar_shares, "1000000");
}

/// `total_shares` is the cross-chain total: seeding Stellar as the primary chain
/// folds the event's shares in instead of overwriting what is already stored.
#[tokio::test]
async fn operation_total_shares_accumulates_onto_existing_total() {
    let Some(url) = test_db_url() else {
        eprintln!(
            "TEST_DATABASE_URL unset — skipping repository_db accumulate path"
        );
        return;
    };
    let _guard = common::db_serial().lock().await;
    let db = Database::connect(&url).await.expect("connect");
    let op_id = Uuid::from_u128(0xF00D);
    setup(&db, op_id).await;

    // Shares already recorded for this operation, no chain entries yet.
    sqlx::query(
        "UPDATE operations SET total_shares = $1 WHERE factory_op_id = $2",
    )
    .bind("250")
    .bind(FOP)
    .execute(db.pool())
    .await
    .expect("seed total_shares");

    let ops = PgOperationStore::with_db(db.clone());
    ops.update_operation_total_shares(
        FOP,
        json!({ "tx_hash": "tx#0", "op_token": OP_TOKEN, "total_shares": "1000000" }),
    )
    .await
    .expect("seed primary");

    let (chains, total_shares, stellar_shares) = raw_shares(&db).await;

    assert_eq!(chains.as_array().expect("array").len(), 1);
    assert_eq!(chains[0]["op_token"], json!(OP_TOKEN));
    assert_eq!(chains[0]["primary"], json!(true));

    assert_eq!(total_shares, "1000250");
    assert_eq!(stellar_shares, "1000000");
}

/// Lost-update regression: another writer commits the primary chain entry while
/// this statement already waits on the row. The append must land on top of that
/// entry instead of writing back the array as it looked beforehand.
#[tokio::test]
async fn operation_total_shares_survives_concurrent_primary_write() {
    let Some(url) = test_db_url() else {
        eprintln!(
            "TEST_DATABASE_URL unset — skipping repository_db concurrency path"
        );
        return;
    };
    let _guard = common::db_serial().lock().await;
    let db = Database::connect(&url).await.expect("connect");
    let op_id = Uuid::from_u128(0xC0FFEE);
    setup(&db, op_id).await;

    // Competing writer: holds the primary chain entry uncommitted, so the
    // repository call blocks on the row lock instead of racing it.
    let mut other = db.pool().begin().await.expect("begin");
    sqlx::query(
        "UPDATE operations
         SET supported_chains = jsonb_build_array(jsonb_build_object(
                 'op_token', '0xhome',
                 'chain_id', 137,
                 'lz_endpoint_id', 30109,
                 'primary', true
             ))
         WHERE factory_op_id = $1",
    )
    .bind(FOP)
    .execute(&mut *other)
    .await
    .expect("stage primary");

    let ops = PgOperationStore::with_db(db.clone());
    let writer = tokio::spawn(async move {
        ops.update_operation_total_shares(
            FOP,
            json!({ "tx_hash": "tx#0", "op_token": OP_TOKEN, "total_shares": "1000000" }),
        )
        .await
        .expect("record stellar chain");
    });

    // Give the repository statement time to reach the lock, then release it.
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    other.commit().await.expect("commit primary");
    writer.await.expect("writer task");

    let (chains, total_shares, stellar_shares) = raw_shares(&db).await;
    let chains = chains.as_array().expect("array").clone();

    assert_eq!(
        chains.len(),
        2,
        "concurrent primary entry was clobbered: {chains:?}"
    );
    assert_eq!(chains[0]["op_token"], json!("0xhome"));
    assert_eq!(chains[0]["primary"], json!(true));
    assert_eq!(chains[1]["op_token"], json!(OP_TOKEN));
    assert_eq!(chains[1]["primary"], json!(false));

    assert_eq!(total_shares, "1000000");
    assert_eq!(stellar_shares, "1000000");
}
