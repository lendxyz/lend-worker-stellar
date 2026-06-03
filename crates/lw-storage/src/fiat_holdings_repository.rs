use async_trait::async_trait;
use sqlx::{Error, postgres::PgQueryResult};

use lw_domain::fiat_holdings::FiatHolding;

use super::helpers::{Database, get_database};

/// Persistence operations for `fiat_holdings` rows.
#[async_trait]
pub trait FiatHoldingStore: Send + Sync {
    async fn insert(
        &self,
        holding: &FiatHolding,
    ) -> Result<PgQueryResult, Error>;
}

/// Postgres-backed [`FiatHoldingStore`].
#[derive(Clone)]
pub struct PgFiatHoldingStore {
    db: Database,
}

impl PgFiatHoldingStore {
    pub fn from_global() -> Self {
        Self { db: get_database() }
    }

    pub fn with_db(db: Database) -> Self {
        Self { db }
    }
}

#[async_trait]
impl FiatHoldingStore for PgFiatHoldingStore {
    async fn insert(
        &self,
        holding: &FiatHolding,
    ) -> Result<PgQueryResult, Error> {
        sqlx::query(
            "INSERT INTO fiat_holdings (
                factory_op_id,
                value,
                user_address,
                created_at
            )
            VALUES ($1, $2, $3, $4)",
        )
        .bind(holding.factory_op_id)
        .bind(&holding.value)
        .bind(holding.user_address.to_string())
        .bind(holding.created_at)
        .execute(self.db.pool())
        .await
    }
}
