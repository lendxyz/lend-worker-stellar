use async_trait::async_trait;
use chrono::Utc;
use log::error;
use sqlx::Error;
use sqlx::postgres::PgQueryResult;
use sqlx::prelude::FromRow;
use std::collections::HashMap;
use uuid::Uuid;

use lw_config::config::STELLAR_CHAIN_ID;
use lw_domain::activity_model::{
    ActivityEventType, OpCreatedEventData, activity_event_to_funding_status,
};
use lw_domain::op_model::{FundingStatus, Operation};

use super::helpers::{Database, get_database};

/// Persistence operations for `operations` rows.
#[async_trait]
pub trait OperationStore: Send + Sync {
    async fn get_all(&self) -> Result<Vec<Operation>, Error>;
    async fn get_op_id_from_fop_id(&self, fopid: i32) -> Result<Uuid, Error>;
    async fn get_ongoing_operations(&self) -> Result<Vec<i32>, Error>;
    async fn get_unfinished_operations(&self) -> Result<Vec<i32>, Error>;
    async fn update_operation_progress(
        &self,
        updates: &HashMap<i32, OperationProgressUpdate>,
    ) -> Result<PgQueryResult, Error>;
    async fn update_operation_status(
        &self,
        updates: &HashMap<i32, ActivityEventType>,
    ) -> Result<PgQueryResult, Error>;
    async fn update_operation_total_shares(
        &self,
        op_id: i32,
        data: serde_json::Value,
    ) -> Result<PgQueryResult, Error>;
    async fn add_supported_chain(
        &self,
        op_id: i32,
        d: serde_json::Value,
    ) -> Result<PgQueryResult, Error>;
}

/// Postgres-backed [`OperationStore`].
#[derive(Clone)]
pub struct PgOperationStore {
    db: Database,
}

impl PgOperationStore {
    pub fn from_global() -> Self {
        Self { db: get_database() }
    }

    pub fn with_db(db: Database) -> Self {
        Self { db }
    }
}

#[derive(Debug, Clone, FromRow)]
struct ActiveOperationsQuery {
    factory_op_id: i32,
}

#[derive(Debug, Clone, FromRow)]
struct OperationIdQuery {
    op_id: Uuid,
}

#[derive(Debug, Clone)]
pub struct OperationProgressUpdate {
    pub participants: i64,
    pub funded_amount: String,
    pub stellar_funded_amount: String,
}

#[async_trait]
impl OperationStore for PgOperationStore {
    async fn get_all(&self) -> Result<Vec<Operation>, Error> {
        sqlx::query_as::<_, Operation>(
            "SELECT
                id,
                funding_status,
                funding_goal,
                shares_sold,
                stellar_shares_sold,
                funding_participants,
                total_shares,
                stellar_shares,
                supported_chains,
                factory_op_id
            FROM operations
            WHERE factory_op_id IS NOT NULL
            AND published = true",
        )
        .fetch_all(self.db.pool())
        .await
    }

    async fn get_op_id_from_fop_id(&self, fopid: i32) -> Result<Uuid, Error> {
        let data = sqlx::query_as::<_, OperationIdQuery>(
            "SELECT id AS op_id
            FROM operations
            WHERE factory_op_id = ($1)::INT",
        )
        .bind(fopid)
        .fetch_all(self.db.pool())
        .await?;

        if data.to_vec().is_empty() {
            return Ok(Uuid::default());
        }

        Ok(data.to_vec()[0].op_id)
    }

    async fn get_ongoing_operations(&self) -> Result<Vec<i32>, Error> {
        let data = sqlx::query_as::<_, ActiveOperationsQuery>(
            "SELECT factory_op_id
            FROM operations
            WHERE factory_op_id IS NOT NULL
            AND funding_status IN ('open', 'predeposit')
            AND published = true",
        )
        .fetch_all(self.db.pool())
        .await?;

        Ok(data.into_iter().map(|i| i.factory_op_id).collect())
    }

    async fn get_unfinished_operations(&self) -> Result<Vec<i32>, Error> {
        let data = sqlx::query_as::<_, ActiveOperationsQuery>(
            "SELECT factory_op_id
            FROM operations
            WHERE factory_op_id IS NOT NULL
            AND funding_status IN ('open', 'predeposit', 'upcoming')
            AND published = true",
        )
        .fetch_all(self.db.pool())
        .await?;

        Ok(data.into_iter().map(|i| i.factory_op_id).collect())
    }

    async fn update_operation_progress(
        &self,
        updates: &HashMap<i32, OperationProgressUpdate>,
    ) -> Result<PgQueryResult, Error> {
        let pool = self.db.pool();
        let mut tx = pool.begin().await?;

        let factory_op_ids: Vec<i32> = updates.keys().copied().collect();
        let funding_participants: Vec<i64> =
            updates.values().map(|v| v.participants).collect();
        let shares_sold: Vec<String> =
            updates.values().map(|v| v.funded_amount.clone()).collect();
        let shares_sold_stlr: Vec<String> = updates
            .values()
            .map(|v| v.stellar_funded_amount.clone())
            .collect();
        let updated_ats: Vec<chrono::DateTime<chrono::Utc>> =
            vec![Utc::now(); updates.len()];

        let sql = r#"
            WITH data AS (
                SELECT
                    UNNEST($1::INT[]) AS factory_op_id,
                    UNNEST($2::BIGINT[]) AS funding_participants,
                    UNNEST($3::TEXT[]) AS shares_sold,
                    UNNEST($4::TEXT[]) AS stellar_shares_sold,
                    UNNEST($5::TIMESTAMPTZ[]) AS updated_at
            )
            UPDATE operations o
            SET
                funding_participants = d.funding_participants,
                shares_sold = d.shares_sold,
                stellar_shares_sold = d.stellar_shares_sold,
                updated_at = d.updated_at
            FROM data d
            WHERE o.factory_op_id = d.factory_op_id
        "#;

        let res = sqlx::query(sql)
            .bind(&factory_op_ids)
            .bind(funding_participants)
            .bind(shares_sold)
            .bind(shares_sold_stlr)
            .bind(updated_ats)
            .execute(&mut *tx)
            .await?;

        for op in factory_op_ids {
            sqlx::query(r#"SELECT pg_notify($1, $2)"#)
                .bind("op_progress")
                .bind(op.to_string())
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;

        Ok(res)
    }

    async fn update_operation_status(
        &self,
        updates: &HashMap<i32, ActivityEventType>,
    ) -> Result<PgQueryResult, Error> {
        let valid_updates: Vec<(i32, FundingStatus)> = updates
            .iter()
            .filter_map(|(&id, event)| {
                activity_event_to_funding_status(event)
                    .map(|status| (id, status))
            })
            .collect();

        if valid_updates.is_empty() {
            return Ok(PgQueryResult::default()); // or handle as appropriate
        }

        let factory_op_ids: Vec<i32> =
            valid_updates.iter().map(|(id, _)| *id).collect();
        let funding_statuses: Vec<FundingStatus> = valid_updates
            .iter()
            .map(|(_, status)| status.clone())
            .collect();
        let updated_ats: Vec<chrono::DateTime<chrono::Utc>> =
            vec![Utc::now(); valid_updates.len()];

        let pool = self.db.pool();
        let mut tx = pool.begin().await?;

        let sql = r#"
            WITH data AS (
                SELECT
                    UNNEST($1::INT[]) AS factory_op_id,
                    UNNEST($2::funding_status[]) AS funding_status,
                    UNNEST($3::TIMESTAMPTZ[]) AS updated_at
            )
            UPDATE operations o
            SET
                funding_status = d.funding_status,
                updated_at = d.updated_at
            FROM data d
            WHERE o.factory_op_id = d.factory_op_id
        "#;

        let res = sqlx::query(sql)
            .bind(&factory_op_ids)
            .bind(funding_statuses)
            .bind(updated_ats)
            .execute(&mut *tx)
            .await?;

        for op in factory_op_ids {
            sqlx::query(r#"SELECT pg_notify($1, $2)"#)
                .bind("op_progress")
                .bind(op.to_string())
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;

        Ok(res)
    }

    async fn update_operation_total_shares(
        &self,
        op_id: i32,
        d: serde_json::Value,
    ) -> Result<PgQueryResult, Error> {
        let data = match serde_json::from_value::<OpCreatedEventData>(d) {
            Ok(data) => data,
            Err(err) => {
                error!(
                    "[OpRepository::update_operation_total_shares] Failed to deserialize OpCreatedEventData: {err:?}"
                );
                return Err(Error::Protocol(err.to_string()));
            }
        };

        let sql = r#"
            UPDATE operations
            SET supported_chains = COALESCE(supported_chains, '[]'::JSONB)
                    || jsonb_build_object(
                        'op_token', $1::TEXT,
                        'chain_id', $2::INT,
                        'lz_endpoint_id', 0,
                        'primary', NOT (
                            COALESCE(supported_chains, '[]'::JSONB)
                                @> '[{"primary": true}]'::JSONB
                        )
                    ),
                stellar_shares = $3,
                total_shares = (
                    COALESCE(NULLIF(total_shares, ''), '0')::NUMERIC
                        + $3::NUMERIC
                )::TEXT
            WHERE factory_op_id = $4
            AND NOT (
                COALESCE(supported_chains, '[]'::JSONB) @> jsonb_build_array(
                    jsonb_build_object(
                        'op_token', $1::TEXT,
                        'chain_id', $2::INT
                    )
                )
            )
        "#;

        sqlx::query(sql)
            .bind(data.op_token.as_str())
            .bind(STELLAR_CHAIN_ID)
            .bind(data.total_shares.as_str())
            .bind(op_id)
            .execute(self.db.pool())
            .await
    }

    async fn add_supported_chain(
        &self,
        _op_id: i32,
        _d: serde_json::Value,
    ) -> Result<PgQueryResult, Error> {
        // Multi-chain peer tracking is not used on Stellar (single chain).
        Ok(PgQueryResult::default())
    }
}
