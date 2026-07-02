use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{Error, postgres::PgQueryResult, prelude::FromRow};
use std::collections::HashMap;
use uuid::Uuid;

use lw_config::config::STELLAR_CHAIN_ID;
use lw_domain::activity_model::{
    Activity, ActivityEventType, activity_event_priority,
};

use super::helpers::{Database, get_database};

/// Persistence operations for `activity` rows. Implemented by
/// [`PgActivityStore`] for production and by fakes in tests.
#[async_trait]
pub trait ActivityStore: Send + Sync {
    async fn insert(&self, activity: &Activity)
    -> Result<PgQueryResult, Error>;
    async fn insert_many(
        &self,
        activities: &[Activity],
    ) -> Result<PgQueryResult, Error>;
    async fn get_oplend_latest_blocks(&self, op_id: Uuid)
    -> Result<i32, Error>;
    async fn get_rewards_latest_blocks(&self) -> Result<i32, Error>;
    async fn get_factory_latest_block(&self) -> Result<i32, Error>;
    async fn get_total_invested_amounts(
        &self,
        op_ids: Vec<i32>,
    ) -> Result<HashMap<i32, InvestedTotals>, Error>;
    async fn get_total_refunded_amounts(
        &self,
        op_ids: Vec<i32>,
    ) -> Result<HashMap<i32, RefundedTotals>, Error>;
    async fn get_total_stellar_invested_amounts(
        &self,
        op_ids: Vec<i32>,
    ) -> Result<HashMap<i32, InvestedTotals>, Error>;
    async fn get_total_stellar_refunded_amounts(
        &self,
        op_ids: Vec<i32>,
    ) -> Result<HashMap<i32, RefundedTotals>, Error>;
    async fn get_operation_participants(
        &self,
        op_ids: Vec<i32>,
    ) -> Result<HashMap<i32, i64>, Error>;
    async fn get_operation_status_history(
        &self,
        op_ids: Vec<i32>,
    ) -> Result<HashMap<i32, ActivityEventType>, Error>;
}

/// Postgres-backed [`ActivityStore`].
#[derive(Clone)]
pub struct PgActivityStore {
    db: Database,
}

impl PgActivityStore {
    pub fn from_global() -> Self {
        Self { db: get_database() }
    }

    pub fn with_db(db: Database) -> Self {
        Self { db }
    }
}

#[derive(Debug, Clone, FromRow)]
struct LatestActivityBlocks {
    max_block_number: i32,
}

#[derive(Debug, Clone, FromRow)]
pub struct InvestedTotals {
    pub factory_op_id: i32,
    pub total_usdc_amount: String,
    pub total_shares_bought: String,
}

#[derive(Debug, Clone, FromRow)]
pub struct RefundedTotals {
    pub factory_op_id: i32,
    pub total_usdc_amount: String,
    pub total_shares_refunded: String,
}

#[derive(Debug, Clone, FromRow)]
pub struct OperationParticipants {
    pub factory_op_id: i32,
    pub unique_user_count: i64,
}

#[derive(Debug, Clone, FromRow)]
pub struct OperationStatus {
    pub factory_op_id: i32,
    pub event_type: ActivityEventType,
}

#[async_trait]
impl ActivityStore for PgActivityStore {
    async fn insert(
        &self,
        activity: &Activity,
    ) -> Result<PgQueryResult, Error> {
        sqlx::query(
            "INSERT INTO activity (
                op_id,
                factory_op_id,
                event_hash,
                chain_id,
                event_type,
                user_address,
                block_number,
                data
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::JSONB)",
        )
        .bind(activity.op_id)
        .bind(activity.factory_op_id)
        .bind(&activity.event_hash)
        .bind(activity.chain_id)
        .bind(&activity.event_type)
        .bind(activity.user_address.clone().unwrap_or_default())
        .bind(activity.block_number)
        .bind(activity.data.to_string())
        .execute(self.db.pool())
        .await
    }

    async fn get_oplend_latest_blocks(
        &self,
        op_id: Uuid,
    ) -> Result<i32, Error> {
        let sql = r#"
            SELECT MAX(block_number)
            AS max_block_number
            FROM activity
            WHERE chain_id = $1
            AND op_id = $2
            AND event_type IN ('op_lend_bridged', 'op_lend_transfered')
        "#;

        match sqlx::query_as::<_, LatestActivityBlocks>(sql)
            .bind(STELLAR_CHAIN_ID)
            .bind(op_id)
            .fetch_all(self.db.pool())
            .await
        {
            Ok(res) => Ok(res[0].max_block_number),
            Err(_) => Ok(0),
        }
    }

    async fn get_rewards_latest_blocks(&self) -> Result<i32, Error> {
        let sql = r#"
            SELECT MAX(block_number)
            AS max_block_number
            FROM activity
            WHERE chain_id = $1
            AND event_type IN ('claimed_rewards', 'rewards_distributed')
        "#;

        match sqlx::query_as::<_, LatestActivityBlocks>(sql)
            .bind(STELLAR_CHAIN_ID)
            .fetch_all(self.db.pool())
            .await
        {
            Ok(res) => Ok(res[0].max_block_number),
            Err(_) => Ok(0),
        }
    }

    async fn get_factory_latest_block(&self) -> Result<i32, Error> {
        let sql = r#"
            SELECT MAX(block_number)
            AS max_block_number
            FROM activity
            WHERE chain_id = $1
            AND event_type IN (
                'invested',
                'invested_fiat',
                'refunded',
                'op_lend_peer_added',
                'op_predeposits_open',
                'op_predeposits_closed',
                'claimed_op_token',
                'op_created',
                'op_paused',
                'op_resumed',
                'op_started',
                'op_canceled',
                'op_finished'
            )
        "#;

        match sqlx::query_as::<_, LatestActivityBlocks>(sql)
            .bind(STELLAR_CHAIN_ID)
            .fetch_all(self.db.pool())
            .await
        {
            Ok(res) => Ok(res[0].max_block_number),
            Err(_) => Ok(0),
        }
    }

    async fn get_total_invested_amounts(
        &self,
        op_ids: Vec<i32>,
    ) -> Result<HashMap<i32, InvestedTotals>, Error> {
        let sql = r#"
            SELECT
                factory_op_id,
                COALESCE(SUM((data ->> 'usdc_amount')  ::BIGINT), 0)::TEXT AS total_usdc_amount,
                COALESCE(SUM((data ->> 'shares_bought')::BIGINT), 0)::TEXT AS total_shares_bought
            FROM activity
            WHERE event_type = 'invested' AND factory_op_id = ANY($1::INT[])
            GROUP BY factory_op_id
        "#;
        let rows = sqlx::query_as::<_, InvestedTotals>(sql)
            .bind(op_ids)
            .fetch_all(self.db.pool())
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| (row.factory_op_id, row))
            .collect())
    }

    async fn get_total_refunded_amounts(
        &self,
        op_ids: Vec<i32>,
    ) -> Result<HashMap<i32, RefundedTotals>, Error> {
        let sql = r#"
            SELECT
                factory_op_id,
                COALESCE(SUM((data ->> 'usdc_amount')  ::BIGINT), 0)::TEXT AS total_usdc_amount,
                COALESCE(SUM((data ->> 'shares_refunded')::BIGINT), 0)::TEXT AS total_shares_refunded
            FROM activity
            WHERE event_type = 'refunded' AND factory_op_id = ANY($1::INT[])
            GROUP BY factory_op_id
        "#;

        let rows = sqlx::query_as::<_, RefundedTotals>(sql)
            .bind(op_ids)
            .fetch_all(self.db.pool())
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| (row.factory_op_id, row))
            .collect())
    }

    async fn get_total_stellar_invested_amounts(
        &self,
        op_ids: Vec<i32>,
    ) -> Result<HashMap<i32, InvestedTotals>, Error> {
        let sql = r#"
            SELECT
                factory_op_id,
                COALESCE(SUM((data ->> 'usdc_amount')  ::BIGINT), 0)::TEXT AS total_usdc_amount,
                COALESCE(SUM((data ->> 'shares_bought')::BIGINT), 0)::TEXT AS total_shares_bought
            FROM activity
            WHERE event_type = 'invested' AND factory_op_id = ANY($1::INT[]) AND chain_id = $2
            GROUP BY factory_op_id
        "#;
        let rows = sqlx::query_as::<_, InvestedTotals>(sql)
            .bind(op_ids)
            .bind(STELLAR_CHAIN_ID)
            .fetch_all(self.db.pool())
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| (row.factory_op_id, row))
            .collect())
    }

    async fn get_total_stellar_refunded_amounts(
        &self,
        op_ids: Vec<i32>,
    ) -> Result<HashMap<i32, RefundedTotals>, Error> {
        let sql = r#"
            SELECT
                factory_op_id,
                COALESCE(SUM((data ->> 'usdc_amount')  ::BIGINT), 0)::TEXT AS total_usdc_amount,
                COALESCE(SUM((data ->> 'shares_refunded')::BIGINT), 0)::TEXT AS total_shares_refunded
            FROM activity
            WHERE event_type = 'refunded' AND factory_op_id = ANY($1::INT[]) AND chain_id = $2
            GROUP BY factory_op_id
        "#;

        let rows = sqlx::query_as::<_, RefundedTotals>(sql)
            .bind(op_ids)
            .bind(STELLAR_CHAIN_ID)
            .fetch_all(self.db.pool())
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| (row.factory_op_id, row))
            .collect())
    }

    async fn get_operation_participants(
        &self,
        op_ids: Vec<i32>,
    ) -> Result<HashMap<i32, i64>, Error> {
        let sql = r#"
            SELECT factory_op_id, COUNT(DISTINCT user_address) AS unique_user_count
            FROM activity
            WHERE user_address IS NOT NULL
            AND factory_op_id = ANY($1::INT[]) AND event_type = 'invested'
            GROUP BY factory_op_id
        "#;

        let rows = sqlx::query_as::<_, OperationParticipants>(sql)
            .bind(op_ids)
            .fetch_all(self.db.pool())
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| (row.factory_op_id, row.unique_user_count))
            .collect())
    }

    async fn get_operation_status_history(
        &self,
        op_ids: Vec<i32>,
    ) -> Result<HashMap<i32, ActivityEventType>, Error> {
        let sql = r#"
            SELECT DISTINCT factory_op_id, event_type
            FROM activity
            WHERE factory_op_id = ANY($1::INT[])
            AND event_type IN (
                'op_created',
                'op_predeposits_open',
                'op_paused',
                'op_resumed',
                'op_canceled',
                'op_started',
                'op_finished'
            )
        "#;

        let rows = sqlx::query_as::<_, OperationStatus>(sql)
            .bind(op_ids)
            .fetch_all(self.db.pool())
            .await?;

        let mut res: HashMap<i32, ActivityEventType> = HashMap::new();

        for row in rows {
            res.entry(row.factory_op_id)
                .and_modify(|existing| {
                    if activity_event_priority(&row.event_type)
                        > activity_event_priority(existing)
                    {
                        *existing = row.event_type.clone();
                    }
                })
                .or_insert(row.event_type);
        }

        Ok(res)
    }

    async fn insert_many(
        &self,
        activities: &[Activity],
    ) -> Result<PgQueryResult, Error> {
        let op_ids: Vec<Uuid> = activities.iter().map(|a| a.op_id).collect();
        let factory_op_ids: Vec<i32> =
            activities.iter().map(|a| a.factory_op_id).collect();
        let event_hashes: Vec<&str> =
            activities.iter().map(|a| a.event_hash.as_str()).collect();
        let chain_ids: Vec<i32> =
            activities.iter().map(|a| a.chain_id).collect();
        let event_types: Vec<String> = activities
            .iter()
            .map(|a| a.event_type.to_string())
            .collect();
        let user_addresses: Vec<Option<String>> =
            activities.iter().map(|a| a.user_address.clone()).collect();
        let block_numbers: Vec<i32> =
            activities.iter().map(|a| a.block_number).collect();
        let data: Vec<String> =
            activities.iter().map(|a| a.data.to_string()).collect();
        let creation_dates: Vec<DateTime<Utc>> =
            activities.iter().map(|a| a.created_at).collect();

        let sql = r#"
            INSERT INTO activity (
                op_id,
                factory_op_id,
                event_hash,
                chain_id,
                event_type,
                user_address,
                block_number,
                data,
                created_at,
                updated_at
            )
            SELECT * FROM UNNEST(
                $1::UUID[], -- op_ids
                $2::BIGINT[], -- fop_ids
                $3::TEXT[], -- event_hashes
                $4::INT[], -- chain_ids
                $5::activity_event_type[], -- events_types
                $6::TEXT[], -- user_addresses
                $7::BIGINT[], -- block_numbers
                $8::JSONB[], -- data
                $9::TIMESTAMPTZ[], -- created_at
                $9::TIMESTAMPTZ[] -- updated_at
            )
        "#;

        sqlx::query(sql)
            .bind(op_ids)
            .bind(factory_op_ids)
            .bind(event_hashes)
            .bind(chain_ids)
            .bind(event_types)
            .bind(user_addresses)
            .bind(block_numbers)
            .bind(data)
            .bind(creation_dates)
            .execute(self.db.pool())
            .await
    }
}
