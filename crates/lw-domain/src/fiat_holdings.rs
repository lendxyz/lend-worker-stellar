use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct FiatHolding {
    pub id: Uuid,
    pub user_address: String,
    pub value: String,
    pub factory_op_id: i32,
    pub withdrew_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl FiatHolding {
    pub fn new(
        factory_op_id: i32,
        user_address: String,
        value: String,
    ) -> Self {
        Self {
            id: Uuid::default(),
            factory_op_id,
            value,
            user_address,
            withdrew_at: None,
            created_at: Utc::now(),
        }
    }

    pub fn set_created_at(&mut self, new_date: DateTime<Utc>) -> Self {
        self.created_at = new_date;
        self.clone()
    }
}
