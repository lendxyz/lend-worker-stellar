use once_cell::sync::OnceCell;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::sync::Arc;
use std::time::Duration;

use lw_config::config::get_config;

/// An injectable handle to the Postgres connection pool.
///
/// This is the database seam: it owns pool construction in one place and is the
/// type that stores/repositories will depend on (Phase 3). During the migration
/// the process-wide instance is still reachable via [`get_db`], so existing
/// repository code keeps working unchanged.
#[derive(Clone)]
pub struct Database {
    pool: Arc<PgPool>,
}

impl Database {
    /// Build a pool for `url`. The single construction site for pool tuning.
    pub async fn connect(url: &str) -> eyre::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(50)
            .max_lifetime(Duration::from_secs(60 * 60))
            .connect(url)
            .await?;

        Ok(Self {
            pool: Arc::new(pool),
        })
    }

    /// Wrap an already-built pool (e.g. tests that own their pool).
    pub fn from_pool(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    /// Borrow the underlying pool for query execution.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

static DB: OnceCell<Database> = OnceCell::new();

/// Install the process-wide database handle. Used by [`setup_db`] and by tests
/// that construct their own [`Database`] (no env hijacking required).
pub fn init_database(db: Database) {
    DB.set(db).unwrap_or_else(|_| {
        panic!("Database was already set");
    });
}

/// Construct and install the process-wide pool from the app config.
pub async fn setup_db() {
    let db = Database::connect(&get_config().db_url)
        .await
        .expect("Failed to create DB pool");

    init_database(db);
}

/// The process-wide database handle.
pub fn get_database() -> Database {
    DB.get()
        .expect("DB not initialized. Call setup_db() first.")
        .clone()
}

/// Back-compat shim: the pool behind the process-wide handle. Repository code
/// uses this until it takes an injected `&Database` (Phase 3).
pub fn get_db() -> Arc<PgPool> {
    get_database().pool.clone()
}
