//! `lend_worker_stellar` — the binary crate. It owns process bootstrap
//! ([`bootstrap`]) and a thin facade that re-exports the workspace layer crates
//! under their original module paths.

pub mod bootstrap;

/// Application orchestration — the `Handler` (the `lw-app` crate).
pub use lw_app::handler;

/// Chain indexer (the `lw-chain` crate); `chain_config` re-exported from `lw-config`.
pub mod chain {
    pub use lw_chain::{event_loop, event_source, log_handlers, scval};
    pub use lw_config::chain_config;
}

/// Persistence layer (the `lw-storage` crate).
pub mod repositories {
    pub use lw_storage::{
        activity_repository, fiat_holdings_repository, helpers, op_repository,
    };
}

/// Domain models + business math (the `lw-domain` crate).
pub mod models {
    pub use lw_domain::{activity_model, fiat_holdings, op_model, utils};
}

/// App/env config, native event types, and pure conversions (the `lw-config` crate).
pub mod utils {
    pub use lw_config::{config, types};
}
