//! Domain model: the persisted entity types (`Activity`, `Operation`,
//! `DexOrder`, `FiatHolding`), the activity event taxonomy, and the pure
//! business math (`compute_order_fill`, `resolve_order_status`,
//! `net_funded_shares`). Depends only on `lw-config` (for the network defaults)
//! — never on storage, the chain indexer, or the handler.

pub mod activity_builder;
pub mod activity_model;
pub mod fiat_holdings;
pub mod op_model;
pub mod utils;
