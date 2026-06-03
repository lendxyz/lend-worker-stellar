//! Configuration and chain primitives: app/env config, the on-chain network
//! registry and provider construction, the `sol!`-generated contract types,
//! and small pure conversions. This crate is a dependency-free leaf (only
//! external crates) — nothing here reaches into storage, the chain indexer, or
//! the handler.

pub mod chain_config;
pub mod config;
pub mod types;
