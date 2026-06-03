//! Application orchestration: the `Handler` that bootstraps op/contract state,
//! drives the WebSocket manager, and processes incoming `Activity` batches
//! (persisting them and syncing operation/order state). Depends on every lower
//! layer (`lw-chain`, `lw-storage`, `lw-domain`, `lw-config`) and holds its
//! stores as `Arc<dyn …Store>` so it can be unit-tested with fakes.

pub mod handler;
