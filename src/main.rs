use itertools::Itertools;
use lend_worker_stellar::{
    bootstrap::{setup_env, start_heartbeat},
    handler::Handler,
};
use log::info;
use std::env::args;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    setup_env().await;

    if args().contains(&"--debug".to_string()) {
        info!("Running debug hook...");
        let _ = debug().await;
        return Ok(());
    }

    tokio::spawn(async move {
        start_heartbeat().await;
    });

    Handler::new().run().await
}

// Debug hook - put some arbitrary logic here and use the --debug flag to run
pub async fn debug() -> eyre::Result<()> {
    Ok(())
}
