//! Process bootstrap: logging, environment loading, and wiring the database.
//! This is application-startup orchestration, so it lives in
//! the binary's crate rather than in the pure `lw-config` crate.

use std::time::Duration;

use fern::colors::{Color, ColoredLevelConfig};
use log::{LevelFilter, error, info};

use crate::repositories::helpers::setup_db;
use crate::utils::config::{get_app_env, get_config, is_production};

fn setup_logger() -> eyre::Result<()> {
    let colors = ColoredLevelConfig {
        trace: Color::Cyan,
        debug: Color::Magenta,
        info: Color::Green,
        warn: Color::Yellow,
        error: Color::Red,
    };

    fern::Dispatch::new()
        .format(move |out, message, record| {
            out.finish(format_args!(
                "{}[{}] {}",
                chrono::Local::now().format("[%H:%M:%S]"),
                colors.color(record.level()),
                message
            ))
        })
        .chain(std::io::stdout())
        .level(log::LevelFilter::Error)
        .level_for("lend_worker_stellar", LevelFilter::Info)
        .level_for("lw_app", LevelFilter::Info)
        .level_for("lw_chain", LevelFilter::Info)
        .level_for("lw_config", LevelFilter::Info)
        .level_for("lw_domain", LevelFilter::Info)
        .level_for("lw_storage", LevelFilter::Info)
        .apply()?;

    Ok(())
}

pub async fn setup_env() {
    dotenv::dotenv().ok();
    setup_logger().unwrap();

    info!("[setup] Starting lend worker...");

    setup_db().await;

    info!("[setup] App env: {:?}", get_app_env());
}

pub async fn start_heartbeat() {
    let config = get_config();
    if !is_production() || config.health_check_url.is_empty() {
        info!("[setup] health check disabled");
        return;
    }

    let client = reqwest::Client::new();
    let mut interval = tokio::time::interval(Duration::from_secs(120));

    loop {
        interval.tick().await;

        match client.get(config.health_check_url.clone()).send().await {
            Ok(res) => {
                if !res.status().is_success() {
                    error!(
                        "[healthcheck] Heartbeat request failed with status: {}",
                        res.status()
                    );
                }
            }
            Err(e) => error!("[healthcheck] Heartbeat request failed: {e}"),
        }
    }
}
