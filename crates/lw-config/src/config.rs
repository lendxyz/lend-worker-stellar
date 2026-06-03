use once_cell::sync::Lazy;

pub static ENV: Lazy<LocalEnv> = Lazy::new(LocalEnv::new);

/// Fixed chain id for the Stellar deployment in the shared data model.
pub const STELLAR_CHAIN_ID: i32 = 0;

pub fn get_config() -> LocalEnv {
    ENV.clone()
}

pub fn get_app_env() -> AppEnv {
    ENV.clone().env
}

pub fn is_production() -> bool {
    ENV.env == AppEnv::Production
}

fn get_env(key: &str) -> String {
    std::env::var(key).unwrap_or_default()
}

fn get_env_or(key: &str, default: &str) -> String {
    let v = get_env(key);
    if v.is_empty() { default.to_string() } else { v }
}

/// Stellar account/contract ids we never attribute activity to (the factory
/// itself; empty string guards unset addresses).
pub fn get_ignored_addresses() -> Vec<String> {
    vec![String::new(), get_config().factory_contract_id]
}

#[derive(Debug, Clone, PartialEq)]
pub enum AppEnv {
    Dev,
    Staging,
    Production,
}

#[derive(Debug, Clone)]
pub struct LocalEnv {
    pub db_url: String,
    pub health_check_url: String,
    pub factory_contract_id: String,
    pub fiat_safe_address: String,
    pub soroban_rpc_url: String,
    pub backfill_source_url: String,
    pub start_ledger: i32,
    pub poll_interval_ms: u64,
    pub chain_id: i32,
    pub env: AppEnv,
}

impl Default for LocalEnv {
    fn default() -> Self {
        Self::new()
    }
}

impl AppEnv {
    pub fn get_env_value() -> AppEnv {
        match get_env("APP_ENV").as_str() {
            "development" => AppEnv::Dev,
            "staging" => AppEnv::Staging,
            _ => AppEnv::Production,
        }
    }
}

impl LocalEnv {
    pub fn new() -> Self {
        LocalEnv {
            db_url: get_env("DATABASE_URL"),
            health_check_url: get_env("HEALTH_CHECK_URL"),
            factory_contract_id: get_env("FACTORY_CONTRACT_ID"),
            fiat_safe_address: get_env("FIAT_SAFE_ADDRESS"),
            soroban_rpc_url: get_env_or(
                "SOROBAN_RPC_URL",
                "https://soroban-testnet.stellar.org",
            ),
            backfill_source_url: get_env("BACKFILL_SOURCE_URL"),
            start_ledger: get_env("START_LEDGER").parse().unwrap_or(0),
            poll_interval_ms: get_env("POLL_INTERVAL_MS")
                .parse()
                .unwrap_or(5_000),
            chain_id: STELLAR_CHAIN_ID,
            env: AppEnv::get_env_value(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignored_addresses_contains_factory_id() {
        unsafe {
            std::env::set_var("APP_ENV", "production");
            std::env::set_var("FACTORY_CONTRACT_ID", "CAFACTORY");
        }
        let cfg = LocalEnv::new();
        assert_eq!(cfg.factory_contract_id, "CAFACTORY");
        assert_eq!(cfg.chain_id, 0);
    }
}
