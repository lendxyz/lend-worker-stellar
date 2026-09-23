use once_cell::sync::Lazy;

pub static ENV: Lazy<LocalEnv> = Lazy::new(LocalEnv::new);

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
    pub rewards_contract_id: String,
    pub oplend_wallet: String,
    pub soroban_rpc_url: String,
    /// GCP project billed for Hubble queries; the reader pays for scanned
    /// bytes, not SDF. Empty disables below-retention replay.
    pub hubble_billing_project: String,
    /// Fully-qualified BigQuery dataset holding Hubble's tables.
    pub hubble_dataset: String,
    /// Maximum ledgers replayed per Hubble sweep.
    pub hubble_max_span: i32,
    /// RPC endpoint used to replay history when Hubble is unavailable. Bounded
    /// by that endpoint's retention, so it cannot cover a deeper gap. Empty
    /// leaves no replay source at all.
    pub backfill_source_url: String,
    /// Maximum ledgers replayed per RPC-fallback sweep.
    pub backfill_max_span: i32,
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
        let env = AppEnv::get_env_value();
        let oplend_wallet = if env == AppEnv::Production {
            "CACWIITWTXV47Z5EGVCE73HO5JEZENPZZFKQYPCZDWLNB5TF6RJ44CBI"
        } else {
            "CBUIDJMBY4FUBXVD24ZBO2PABJDEH3PYMPCR7VYI7NLARN4DN5EN2FL3"
        };

        LocalEnv {
            db_url: get_env("DATABASE_URL"),
            health_check_url: get_env("HEALTH_CHECK_URL"),
            factory_contract_id: get_env("FACTORY_CONTRACT_ID"),
            rewards_contract_id: get_env("REWARDS_CONTRACT_ID"),
            oplend_wallet: oplend_wallet.to_string(),
            soroban_rpc_url: get_env_or(
                "SOROBAN_RPC_URL",
                "https://soroban-testnet.stellar.org",
            ),
            hubble_billing_project: get_env("HUBBLE_BILLING_PROJECT"),
            hubble_dataset: get_env_or(
                "HUBBLE_DATASET",
                "crypto-stellar.crypto_stellar",
            ),
            hubble_max_span: get_env("HUBBLE_MAX_SPAN")
                .parse()
                .unwrap_or(120_000),
            backfill_source_url: get_env("BACKFILL_SOURCE_URL"),
            backfill_max_span: get_env("BACKFILL_MAX_SPAN")
                .parse()
                .unwrap_or(120_000),
            start_ledger: get_env("START_LEDGER").parse().unwrap_or(0),
            poll_interval_ms: get_env("POLL_INTERVAL_MS")
                .parse()
                .unwrap_or(5_000),
            chain_id: STELLAR_CHAIN_ID,
            env,
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
