use crate::config::{STELLAR_CHAIN_ID, get_config};
use stellar_rpc_client::Client;

/// Single network this service indexes. Replaces the former multi-chain
/// registry. `chain_id` is fixed to the Stellar sentinel (0); `rewards_address`
/// is `Some` once `REWARDS_CONTRACT_ID` is configured.
#[derive(Debug, Clone)]
pub struct StellarNetwork {
    pub chain_id: i32,
    pub factory_contract_id: String,
    pub factory_start_ledger: i32,
    pub rewards_address: Option<String>,
}

impl Default for StellarNetwork {
    fn default() -> Self {
        let cfg = get_config();
        let rewards_address = if cfg.rewards_contract_id.is_empty() {
            None
        } else {
            Some(cfg.rewards_contract_id)
        };
        Self {
            chain_id: STELLAR_CHAIN_ID,
            factory_contract_id: cfg.factory_contract_id,
            factory_start_ledger: cfg.start_ledger,
            rewards_address,
        }
    }
}

/// Build a Soroban RPC client from the configured endpoint.
pub fn get_rpc_client() -> eyre::Result<Client> {
    let cfg = get_config();
    Client::new(&cfg.soroban_rpc_url)
        .map_err(|e| eyre::eyre!("Failed to build Soroban RPC client: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stellar_network_uses_chain_id_zero() {
        let net = StellarNetwork::default();
        assert_eq!(net.chain_id, 0);
        assert_eq!(net.rewards_address, None);
    }
}
