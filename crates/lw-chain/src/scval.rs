//! Pure extractors from Soroban XDR `ScVal`s. Isolated so event decoding is
//! testable without an RPC and so XDR-type churn stays in one file.

use eyre::eyre;
use stellar_xdr::{Int128Parts, ScMap, ScSymbol, ScVal};

/// The event name carried in topic[0] of every Soroban contract event.
pub fn symbol_name(v: &ScVal) -> eyre::Result<String> {
    match v {
        ScVal::Symbol(ScSymbol(s)) => Ok(s.to_utf8_string_lossy()),
        _ => Err(eyre!("expected ScVal::Symbol, got {v:?}")),
    }
}

/// A `u32` (operation ids are `u32` in the contracts).
pub fn as_u32(v: &ScVal) -> eyre::Result<u32> {
    match v {
        ScVal::U32(n) => Ok(*n),
        _ => Err(eyre!("expected ScVal::U32, got {v:?}")),
    }
}

/// An `i128` amount, reassembled from its hi/lo parts.
pub fn as_i128(v: &ScVal) -> eyre::Result<i128> {
    match v {
        ScVal::I128(Int128Parts { hi, lo }) => {
            Ok(((*hi as i128) << 64) | (*lo as i128))
        }
        _ => Err(eyre!("expected ScVal::I128, got {v:?}")),
    }
}

/// A Stellar address (contract `C...` or account `G...`) as its StrKey string.
pub fn as_address(v: &ScVal) -> eyre::Result<String> {
    match v {
        ScVal::Address(addr) => Ok(addr.to_string()),
        _ => Err(eyre!("expected ScVal::Address, got {v:?}")),
    }
}

/// Look up a field by name in an event data `Map`.
pub fn map_field<'a>(map: &'a ScMap, key: &str) -> eyre::Result<&'a ScVal> {
    map.0
        .iter()
        .find(|entry| matches!(&entry.key, ScVal::Symbol(ScSymbol(s)) if s.to_utf8_string_lossy() == key))
        .map(|entry| &entry.val)
        .ok_or_else(|| eyre!("missing data field `{key}`"))
}

/// Borrow the inner `ScMap` of a data `ScVal::Map`.
pub fn as_map(v: &ScVal) -> eyre::Result<&ScMap> {
    match v {
        ScVal::Map(Some(m)) => Ok(m),
        _ => Err(eyre!("expected ScVal::Map, got {v:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stellar_xdr::{Int128Parts, ScSymbol, ScVal, StringM};

    #[test]
    fn decodes_i128_round_trip() {
        let v = ScVal::I128(Int128Parts {
            hi: 0,
            lo: 1_000_000,
        });
        assert_eq!(as_i128(&v).unwrap(), 1_000_000_i128);
    }
    #[test]
    fn decodes_u32() {
        assert_eq!(as_u32(&ScVal::U32(7)).unwrap(), 7);
    }
    #[test]
    fn reads_symbol_name() {
        let sym: StringM<32> = "Invested".try_into().unwrap();
        let v = ScVal::Symbol(ScSymbol(sym));
        assert_eq!(symbol_name(&v).unwrap(), "Invested");
    }
    #[test]
    fn errors_on_type_mismatch() {
        assert!(as_u32(&ScVal::U32(1)).is_ok());
        assert!(as_i128(&ScVal::U32(1)).is_err());
    }
}
