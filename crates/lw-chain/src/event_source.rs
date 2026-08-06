use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use stellar_xdr::ScVal;

use async_trait::async_trait;
use eyre::eyre;
use log::warn;
use stellar_rpc_client::{Client, EventStart, EventType, GetEventsResponse};
use stellar_xdr::{Limits, ReadXdr};
use tokio::time::sleep;

/// SDK-agnostic event passed into decoding. Built from the RPC `getEvents`
/// response or the backfill source (later tasks).
#[derive(Debug, Clone)]
pub struct RawSorobanEvent {
    pub tx_hash: String,
    pub event_index: i32,
    pub contract_id: String,
    /// topic[0] is the event-name symbol; the rest are `#[topic]` fields.
    pub topics: Vec<ScVal>,
    /// Event payload: a `Map` for factory events, a single value for `transfer`.
    pub value: ScVal,
    pub ledger_seq: i32,
    pub ledger_closed_at: DateTime<Utc>,
}

/// Parse the RPC `ledgerClosedAt` RFC3339 timestamp; falls back to epoch 0.
pub fn parse_ledger_time(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc.timestamp_opt(0, 0).unwrap())
}

/// Deterministic per-event index parsed from the RPC event id (e.g.
/// "0001234567-0000000001" -> 1). Stable across re-indexing, so the
/// `{tx}#{index}` event hash stays unique and idempotent.
pub fn event_index_from_id(id: &str) -> i32 {
    id.rsplit('-')
        .next()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0) as i32
}

/// Parse the RPC "startLedger must be within the ledger range: LOW - HIGH"
/// rejection into `(low, high)`. Returns `None` for any other error. Lets the
/// event loop detect that its cursor fell behind live-RPC retention (cursor <
/// low) and switch to the backfill source for the gap.
pub fn parse_ledger_range(err: &str) -> Option<(i32, i32)> {
    fn leading_i32(s: &str) -> Option<i32> {
        s.trim()
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse()
            .ok()
    }
    let tail = err.split("ledger range:").nth(1)?;
    let mut parts = tail.splitn(2, '-');
    let low = leading_i32(parts.next()?)?;
    let high = leading_i32(parts.next()?)?;
    Some((low, high))
}

/// A source of Soroban events. `RpcEventSource` = live tail; `BackfillSource` =
/// history beyond RPC retention.
#[async_trait]
pub trait EventSource: Send + Sync {
    /// Fetch all events at/after `start_ledger` for `contract_ids` (paging
    /// internally until exhausted, splitting the contract set across requests
    /// when the transport caps ids per request). Returns the decoded events in
    /// chain order and the ledger to resume from on the next call.
    async fn fetch(
        &self,
        start_ledger: i32,
        contract_ids: &[String],
    ) -> eyre::Result<(Vec<RawSorobanEvent>, i32)>;
}

/// Soroban RPC rejects any `getEvents` filter carrying more than 5 contract ids
/// ("filter 1 invalid: maximum 5 contract IDs per filter"), and the client emits
/// exactly one filter per request, so larger contract sets are swept in batches.
const MAX_CONTRACT_IDS_PER_FILTER: usize = 5;

/// Retry budget per `getEvents` request when the RPC fails transiently. Batching
/// turns one sweep into several requests, so an unretried blip would discard
/// every batch already fetched and stall the cursor for a whole poll.
const RPC_MAX_RETRIES: u32 = 10;
/// First backoff step; doubles per retry, capped at `RPC_BACKOFF_CAP`
/// (250ms, 500ms, 1s, 2s, 4s, then 5s -> ~32s of cover across 10 retries).
const RPC_BACKOFF_BASE: Duration = Duration::from_millis(250);
const RPC_BACKOFF_CAP: Duration = Duration::from_secs(5);

/// Is this RPC failure worth retrying in place? Only transport-level faults are:
/// HTTP 429/5xx rejections, request timeouts and dropped connections. JSON-RPC
/// application errors (bad params, `startLedger` outside retention) are
/// permanent for this request and MUST surface immediately so the event loop can
/// react — e.g. learn the retention floor — instead of burning the whole retry
/// budget on a doomed request.
fn is_transient_rpc_error(err: &str) -> bool {
    // jsonrpsee transport rejection: "Request rejected `503`".
    if let Some(code) = err
        .split("Request rejected `")
        .nth(1)
        .and_then(|rest| rest.split('`').next())
        .and_then(|code| code.parse::<u16>().ok())
    {
        return code == 429 || (500..600).contains(&code);
    }
    const TRANSIENT: [&str; 8] = [
        "Request timeout",
        "error sending request",
        "connection closed",
        "connection reset",
        "connection refused",
        "broken pipe",
        "operation timed out",
        "dns error",
    ];
    TRANSIENT.iter().any(|needle| err.contains(needle))
}

/// Live tail backed by Soroban RPC `getEvents`.
pub struct RpcEventSource {
    client: Client,
    page_limit: usize,
}

impl RpcEventSource {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            page_limit: 10_000,
        }
    }

    fn to_raw(
        tx_hash: String,
        event_index: i32,
        contract_id: String,
        topic_xdr: &[String],
        value_xdr: &str,
        ledger_seq: i32,
        ledger_closed_at: &str,
    ) -> eyre::Result<RawSorobanEvent> {
        let topics = topic_xdr
            .iter()
            .map(|b64| ScVal::from_xdr_base64(b64, Limits::none()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| eyre!("topic xdr decode: {e}"))?;
        let value = ScVal::from_xdr_base64(value_xdr, Limits::none())
            .map_err(|e| eyre!("value xdr decode: {e}"))?;
        Ok(RawSorobanEvent {
            tx_hash,
            event_index,
            contract_id,
            topics,
            value,
            ledger_seq,
            ledger_closed_at: parse_ledger_time(ledger_closed_at),
        })
    }

    /// Single `getEvents` request, retried on transient transport failures with
    /// exponential backoff. Non-transient errors return on the first attempt.
    async fn get_events_retrying(
        &self,
        start: &EventStart,
        contract_ids: &[String],
    ) -> eyre::Result<GetEventsResponse> {
        let mut backoff = RPC_BACKOFF_BASE;
        let mut retries = 0;

        loop {
            let err = match self
                .client
                .get_events(
                    start.clone(),
                    Some(EventType::Contract),
                    contract_ids,
                    &[],
                    Some(self.page_limit),
                )
                .await
            {
                Ok(resp) => return Ok(resp),
                Err(e) => e.to_string(),
            };
            if retries == RPC_MAX_RETRIES || !is_transient_rpc_error(&err) {
                return Err(eyre!("getEvents failed: {err}"));
            }
            retries += 1;
            warn!(
                "[event_source] transient getEvents failure ({err}); retry \
                 {retries}/{RPC_MAX_RETRIES} in {}ms",
                backoff.as_millis()
            );
            sleep(backoff).await;
            backoff = (backoff * 2).min(RPC_BACKOFF_CAP);
        }
    }

    /// One `getEvents` sweep for at most `MAX_CONTRACT_IDS_PER_FILTER`
    /// contracts, paging until exhausted. Appends `(event id, event)` pairs to
    /// `out` — the id doubles as the chain-order sort key — and returns the
    /// ledger this batch must resume from.
    async fn fetch_batch(
        &self,
        start_ledger: i32,
        contract_ids: &[String],
        out: &mut Vec<(String, RawSorobanEvent)>,
    ) -> eyre::Result<i32> {
        let mut max_ledger: Option<i32> = None;
        #[allow(unused_assignments)]
        let mut latest_ledger = start_ledger;
        let mut start = EventStart::Ledger(start_ledger as u32);

        loop {
            let resp = self.get_events_retrying(&start, contract_ids).await?;
            latest_ledger = resp.latest_ledger as i32;
            let page_len = resp.events.len();
            for e in &resp.events {
                let ledger_seq = e.ledger as i32;
                max_ledger =
                    Some(max_ledger.map_or(ledger_seq, |m| m.max(ledger_seq)));
                let tx_hash = e.tx_hash.clone().unwrap_or_else(|| e.id.clone());
                out.push((
                    e.id.clone(),
                    Self::to_raw(
                        tx_hash,
                        event_index_from_id(&e.id),
                        e.contract_id.clone(),
                        &e.topic,
                        &e.value,
                        ledger_seq,
                        &e.ledger_closed_at,
                    )?,
                ));
            }
            // Page until exhausted (no silent truncation): a full page means more may exist.
            if page_len < self.page_limit || resp.cursor.is_empty() {
                break;
            }
            start = EventStart::Cursor(resp.cursor);
        }

        // Resume one past the highest event ledger; if none, jump to the chain
        // tip so we don't re-scan the same empty range forever.
        Ok(match max_ledger {
            Some(m) => m + 1,
            None => (latest_ledger + 1).max(start_ledger),
        })
    }
}

#[async_trait]
impl EventSource for RpcEventSource {
    async fn fetch(
        &self,
        start_ledger: i32,
        contract_ids: &[String],
    ) -> eyre::Result<(Vec<RawSorobanEvent>, i32)> {
        let mut merged: Vec<(String, RawSorobanEvent)> = Vec::new();
        // Resume from the lowest batch cursor: batches observe slightly
        // different chain tips, and a higher cursor would skip ledgers for the
        // batches lagging it. Overlapping re-reads are idempotent downstream.
        let mut next: Option<i32> = None;

        for batch in contract_ids.chunks(MAX_CONTRACT_IDS_PER_FILTER) {
            let batch_next =
                self.fetch_batch(start_ledger, batch, &mut merged).await?;
            next = Some(next.map_or(batch_next, |n| n.min(batch_next)));
        }

        // Restore chain order across batches: RPC event ids are fixed-width and
        // zero-padded, so (ledger, id) orders by ledger then tx/event position.
        merged.sort_unstable_by(|a, b| {
            (a.1.ledger_seq, &a.0).cmp(&(b.1.ledger_seq, &b.0))
        });
        let events = merged.into_iter().map(|(_, e)| e).collect();
        Ok((events, next.unwrap_or(start_ledger)))
    }
}

/// Backfill source for history older than the live RPC retention window. Reads
/// the same `getEvents` shape from a (possibly different) extended-retention
/// endpoint, bounded to `[start_ledger, start_ledger + max_span]`.
pub struct BackfillSource {
    inner: RpcEventSource,
    max_span: i32,
}

impl BackfillSource {
    /// Build from `BACKFILL_SOURCE_URL`; falls back to the live RPC url when unset.
    pub fn from_config(max_span: i32) -> eyre::Result<Self> {
        let cfg = lw_config::config::get_config();
        let url = if cfg.backfill_source_url.is_empty() {
            cfg.soroban_rpc_url
        } else {
            cfg.backfill_source_url
        };
        let client =
            Client::new(&url).map_err(|e| eyre!("backfill client: {e}"))?;
        Ok(Self {
            inner: RpcEventSource::new(client),
            max_span,
        })
    }
}

#[async_trait]
impl EventSource for BackfillSource {
    async fn fetch(
        &self,
        start_ledger: i32,
        contract_ids: &[String],
    ) -> eyre::Result<(Vec<RawSorobanEvent>, i32)> {
        let (mut events, next) =
            self.inner.fetch(start_ledger, contract_ids).await?;
        let ceiling = start_ledger + self.max_span;
        events.retain(|e| e.ledger_seq <= ceiling);
        Ok((events, next.min(ceiling + 1)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ledger_closed_at_rfc3339() {
        let ts = parse_ledger_time("2026-06-03T10:00:00Z");
        assert_eq!(ts.timestamp(), 1_780_480_800);
    }

    #[test]
    fn event_index_parsed_from_id_suffix() {
        assert_eq!(event_index_from_id("0001234567-0000000009"), 9);
        assert_eq!(event_index_from_id("garbage"), 0);
    }

    #[test]
    fn parses_retention_range_from_rpc_error() {
        let err = "getEvents failed: ErrorObject { code: InvalidRequest, \
                   message: \"startLedger must be within the ledger range: \
                   3276662 - 3397621\", data: None }";
        assert_eq!(parse_ledger_range(err), Some((3_276_662, 3_397_621)));
        assert_eq!(parse_ledger_range("some other transport error"), None);
    }

    #[test]
    fn retries_only_transport_level_rpc_failures() {
        // The 503 seen in production, plus throttling and dropped connections.
        assert!(is_transient_rpc_error(
            "getEvents failed: Request rejected `503`"
        ));
        assert!(is_transient_rpc_error("Request rejected `429`"));
        assert!(is_transient_rpc_error("Request timeout"));
        assert!(is_transient_rpc_error(
            "error sending request for url (...)"
        ));
        // Client-side rejections are permanent for this request.
        assert!(!is_transient_rpc_error("Request rejected `400`"));
        assert!(!is_transient_rpc_error(
            "ErrorObject { code: InvalidParams, message: \"filter 1 invalid: \
             maximum 5 contract IDs per filter\", data: None }"
        ));
        // Must stay non-transient: the event loop parses this to learn the
        // retention floor and switch to backfill. Retrying would stall that.
        assert!(!is_transient_rpc_error(
            "ErrorObject { code: InvalidRequest, message: \"startLedger must \
             be within the ledger range: 3276662 - 3397621\", data: None }"
        ));
    }
}
