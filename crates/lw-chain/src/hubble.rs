//! Backfill from Stellar Hubble, SDF's public BigQuery dataset
//! (`crypto-stellar.crypto_stellar`).
//!
//! Used only for history the live RPC has already dropped. The event loop keeps
//! tailing `getEvents` and only routes here while the cursor sits below the
//! learned retention floor, so Hubble covers `[cursor, retention_floor)` and
//! hands back as soon as the RPC can serve the range itself. Hubble trails the
//! chain by minutes while the RPC retains ~7 days, so the two windows overlap
//! by orders of magnitude and the handoff has no hole.
//!
//! Two properties of the dataset drive this implementation:
//!
//! 1. `history_contract_events` is MONTH-partitioned on `closed_at` and
//!    clustered on `contract_id`; `ledger_sequence` is neither. A query filtered
//!    only by ledger range scans every monthly partition since Soroban launch,
//!    so every sweep first resolves its ledger range to a `closed_at` window.
//! 2. The table is sparse — a ledger with no matching events has no row — so
//!    "no rows" cannot distinguish "nothing happened" from "not ingested yet".
//!    The watermark therefore comes from the dense `history_ledgers` table, and
//!    the cursor never advances past it.

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use eyre::eyre;
use gcp_bigquery_client::Client;
use gcp_bigquery_client::model::get_query_results_parameters::GetQueryResultsParameters;
use gcp_bigquery_client::model::query_parameter::QueryParameter;
use gcp_bigquery_client::model::query_parameter_type::QueryParameterType;
use gcp_bigquery_client::model::query_parameter_value::QueryParameterValue;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::query_response::ResultSet;
use log::{info, warn};
use stellar_xdr::{Limits, ReadXdr, ScVal};

use crate::event_source::{EventSource, RawSorobanEvent};

/// Rows per result page. Responses are capped at 10 MB regardless, so this only
/// bounds how often we page.
const PAGE_ROWS: i32 = 20_000;

/// How long BigQuery may take before the first response returns; paging
/// continues afterwards regardless.
const QUERY_TIMEOUT_MS: i32 = 60_000;

/// Largest tip difference still consistent with Hubble simply lagging (~58
/// days of ledgers). A wrong-network dataset is off by tens of millions —
/// pubnet sits near 64M while testnet is an order of magnitude lower and resets
/// periodically — so anything between the two is unambiguous.
const MAX_TIP_DRIFT: i32 = 1_000_000;

/// Backfill source backed by Hubble's BigQuery dataset.
pub struct HubbleSource {
    client: Client,
    /// GCP project billed for query bytes. Hubble's own project only pays for
    /// storage; the reader pays compute.
    billing_project: String,
    /// Fully-qualified dataset, e.g. `crypto-stellar.crypto_stellar`.
    dataset: String,
    /// Ledgers per sweep. Cost tracks monthly partitions touched rather than
    /// span, so this mainly bounds result size and time-to-first-row.
    max_span: i32,
}

fn int_param(name: &str, value: i64) -> QueryParameter {
    QueryParameter {
        name: Some(name.to_string()),
        parameter_type: Some(QueryParameterType {
            r#type: "INT64".to_string(),
            array_type: None,
            struct_types: None,
        }),
        parameter_value: Some(QueryParameterValue {
            value: Some(value.to_string()),
            array_values: None,
            struct_values: None,
        }),
    }
}

fn string_array_param(name: &str, values: &[String]) -> QueryParameter {
    QueryParameter {
        name: Some(name.to_string()),
        parameter_type: Some(QueryParameterType {
            r#type: "ARRAY".to_string(),
            array_type: Some(Box::new(QueryParameterType {
                r#type: "STRING".to_string(),
                array_type: None,
                struct_types: None,
            })),
            struct_types: None,
        }),
        parameter_value: Some(QueryParameterValue {
            value: None,
            array_values: Some(
                values
                    .iter()
                    .map(|v| QueryParameterValue {
                        value: Some(v.clone()),
                        array_values: None,
                        struct_values: None,
                    })
                    .collect(),
            ),
            struct_values: None,
        }),
    }
}

/// Ledger-range coverage resolved from the dense `history_ledgers` table:
/// the `closed_at` window that prunes partitions, plus the highest ledger
/// Hubble has actually ingested in the requested range.
#[derive(Debug, PartialEq, Eq)]
struct Coverage {
    first_closed_at: i64,
    last_closed_at: i64,
    max_ledger: i32,
}

impl HubbleSource {
    /// Connect using Application Default Credentials — the `GOOGLE_APPLICATION_
    /// CREDENTIALS` service-account file locally, the attached identity on GCP.
    ///
    /// Proves credentials, billing project, dataset access *and* network match
    /// at boot by reading Hubble's own tip and comparing it against
    /// `network_tip` from the live RPC. An `Err` here lets the caller fall back
    /// to the RPC replay while the process is still starting, instead of
    /// discovering the problem mid-incident.
    ///
    /// The network check is not optional: SDF publishes Hubble for **pubnet
    /// only**. Pointed at it from testnet the queries would all succeed —
    /// testnet ledger numbers simply address much older pubnet ledgers — match
    /// none of the observed contracts, and advance the cursor over history that
    /// was never read. Loud failure beats silent loss.
    pub async fn connect(
        billing_project: &str,
        dataset: &str,
        max_span: i32,
        network_tip: i32,
    ) -> eyre::Result<Self> {
        let client = Client::from_application_default_credentials()
            .await
            .map_err(|e| eyre!("bigquery auth: {e}"))?;
        let source = Self {
            client,
            billing_project: billing_project.to_string(),
            dataset: dataset.to_string(),
            max_span: max_span.max(1),
        };
        let hubble_tip = source
            .tip()
            .await
            .map_err(|e| eyre!("bigquery probe on {dataset}: {e}"))?
            .ok_or_else(|| eyre!("{dataset}.history_ledgers is empty"))?;
        let drift = (network_tip - hubble_tip).abs();
        if drift > MAX_TIP_DRIFT {
            return Err(eyre!(
                "{dataset} tip is ledger {hubble_tip} but the RPC is at \
                 {network_tip} ({drift} apart): this dataset is for a \
                 different network"
            ));
        }
        info!(
            "[hubble] backfill ready: dataset {dataset} at ledger \
             {hubble_tip} ({} behind the RPC), billed to {billing_project}, \
             {max_span} ledgers/sweep",
            network_tip - hubble_tip
        );
        Ok(source)
    }

    /// Highest ledger Hubble has ingested. Doubles as the boot probe: it
    /// resolves the table, the permissions and the network in one cheap scan
    /// of a single INT64 column over at most two monthly partitions.
    ///
    /// The 40-day window guarantees a non-empty result immediately after a
    /// month boundary.
    async fn tip(&self) -> eyre::Result<Option<i32>> {
        let sql = format!(
            "SELECT MAX(sequence) AS tip \
             FROM `{}.history_ledgers` \
             WHERE closed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), \
                                              INTERVAL 40 DAY)",
            self.dataset
        );
        let mut pages = self.query(sql, Vec::new()).await?;
        let Some(rows) = pages.first_mut() else {
            return Ok(None);
        };
        if !rows.next_row() {
            return Ok(None);
        }
        Ok(rows.get_i64_by_name("tip")?.map(|t| t as i32))
    }

    /// Run `sql` and collect every page into one `ResultSet` list.
    async fn query(
        &self,
        sql: String,
        params: Vec<QueryParameter>,
    ) -> eyre::Result<Vec<ResultSet>> {
        let mut request = QueryRequest::new(sql);
        request.parameter_mode = Some("NAMED".to_string());
        request.query_parameters = Some(params);
        request.max_results = Some(PAGE_ROWS);
        request.timeout_ms = Some(QUERY_TIMEOUT_MS);

        let first = self
            .client
            .job()
            .query(&self.billing_project, request)
            .await
            .map_err(|e| eyre!("bigquery query: {e}"))?;

        let job_id = first
            .job_reference
            .as_ref()
            .and_then(|r| r.job_id.clone())
            .ok_or_else(|| {
                eyre!("bigquery query: response carried no job id")
            })?;
        let mut page_token = first.page_token.clone();
        let mut pages = vec![ResultSet::new_from_query_response(first)];

        while let Some(token) = page_token {
            let page = self
                .client
                .job()
                .get_query_results(
                    &self.billing_project,
                    &job_id,
                    GetQueryResultsParameters {
                        page_token: Some(token),
                        max_results: Some(PAGE_ROWS),
                        timeout_ms: Some(QUERY_TIMEOUT_MS),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| eyre!("bigquery paging: {e}"))?;
            page_token = page.page_token.clone();
            pages.push(ResultSet::new_from_get_query_results_response(page));
        }
        Ok(pages)
    }

    /// Resolve `[from, to]` against `history_ledgers`. `None` means Hubble has
    /// not ingested any ledger in that range yet.
    ///
    /// This table is dense (one row per ledger) and clustered on
    /// `sequence, closed_at`, so it answers both questions the events query
    /// needs — the partition window and the ingestion watermark — in one cheap
    /// scan. Deriving them inside the events query instead would defeat
    /// partition pruning, because BigQuery cannot prune on a value it only
    /// learns at execution time.
    async fn coverage(
        &self,
        from: i32,
        to: i32,
    ) -> eyre::Result<Option<Coverage>> {
        let sql = format!(
            "SELECT UNIX_SECONDS(MIN(closed_at)) AS t0, \
                    UNIX_SECONDS(MAX(closed_at)) AS t1, \
                    MAX(sequence) AS max_ledger \
             FROM `{}.history_ledgers` \
             WHERE sequence BETWEEN @from AND @to",
            self.dataset
        );
        let pages = self
            .query(
                sql,
                vec![
                    int_param("from", from as i64),
                    int_param("to", to as i64),
                ],
            )
            .await?;
        let mut rows = pages.into_iter().next().unwrap_or_else(|| {
            ResultSet::new_from_query_response(Default::default())
        });
        if !rows.next_row() {
            return Ok(None);
        }
        // MIN/MAX over an empty range yield NULL rather than no row.
        let (Some(t0), Some(t1), Some(max_ledger)) = (
            rows.get_i64_by_name("t0")?,
            rows.get_i64_by_name("t1")?,
            rows.get_i64_by_name("max_ledger")?,
        ) else {
            return Ok(None);
        };
        Ok(Some(Coverage {
            first_closed_at: t0,
            last_closed_at: t1,
            max_ledger: max_ledger as i32,
        }))
    }
}

/// Build the contract-events query for a resolved range.
///
/// `event_index` is synthesised, not read: Hubble keeps no per-event ordinal and
/// BigQuery guarantees no row order, so the RPC's own index cannot be
/// reproduced. It exists solely to keep `{tx_hash}#{event_index}` unique, so
/// what matters is determinism — the same range must always yield the same
/// indices. Partitioning by `contract_id` keeps them stable as the observed
/// contract set grows; ordering by `contract_event_xdr` keeps them stable
/// across re-runs. Ties are byte-identical events, so either assignment
/// produces the same set of hashes.
///
/// `type = 1` keeps contract events only, and `successful` drops
/// failed-transaction events — `in_successful_contract_call` cannot be used for
/// that, since the ETL hardcodes it to true when wrapping contract events.
fn events_sql(dataset: &str, coverage: &Coverage) -> String {
    format!(
        "SELECT transaction_hash, \
                contract_id, \
                ledger_sequence, \
                UNIX_SECONDS(closed_at) AS closed_at_s, \
                ARRAY_TO_STRING(JSON_VALUE_ARRAY(topics), ',') AS topics_b64, \
                JSON_VALUE(data) AS data_b64, \
                ROW_NUMBER() OVER ( \
                    PARTITION BY transaction_hash, operation_id, contract_id \
                    ORDER BY contract_event_xdr \
                ) - 1 AS event_index \
         FROM `{dataset}.history_contract_events` \
         WHERE closed_at BETWEEN TIMESTAMP_SECONDS({t0}) \
                             AND TIMESTAMP_SECONDS({t1}) \
           AND ledger_sequence BETWEEN @from AND @to \
           AND contract_id IN UNNEST(@ids) \
           AND type = 1 \
           AND successful \
         ORDER BY ledger_sequence, transaction_id, operation_id, event_index",
        t0 = coverage.first_closed_at,
        t1 = coverage.last_closed_at,
    )
}

/// Decode one result row. Returns `Err` for a row we cannot turn into a usable
/// event; the caller skips it rather than failing the whole sweep.
fn row_to_event(rows: &ResultSet) -> eyre::Result<RawSorobanEvent> {
    let want = |name: &str| -> eyre::Result<String> {
        rows.get_string_by_name(name)
            .map_err(|e| eyre!("hubble column {name}: {e}"))?
            .ok_or_else(|| eyre!("hubble column {name} is null"))
    };
    let want_i64 = |name: &str| -> eyre::Result<i64> {
        rows.get_i64_by_name(name)
            .map_err(|e| eyre!("hubble column {name}: {e}"))?
            .ok_or_else(|| eyre!("hubble column {name} is null"))
    };

    // Base64 never contains a comma, so the topic array survives being flattened
    // into one column — which keeps every column a plain STRING or INT64 and
    // avoids BigQuery's nested `{"v": ...}` row envelopes entirely.
    let topics = want("topics_b64")?
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|b64| ScVal::from_xdr_base64(b64, Limits::none()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| eyre!("hubble topic xdr: {e}"))?;
    // `topics`/`data` hold the literal "n/a" when the ETL failed to serialise
    // the ScVal, so a decode error here is a data defect, not a bug.
    let value = ScVal::from_xdr_base64(&want("data_b64")?, Limits::none())
        .map_err(|e| eyre!("hubble value xdr: {e}"))?;

    Ok(RawSorobanEvent {
        tx_hash: want("transaction_hash")?,
        event_index: want_i64("event_index")? as i32,
        contract_id: want("contract_id")?,
        topics,
        value,
        ledger_seq: want_i64("ledger_sequence")? as i32,
        ledger_closed_at: to_utc(want_i64("closed_at_s")?),
    })
}

fn to_utc(unix_seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(unix_seconds, 0)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).unwrap())
}

#[async_trait]
impl EventSource for HubbleSource {
    async fn fetch(
        &self,
        start_ledger: i32,
        contract_ids: &[String],
    ) -> eyre::Result<(Vec<RawSorobanEvent>, i32)> {
        let requested_end = start_ledger.saturating_add(self.max_span - 1);
        let Some(coverage) = self.coverage(start_ledger, requested_end).await?
        else {
            warn!(
                "[hubble] no ledgers ingested in {start_ledger}-{requested_end} \
                 yet; holding cursor"
            );
            return Ok((Vec::new(), start_ledger));
        };

        let sql = events_sql(&self.dataset, &coverage);
        let params = vec![
            int_param("from", start_ledger as i64),
            int_param("to", coverage.max_ledger as i64),
            string_array_param("ids", contract_ids),
        ];

        let mut events = Vec::new();
        let mut skipped: HashMap<String, usize> = HashMap::new();
        for mut page in self.query(sql, params).await? {
            while page.next_row() {
                match row_to_event(&page) {
                    Ok(event) => events.push(event),
                    Err(e) => *skipped.entry(e.to_string()).or_default() += 1,
                }
            }
        }
        for (reason, count) in &skipped {
            warn!("[hubble] skipped {count} undecodable row(s): {reason}");
        }

        info!(
            "[hubble] replayed {}-{} ({} event(s))",
            start_ledger,
            coverage.max_ledger,
            events.len()
        );
        // Advance only as far as Hubble has actually ingested: the events table
        // is sparse, so an empty result past this point would be
        // indistinguishable from un-ingested ledgers.
        Ok((events, coverage.max_ledger + 1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COVERAGE: Coverage = Coverage {
        first_closed_at: 1_780_000_000,
        last_closed_at: 1_780_600_000,
        max_ledger: 64_010_314,
    };

    /// A `jobs.query` reply shaped exactly as BigQuery returns it for the
    /// backfill SELECT: every value is a JSON string wrapped in the REST
    /// `{"f":[{"v":…}]}` row envelope, including INT64 columns.
    fn canned_page() -> ResultSet {
        let field = |name: &str, ty: &str| serde_json::json!({ "name": name, "type": ty });
        let response = serde_json::json!({
            "kind": "bigquery#queryResponse",
            "jobComplete": true,
            "jobReference": { "projectId": "billing", "jobId": "job-1" },
            "totalRows": "1",
            "schema": { "fields": [
                field("transaction_hash", "STRING"),
                field("contract_id", "STRING"),
                field("ledger_sequence", "INTEGER"),
                field("closed_at_s", "INTEGER"),
                field("topics_b64", "STRING"),
                field("data_b64", "STRING"),
                field("event_index", "INTEGER"),
            ]},
            "rows": [{ "f": [
                { "v": "ea577c659d3011b1d9f1cf1ff1b1a1c1d1e1f101112131415161718191a1b1c1" },
                { "v": "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA" },
                { "v": "64010000" },
                { "v": "1780480800" },
                // Real mainnet ScVal XDR: Symbol("core_metrics"),
                // Symbol("write_entry"), flattened on a comma.
                { "v": "AAAADwAAAAxjb3JlX21ldHJpY3M=,AAAADwAAAAt3cml0ZV9lbnRyeQA=" },
                { "v": "AAAABQAAAAAAAAAG" },
                { "v": "0" },
            ]}],
        });
        ResultSet::new_from_query_response(
            serde_json::from_value(response).expect("canned query response"),
        )
    }

    #[test]
    fn decodes_a_bigquery_row_into_a_raw_event() {
        let mut page = canned_page();
        assert!(page.next_row());
        let event = row_to_event(&page).expect("row decodes");

        // INT64 columns arrive as JSON strings and must be coerced, not cast.
        assert_eq!(event.ledger_seq, 64_010_000);
        assert_eq!(event.event_index, 0);
        assert_eq!(event.ledger_closed_at.timestamp(), 1_780_480_800);
        assert_eq!(
            event.contract_id,
            "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
        );
        // The base64 XDR Hubble stores is byte-identical to the RPC's, so the
        // existing ScVal decode path applies unchanged.
        assert_eq!(event.topics.len(), 2);
        assert_eq!(
            crate::scval::symbol_name(&event.topics[0]).unwrap(),
            "core_metrics"
        );
        assert_eq!(event.value, ScVal::U64(6));
    }

    #[test]
    fn rejects_a_row_whose_xdr_is_the_etl_sentinel() {
        // stellar-etl writes the literal "n/a" when it cannot serialise an
        // ScVal. That must fail the row, not the sweep, and never panic.
        let response = serde_json::json!({
            "kind": "bigquery#queryResponse",
            "jobComplete": true,
            "jobReference": { "projectId": "billing", "jobId": "job-2" },
            "totalRows": "1",
            "schema": { "fields": [
                { "name": "transaction_hash", "type": "STRING" },
                { "name": "contract_id", "type": "STRING" },
                { "name": "ledger_sequence", "type": "INTEGER" },
                { "name": "closed_at_s", "type": "INTEGER" },
                { "name": "topics_b64", "type": "STRING" },
                { "name": "data_b64", "type": "STRING" },
                { "name": "event_index", "type": "INTEGER" },
            ]},
            "rows": [{ "f": [
                { "v": "abcd" },
                { "v": "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA" },
                { "v": "64010000" },
                { "v": "1780480800" },
                { "v": "n/a" },
                { "v": "n/a" },
                { "v": "0" },
            ]}],
        });
        let mut page = ResultSet::new_from_query_response(
            serde_json::from_value(response).expect("canned query response"),
        );
        assert!(page.next_row());
        assert!(row_to_event(&page).is_err());
    }

    #[test]
    fn query_prunes_partitions_by_closed_at_not_ledger_sequence() {
        // history_contract_events is MONTH-partitioned on closed_at and
        // clustered on contract_id; ledger_sequence is neither. Without the
        // literal closed_at window every monthly partition since Soroban
        // launch is scanned, so this predicate is the whole cost control.
        let sql = events_sql("crypto-stellar.crypto_stellar", &COVERAGE);
        assert!(sql.contains(
            "closed_at BETWEEN TIMESTAMP_SECONDS(1780000000) \
                             AND TIMESTAMP_SECONDS(1780600000)"
        ));
        assert!(sql.contains("contract_id IN UNNEST(@ids)"));
        assert!(sql.contains("ledger_sequence BETWEEN @from AND @to"));
    }

    #[test]
    fn query_excludes_failed_and_non_contract_events() {
        let sql = events_sql("crypto-stellar.crypto_stellar", &COVERAGE);
        assert!(sql.contains("type = 1"));
        assert!(sql.contains("AND successful"));
        // in_successful_contract_call is hardcoded true by the ETL for wrapped
        // contract events, so it must never stand in for `successful`.
        assert!(!sql.contains("in_successful_contract_call"));
    }

    #[test]
    fn synthesised_index_is_stable_across_reruns_and_contract_growth() {
        let sql = events_sql("crypto-stellar.crypto_stellar", &COVERAGE);
        // Partitioning by contract_id keeps indices stable when the observed
        // contract set grows; ordering by the event XDR keeps them stable
        // across re-runs of the same range.
        assert!(sql.contains(
            "PARTITION BY transaction_hash, operation_id, contract_id \
                    ORDER BY contract_event_xdr"
        ));
        assert!(sql.contains(") - 1 AS event_index"));
    }

    #[test]
    fn array_parameter_carries_each_contract_id_separately() {
        let ids = vec!["CA000".to_string(), "CB111".to_string()];
        let param = string_array_param("ids", &ids);
        let ty = param.parameter_type.unwrap();
        assert_eq!(ty.r#type, "ARRAY");
        assert_eq!(ty.array_type.unwrap().r#type, "STRING");
        let values = param.parameter_value.unwrap().array_values.unwrap();
        let sent: Vec<String> =
            values.into_iter().filter_map(|v| v.value).collect();
        assert_eq!(sent, ids);
    }
}
