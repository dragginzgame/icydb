//! SQL output payload attribution DTOs and builders.
//! Does not own: SQL response shaping or public row rendering.

use crate::{db::session::sql::result::SqlStatementResult, value::OutputValue};
use candid::CandidType;
use serde::Deserialize;

///
/// SqlOutputBlobAttribution
///
/// Candid diagnostics payload for SQL projection payload size. Raw bytes count
/// the blob bytes projected into SQL output values; rendered hex bytes count
/// the blob-specific `0x...` text that public SQL row rendering will emit.
///

#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlOutputBlobAttribution {
    pub projected_values: u64,
    pub projected_bytes: u64,
    pub rendered_hex_bytes: u64,
}

pub(super) fn sql_output_blob_attribution(result: &SqlStatementResult) -> SqlOutputBlobAttribution {
    let mut attribution = SqlOutputBlobAttribution::default();

    match result {
        SqlStatementResult::Projection { rows, .. } => {
            for row in rows {
                for value in row {
                    record_output_value_blob_attribution(value, &mut attribution);
                }
            }
        }
        SqlStatementResult::Grouped { rows, .. } => {
            for row in rows {
                for value in row.group_key().iter().chain(row.aggregate_values()) {
                    record_output_value_blob_attribution(value, &mut attribution);
                }
            }
        }
        SqlStatementResult::Count { .. }
        | SqlStatementResult::Describe(_)
        | SqlStatementResult::ShowIndexes(_)
        | SqlStatementResult::ShowColumns(_)
        | SqlStatementResult::ShowEntities { .. }
        | SqlStatementResult::ShowStores { .. }
        | SqlStatementResult::ShowMemory(_)
        | SqlStatementResult::Ddl(_) => {}
        #[cfg(feature = "sql-explain")]
        SqlStatementResult::Explain(_) => {}
    }

    attribution
}

fn record_output_value_blob_attribution(
    value: &OutputValue,
    attribution: &mut SqlOutputBlobAttribution,
) {
    match value {
        OutputValue::Blob(bytes) => {
            let byte_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            attribution.projected_values = attribution.projected_values.saturating_add(1);
            attribution.projected_bytes = attribution.projected_bytes.saturating_add(byte_len);
            attribution.rendered_hex_bytes = attribution
                .rendered_hex_bytes
                .saturating_add(byte_len.saturating_mul(2).saturating_add(2));
        }
        OutputValue::Enum(value) => {
            if let Some(payload) = value.payload() {
                record_output_value_blob_attribution(payload, attribution);
            }
        }
        OutputValue::List(items) => {
            for item in items {
                record_output_value_blob_attribution(item, attribution);
            }
        }
        OutputValue::Map(entries) => {
            for (key, value) in entries {
                record_output_value_blob_attribution(key, attribution);
                record_output_value_blob_attribution(value, attribution);
            }
        }
        OutputValue::Account(_)
        | OutputValue::Bool(_)
        | OutputValue::Date(_)
        | OutputValue::Decimal(_)
        | OutputValue::Duration(_)
        | OutputValue::Float32(_)
        | OutputValue::Float64(_)
        | OutputValue::Int64(_)
        | OutputValue::Int128(_)
        | OutputValue::IntBig(_)
        | OutputValue::Null
        | OutputValue::Principal(_)
        | OutputValue::Subaccount(_)
        | OutputValue::Text(_)
        | OutputValue::Timestamp(_)
        | OutputValue::Nat64(_)
        | OutputValue::Nat128(_)
        | OutputValue::NatBig(_)
        | OutputValue::Ulid(_)
        | OutputValue::Unit => {}
    }
}
