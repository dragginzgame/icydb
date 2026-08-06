//! Module: response::page
//! Responsibility: public bounded scalar-page response payloads.
//! Does not own: cursor validation, planning, or source revision proofs.
//! Boundary: executor progress -> Candid-safe live page DTO.

use crate::value::OutputValue;
use candid::CandidType;
use serde::Deserialize;

/// Bounded work observed while producing one scalar page.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub struct ScalarPageWork {
    /// Immutable identity of the page-work envelope bound into continuation.
    pub envelope_identity: u64,
    /// Physical keys or index entries visited by this page execution.
    pub entries_visited: u64,
    /// Logical rows returned to the caller.
    pub result_rows: u32,
}

/// One revision-tolerant scalar keyset page.
///
/// A non-null continuation means traversal has not been proven exhausted. The
/// token is authenticated but not encrypted and must be treated as opaque.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct LiveQueryPageOutput {
    /// Accepted entity name used for the read.
    pub entity: String,
    /// Selected output-column names in row order.
    pub columns: Vec<String>,
    /// Row-oriented output values.
    pub rows: Vec<Vec<OutputValue>>,
    /// Number of returned rows.
    pub row_count: u32,
    /// Authenticated continuation, or `None` after proven exhaustion.
    pub continuation: Option<String>,
    /// Bounded work observed while producing this page.
    pub work: ScalarPageWork,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(CandidType)]
    struct FrozenScalarPageWorkWire {
        envelope_identity: u64,
        entries_visited: u64,
        result_rows: u32,
    }

    #[derive(CandidType)]
    struct FrozenLiveQueryPageOutputWire {
        entity: String,
        columns: Vec<String>,
        rows: Vec<Vec<OutputValue>>,
        row_count: u32,
        continuation: Option<String>,
        work: FrozenScalarPageWorkWire,
    }

    #[test]
    fn live_query_page_output_preserves_its_initial_candid_record_shape() {
        let current = LiveQueryPageOutput {
            entity: "example".to_string(),
            columns: vec!["id".to_string()],
            rows: vec![vec![OutputValue::Nat64(7)]],
            row_count: 1,
            continuation: Some("opaque".to_string()),
            work: ScalarPageWork {
                envelope_identity: 11,
                entries_visited: 2,
                result_rows: 1,
            },
        };
        let frozen = FrozenLiveQueryPageOutputWire {
            entity: current.entity.clone(),
            columns: current.columns.clone(),
            rows: current.rows.clone(),
            row_count: current.row_count,
            continuation: current.continuation.clone(),
            work: FrozenScalarPageWorkWire {
                envelope_identity: current.work.envelope_identity,
                entries_visited: current.work.entries_visited,
                result_rows: current.work.result_rows,
            },
        };

        assert_eq!(
            candid::encode_one(&current).expect("current live page should encode"),
            candid::encode_one(&frozen).expect("frozen live page should encode"),
        );
    }
}
