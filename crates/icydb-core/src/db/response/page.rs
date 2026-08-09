//! Module: response::page
//! Responsibility: public bounded scalar-page response payloads.
//! Does not own: cursor validation, planning, or source revision proofs.
//! Boundary: executor progress -> Candid-safe live page DTO.

use crate::{db::ReadSetRevisionProof, value::OutputValue};
use candid::CandidType;
use serde::Deserialize;

/// Bounded work observed while producing one scalar page.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub struct ScalarPageWork {
    /// Exact identity of the operational work envelope used for this page.
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

/// One revision-strict exhaustive scalar page.
///
/// The returned proof must be persisted beside the continuation and supplied
/// unchanged on resume. Any participating source change invalidates traversal.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ExhaustiveQueryPageOutput {
    /// Accepted entity name used for the read.
    pub entity: String,
    /// Selected output-column names in row order.
    pub columns: Vec<String>,
    /// Row-oriented output values.
    pub rows: Vec<Vec<OutputValue>>,
    /// Number of returned rows.
    pub row_count: u32,
    /// Authenticated continuation, or `None` after proof-bound exhaustion.
    pub continuation: Option<String>,
    /// Bounded work observed while producing this page.
    pub work: ScalarPageWork,
    /// Complete source authority that must accompany a resume.
    pub proof: ReadSetRevisionProof,
}

impl ExhaustiveQueryPageOutput {
    pub(in crate::db) fn from_live_page(
        page: LiveQueryPageOutput,
        proof: ReadSetRevisionProof,
    ) -> Self {
        Self {
            entity: page.entity,
            columns: page.columns,
            rows: page.rows,
            row_count: page.row_count,
            continuation: page.continuation,
            work: page.work,
            proof,
        }
    }
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

    #[derive(CandidType)]
    struct FrozenReadSetStoreIdentityWire([u8; 32]);

    #[derive(CandidType)]
    struct FrozenReadSetStoreRevisionWire {
        store: FrozenReadSetStoreIdentityWire,
        data_revision: u64,
        access_state_revision: u64,
    }

    #[derive(CandidType)]
    struct FrozenReadSetRevisionProofWire {
        database_incarnation: [u8; 16],
        accepted_root_revision: u64,
        accepted_root_fingerprint_method: u8,
        accepted_root_fingerprint: [u8; 32],
        stores: Vec<FrozenReadSetStoreRevisionWire>,
    }

    #[derive(CandidType)]
    struct FrozenExhaustiveQueryPageOutputWire {
        entity: String,
        columns: Vec<String>,
        rows: Vec<Vec<OutputValue>>,
        row_count: u32,
        continuation: Option<String>,
        work: FrozenScalarPageWorkWire,
        proof: FrozenReadSetRevisionProofWire,
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

    #[test]
    fn exhaustive_query_page_output_freezes_its_initial_candid_record_shape() {
        let proof = ReadSetRevisionProof::from_parts(
            [1; 16],
            7,
            1,
            [2; 32],
            vec![crate::db::ReadSetStoreRevision::new(
                crate::db::ReadSetStoreIdentity::from_bytes([3; 32]),
                11,
                13,
            )],
        )
        .expect("bounded canonical proof should admit");
        let current = ExhaustiveQueryPageOutput {
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
            proof,
        };
        let frozen = FrozenExhaustiveQueryPageOutputWire {
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
            proof: FrozenReadSetRevisionProofWire {
                database_incarnation: current.proof.database_incarnation(),
                accepted_root_revision: current.proof.accepted_root_revision(),
                accepted_root_fingerprint_method: current.proof.accepted_root_fingerprint_method(),
                accepted_root_fingerprint: current.proof.accepted_root_fingerprint(),
                stores: current
                    .proof
                    .stores()
                    .iter()
                    .map(|store| FrozenReadSetStoreRevisionWire {
                        store: FrozenReadSetStoreIdentityWire(store.store().to_bytes()),
                        data_revision: store.data_revision(),
                        access_state_revision: store.access_state_revision(),
                    })
                    .collect(),
            },
        };

        assert_eq!(
            candid::encode_one(&current).expect("current exhaustive page should encode"),
            candid::encode_one(&frozen).expect("frozen exhaustive page should encode"),
        );
    }
}
