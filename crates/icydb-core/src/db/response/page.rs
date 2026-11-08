//! Module: response::page
//! Responsibility: public bounded scalar-page response payloads.
//! Does not own: cursor validation, planning, or source revision proofs.
//! Boundary: executor progress -> Wire-safe live page DTO.

use crate::{db::ReadSetRevisionProof, value::OutputValue};

use serde::Deserialize;

/// Bounded work observed while producing one scalar page.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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

impl LiveQueryPageOutput {
    /// Return the number of rows carried by this page.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.rows.len()
    }

    /// Return whether this page carries no rows.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl ExhaustiveQueryPageOutput {
    /// Return the number of rows carried by this page.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.rows.len()
    }

    /// Return whether this page carries no rows.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

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
mod tests;
