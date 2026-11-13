//! Module: db::sql::types
//!
//! Responsibility: public native SQL results.
//! Does not own: SQL parsing, lowering, planning, or execution.
//! Boundary: converts executed core SQL outputs into endpoint-friendly payloads.

use crate::{
    ConstraintValidationFindingOutput, Error,
    db::{
        EntityCatalogDescription, EntityConstraintDescription, MemoryCatalogDescription,
        RowProjectionOutput, SqlDescribeOutput, SqlShowColumnsOutput, SqlShowRelationsOutput,
        StoreCatalogDescription,
    },
};

use serde::Deserialize;

/// One caller-authorized SQL read under the canonical public read contract.
///
/// Scalar progress is explicit. Grouped statements retain their grouped cursor;
/// metadata statements have no continuation. Authorization must run on each call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub enum SqlQueryPageResult {
    /// A bounded scalar projection with authenticated traversal progress.
    Scalar {
        /// SQL-rendered projection for this page only.
        projection: RowProjectionOutput,
        /// Opaque authenticated token, or `None` after proven exhaustion.
        continuation: Option<String>,
        /// Canonical public scalar envelope identity and observed work.
        work: crate::db::ScalarPageWork,
    },
    /// Metadata or a bounded grouped result with its own grouped cursor.
    Statement(SqlQueryResult),
}

impl SqlQueryPageResult {
    /// Check the complete encoded public page reply against the query boundary.
    #[doc(hidden)]
    pub fn into_deliverable_query_reply(self) -> Result<Self, Error> {
        crate::wire::reply::into_deliverable_query_reply(
            self,
            crate::diagnostic::RuntimeBoundaryCode::SqlQueryReplyBytesExceeded,
        )
    }
}

#[doc = "SqlGroupedRowsOutput\n\nStructured grouped SQL payload."]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub struct SqlGroupedRowsOutput {
    /// Accepted entity name.
    pub entity: String,
    /// Output column labels in row-cell order.
    pub columns: Vec<String>,
    /// SQL display cells: NULL is uppercase and marker-like text is quoted.
    /// Values are formatted before conversion; consumers must not infer types
    /// from these strings or rewrite them.
    pub rows: Vec<Vec<String>>,
    /// Number of rows represented by this result.
    pub row_count: u32,
    /// Opaque grouped continuation token, absent after exhaustion.
    pub next_cursor: Option<String>,
}

#[doc = "SqlConstraintValidationOutput\n\nTyped progress from one bounded constraint-validation step."]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub struct SqlConstraintValidationOutput {
    /// Stable accepted constraint identity.
    pub constraint_id: u32,
    /// Durable activation identity while validation remains active.
    pub activation_epoch: Option<u64>,
    /// Sequence to acknowledge before advancing a retained finding page.
    pub page_sequence: Option<u64>,
    /// Current engine-owned validation state.
    pub state: String,
    /// Current revision-proof status.
    pub revision_status: String,
    /// Cumulative classified-row count for this job.
    pub rows_scanned: u64,
    /// Bounded findings retained by this page.
    pub findings: Vec<ConstraintValidationFindingOutput>,
    /// Whether validation and accepted publication are complete.
    pub complete: bool,
}

#[doc = "SqlQueryResult\n\nUnified SQL endpoint result."]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub enum SqlQueryResult {
    /// Number of rows selected by a count statement.
    Count {
        /// Accepted entity name.
        entity: String,
        /// Number of rows represented by this result.
        row_count: u32,
    },
    /// Projected scalar rows.
    Projection(RowProjectionOutput),
    /// Grouped aggregate rows and continuation.
    Grouped(SqlGroupedRowsOutput),
    /// Rendered query plan.
    Explain {
        /// Accepted entity name.
        entity: String,
        /// Human-readable plan produced by the engine.
        explain: String,
    },
    /// Accepted entity description.
    Describe(SqlDescribeOutput),
    /// Accepted constraints for an entity.
    ShowConstraints {
        /// Accepted entity name.
        entity: String,
        /// Accepted constraint descriptions.
        constraints: Vec<EntityConstraintDescription>,
    },
    /// Index names for an entity.
    ShowIndexes {
        /// Accepted entity name.
        entity: String,
        /// Accepted index names.
        indexes: Vec<String>,
    },
    /// Accepted column metadata.
    ShowColumns(SqlShowColumnsOutput),
    /// Accepted relationship metadata.
    ShowRelations(SqlShowRelationsOutput),
    /// Database entity catalog.
    ShowEntities {
        /// Entity catalog entries.
        entities: Vec<EntityCatalogDescription>,
        /// Whether detailed catalog output was requested.
        verbose: bool,
    },
    /// Database store catalog.
    ShowStores {
        /// Store catalog entries.
        stores: Vec<StoreCatalogDescription>,
        /// Whether detailed catalog output was requested.
        verbose: bool,
    },
    /// Database memory catalog.
    ShowMemory {
        /// Memory catalog entries.
        memory: Vec<MemoryCatalogDescription>,
    },
    /// Result or bounded progress of a schema mutation.
    Ddl {
        /// Accepted entity name.
        entity: String,
        /// Kind of schema mutation performed.
        mutation_kind: String,
        /// Index targeted by this mutation, when applicable.
        target_index: String,
        /// Store targeted by this mutation, when applicable.
        target_store: String,
        /// Field segments targeted by this mutation, when applicable.
        field_path: Vec<String>,
        /// Engine-reported mutation status.
        status: String,
        /// Cumulative rows scanned by this mutation.
        rows_scanned: u64,
        /// Cumulative index keys written by this mutation.
        index_keys_written: u64,
        /// Typed progress when this statement advances constraint validation.
        constraint_validation: Option<SqlConstraintValidationOutput>,
    },
}

impl SqlQueryResult {
    // Measure the exact native success payload plus its IC Wire prefix.
    // Core calls this with an empty canonical projection to establish framing.
    pub(in crate::db) fn encoded_returning_response_len(
        projection: RowProjectionOutput,
    ) -> Result<usize, icydb_schema::wire::Error> {
        let response: Result<Self, Error> = Ok(Self::Projection(projection));
        ic_wire::native::encoded_frame_len(&response, icydb_schema::wire::sizing_limits())
    }

    /// Reject a success whose exact public Wire envelope cannot be delivered
    /// by the deployed IC query boundary.
    #[doc(hidden)]
    pub fn into_deliverable_query_reply(self) -> Result<Self, Error> {
        crate::wire::reply::into_deliverable_query_reply(
            self,
            crate::diagnostic::RuntimeBoundaryCode::SqlQueryReplyBytesExceeded,
        )
    }
}

#[cfg(test)]
mod tests;
