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

#[cfg_attr(doc, doc = "SqlGroupedRowsOutput\n\nStructured grouped SQL payload.")]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub struct SqlGroupedRowsOutput {
    pub entity: String,
    pub columns: Vec<String>,
    /// SQL display cells: NULL is uppercase and marker-like text is quoted.
    /// Values are formatted before conversion; consumers must not infer types
    /// from these strings or rewrite them.
    pub rows: Vec<Vec<String>>,
    pub row_count: u32,
    pub next_cursor: Option<String>,
}

#[cfg_attr(
    doc,
    doc = "SqlConstraintValidationOutput\n\nTyped progress from one bounded constraint-validation step."
)]
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

#[cfg_attr(doc, doc = "SqlQueryResult\n\nUnified SQL endpoint result.")]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub enum SqlQueryResult {
    Count {
        entity: String,
        row_count: u32,
    },
    Projection(RowProjectionOutput),
    Grouped(SqlGroupedRowsOutput),
    Explain {
        entity: String,
        explain: String,
    },
    Describe(SqlDescribeOutput),
    ShowConstraints {
        entity: String,
        constraints: Vec<EntityConstraintDescription>,
    },
    ShowIndexes {
        entity: String,
        indexes: Vec<String>,
    },
    ShowColumns(SqlShowColumnsOutput),
    ShowRelations(SqlShowRelationsOutput),
    ShowEntities {
        entities: Vec<EntityCatalogDescription>,
        verbose: bool,
    },
    ShowStores {
        stores: Vec<StoreCatalogDescription>,
        verbose: bool,
    },
    ShowMemory {
        memory: Vec<MemoryCatalogDescription>,
    },
    Ddl {
        entity: String,
        mutation_kind: String,
        target_index: String,
        target_store: String,
        field_path: Vec<String>,
        status: String,
        rows_scanned: u64,
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
