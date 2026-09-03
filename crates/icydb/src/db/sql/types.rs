//! Module: db::sql::types
//!
//! Responsibility: public SQL result and rendering facade.
//! Does not own: SQL parsing, lowering, planning, or execution.
//! Boundary: converts executed core SQL outputs into endpoint-friendly payloads.

use crate::{
    ConstraintValidationFindingOutput, Error, ErrorKind, ErrorOrigin, RuntimeErrorKind,
    db::{
        EntityCatalogDescription, EntityConstraintDescription, MemoryCatalogDescription,
        RowProjectionOutput, SqlDescribeOutput, SqlShowColumnsOutput, SqlShowRelationsOutput,
        StoreCatalogDescription,
        sql::table_render::{
            SqlDdlRenderInput, render_constraint_validation_finding_line, render_count_lines,
            render_describe_output_lines, render_explain_lines, render_grouped_lines,
            render_query_rows_lines, render_show_columns_lines, render_show_constraints_lines,
            render_show_entities_lines, render_show_entities_verbose_lines,
            render_show_indexes_lines, render_show_memory_lines, render_show_relations_lines,
            render_show_stores_lines, render_show_stores_verbose_lines, render_sql_ddl_lines,
        },
    },
};

use candid::CandidType;
use serde::Deserialize;

const MAX_PUBLIC_SQL_QUERY_REPLY_BYTES: usize = 3 * 1024 * 1024;

#[cfg_attr(doc, doc = "SqlGroupedRowsOutput\n\nStructured grouped SQL payload.")]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlGroupedRowsOutput {
    pub entity: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub row_count: u32,
    pub next_cursor: Option<String>,
}

#[cfg_attr(
    doc,
    doc = "SqlConstraintValidationOutput\n\nTyped progress from one bounded constraint-validation step."
)]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
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
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
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
    /// Reject a success whose exact public Candid envelope cannot be delivered
    /// by the deployed IC query boundary.
    #[doc(hidden)]
    pub fn into_deliverable_query_reply(self) -> Result<Self, Error> {
        let response: Result<&Self, Error> = Ok(&self);
        let encoded = candid::encode_one(response).map_err(|_| {
            Error::from_kind(
                ErrorKind::Runtime(RuntimeErrorKind::Internal),
                ErrorOrigin::Response,
            )
        })?;
        if encoded.len() > MAX_PUBLIC_SQL_QUERY_REPLY_BYTES {
            return Err(Error::from_runtime_boundary(
                crate::diagnostic::RuntimeBoundaryCode::SqlQueryReplyBytesExceeded,
                ErrorOrigin::Response,
            ));
        }

        Ok(self)
    }

    /// Render this payload into deterministic shell-friendly lines.
    #[must_use]
    pub fn render_lines(&self) -> Vec<String> {
        match self {
            Self::Count { entity, row_count } => render_count_lines(entity.as_str(), *row_count),
            Self::Projection(rows) => render_query_rows_lines(rows),
            Self::Grouped(rows) => render_grouped_lines(rows),
            Self::Explain { explain, .. } => render_explain_lines(explain.as_str()),
            Self::Describe(output) => render_describe_output_lines(output),
            Self::ShowConstraints {
                entity,
                constraints,
            } => render_show_constraints_lines(entity.as_str(), constraints.as_slice()),
            Self::ShowIndexes { entity, indexes } => {
                render_show_indexes_lines(entity.as_str(), indexes.as_slice())
            }
            Self::ShowColumns(output) => render_show_columns_lines(output),
            Self::ShowRelations(output) => render_show_relations_lines(output),
            Self::ShowEntities { entities, verbose } => {
                if *verbose {
                    render_show_entities_verbose_lines(entities.as_slice())
                } else {
                    render_show_entities_lines(entities.as_slice())
                }
            }
            Self::ShowStores { stores, verbose } => {
                if *verbose {
                    render_show_stores_verbose_lines(stores.as_slice())
                } else {
                    render_show_stores_lines(stores.as_slice())
                }
            }
            Self::ShowMemory { memory } => render_show_memory_lines(memory.as_slice()),
            Self::Ddl {
                entity,
                mutation_kind,
                target_index,
                target_store,
                field_path,
                status,
                rows_scanned,
                index_keys_written,
                constraint_validation,
            } => {
                let mut lines = render_sql_ddl_lines(SqlDdlRenderInput {
                    entity: entity.as_str(),
                    mutation_kind: mutation_kind.as_str(),
                    target_index: target_index.as_str(),
                    target_store: target_store.as_str(),
                    field_path: field_path.as_slice(),
                    status: status.as_str(),
                    rows_scanned: *rows_scanned,
                    index_keys_written: *index_keys_written,
                });
                if let Some(validation) = constraint_validation {
                    lines.extend(
                        validation
                            .findings
                            .iter()
                            .map(render_constraint_validation_finding_line),
                    );
                }
                lines
            }
        }
    }

    /// Render this payload into one newline-separated display string.
    #[must_use]
    pub fn render_text(&self) -> String {
        self.render_lines().join("\n")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use candid::types::{CandidType, Label, Type, TypeInner};

    use super::{MAX_PUBLIC_SQL_QUERY_REPLY_BYTES, SqlQueryResult};
    use crate::{Error, ErrorOrigin};

    fn named_fields(ty: Type, expected_kind: &str) -> BTreeSet<String> {
        let fields = match ty.as_ref() {
            TypeInner::Record(fields) if expected_kind == "record" => fields,
            TypeInner::Variant(fields) if expected_kind == "variant" => fields,
            other => panic!("expected Candid {expected_kind}, got {other:?}"),
        };

        fields
            .iter()
            .map(|field| match field.id.as_ref() {
                Label::Named(name) => name.clone(),
                other => panic!("expected named Candid field, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn sql_query_result_candid_shape_always_contains_explain() {
        let variants = named_fields(SqlQueryResult::ty(), "variant");

        assert!(variants.contains("Explain"));
    }

    #[test]
    fn generated_sql_query_reply_guard_measures_the_exact_complete_envelope() {
        let small = SqlQueryResult::ShowIndexes {
            entity: "Entry".to_string(),
            indexes: vec!["by_owner".to_string()],
        };
        let owned: Result<SqlQueryResult, Error> = Ok(small.clone());
        let borrowed: Result<&SqlQueryResult, Error> = Ok(&small);

        assert_eq!(
            candid::encode_one(owned).expect("owned endpoint envelope should encode"),
            candid::encode_one(borrowed).expect("borrowed endpoint envelope should encode"),
            "the zero-copy guard must measure the exact generated endpoint envelope",
        );
        assert_eq!(
            small.into_deliverable_query_reply(),
            Ok(SqlQueryResult::ShowIndexes {
                entity: "Entry".to_string(),
                indexes: vec!["by_owner".to_string()],
            })
        );
    }

    #[test]
    fn generated_sql_query_reply_guard_returns_a_deliverable_typed_error() {
        let oversized = SqlQueryResult::ShowIndexes {
            entity: "Entry".to_string(),
            indexes: vec!["x".repeat(MAX_PUBLIC_SQL_QUERY_REPLY_BYTES)],
        };
        let candidate: Result<&SqlQueryResult, Error> = Ok(&oversized);
        assert!(
            candid::encode_one(candidate)
                .expect("oversized endpoint candidate should remain fallibly encodable")
                .len()
                > MAX_PUBLIC_SQL_QUERY_REPLY_BYTES,
        );

        let error = oversized
            .into_deliverable_query_reply()
            .expect_err("oversized success must become a typed response error");
        assert_eq!(
            error.code(),
            crate::ErrorCode::RUNTIME_BOUNDARY_SQL_QUERY_REPLY_BYTES_EXCEEDED,
        );
        assert_eq!(error.origin(), ErrorOrigin::Response);
        assert!(
            candid::encode_one(Err::<SqlQueryResult, Error>(error))
                .expect("typed oversize error should encode")
                .len()
                <= MAX_PUBLIC_SQL_QUERY_REPLY_BYTES,
        );
    }
}
