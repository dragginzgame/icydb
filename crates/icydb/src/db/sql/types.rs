//! Module: db::sql::types
//!
//! Responsibility: public SQL result and rendering facade.
//! Does not own: SQL parsing, lowering, planning, or execution.
//! Boundary: converts executed core SQL outputs into endpoint-friendly payloads.

use crate::db::sql::table_render::render_explain_lines;
use crate::db::{
    EntityCatalogDescription, EntityConstraintDescription, EntityFieldDescription,
    EntitySchemaDescription, MemoryCatalogDescription, RowProjectionOutput,
    StoreCatalogDescription,
    sql::table_render::{
        SqlDdlRenderInput, render_constraint_diagnostic_line, render_count_lines,
        render_describe_lines, render_grouped_lines, render_query_rows_lines,
        render_show_columns_lines, render_show_constraints_lines, render_show_entities_lines,
        render_show_entities_verbose_lines, render_show_indexes_lines, render_show_memory_lines,
        render_show_stores_lines, render_show_stores_verbose_lines, render_sql_ddl_lines,
    },
};

use candid::CandidType;
use serde::Deserialize;

use crate::ConstraintDiagnostic;

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
    pub findings: Vec<ConstraintDiagnostic>,
    /// Whether validation and accepted publication are complete.
    pub complete: bool,
}

/// Stable result envelope returned by the fixed administrative SQL query
/// endpoint.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlQueryPerfResult {
    /// Executed SQL result.
    pub result: SqlQueryResult,
    /// Total local instructions attributed to compilation and execution.
    pub instructions: u64,
    /// Planner-local instruction attribution.
    pub planner_instructions: u64,
    /// Store-local instruction attribution.
    pub store_instructions: u64,
    /// Executor-local instruction attribution.
    pub executor_instructions: u64,
    /// Pure-covering decode instruction attribution.
    pub pure_covering_decode_instructions: u64,
    /// Pure-covering row-assembly instruction attribution.
    pub pure_covering_row_assembly_instructions: u64,
    /// Response decode instruction attribution.
    pub decode_instructions: u64,
    /// SQL compiler instruction attribution.
    pub compiler_instructions: u64,
}

impl SqlQueryPerfResult {
    /// Construct the fixed endpoint response from maintained SQL attribution.
    #[doc(hidden)]
    #[must_use]
    pub fn from_attribution(
        result: SqlQueryResult,
        attribution: crate::db::SqlQueryPerfAttribution,
    ) -> Self {
        Self {
            result,
            instructions: attribution.total_local_instructions,
            planner_instructions: attribution.execution.planner_local_instructions,
            store_instructions: attribution.execution.store_local_instructions,
            executor_instructions: attribution.execution.executor_local_instructions,
            pure_covering_decode_instructions: attribution
                .pure_covering
                .map_or(0, |pure_covering| pure_covering.decode_local_instructions),
            pure_covering_row_assembly_instructions: attribution
                .pure_covering
                .map_or(0, |pure_covering| {
                    pure_covering.row_assembly_local_instructions
                }),
            decode_instructions: attribution.response_decode_local_instructions,
            compiler_instructions: attribution.compile_local_instructions,
        }
    }
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
    Describe(EntitySchemaDescription),
    ShowConstraints {
        entity: String,
        constraints: Vec<EntityConstraintDescription>,
    },
    ShowIndexes {
        entity: String,
        indexes: Vec<String>,
    },
    ShowColumns {
        entity: String,
        columns: Vec<EntityFieldDescription>,
    },
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
    /// Render this payload into deterministic shell-friendly lines.
    #[must_use]
    pub fn render_lines(&self) -> Vec<String> {
        match self {
            Self::Count { entity, row_count } => render_count_lines(entity.as_str(), *row_count),
            Self::Projection(rows) => render_query_rows_lines(rows),
            Self::Grouped(rows) => render_grouped_lines(rows),
            Self::Explain { explain, .. } => render_explain_lines(explain.as_str()),
            Self::Describe(description) => render_describe_lines(description),
            Self::ShowConstraints {
                entity,
                constraints,
            } => render_show_constraints_lines(entity.as_str(), constraints.as_slice()),
            Self::ShowIndexes { entity, indexes } => {
                render_show_indexes_lines(entity.as_str(), indexes.as_slice())
            }
            Self::ShowColumns { entity, columns } => {
                render_show_columns_lines(entity.as_str(), columns.as_slice())
            }
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
                            .map(render_constraint_diagnostic_line),
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

    use super::{SqlQueryPerfResult, SqlQueryResult};

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
    fn sql_query_perf_result_owns_the_fixed_public_candid_record() {
        let fields = named_fields(SqlQueryPerfResult::ty(), "record");
        let expected = BTreeSet::from([
            "compiler_instructions".to_string(),
            "decode_instructions".to_string(),
            "executor_instructions".to_string(),
            "instructions".to_string(),
            "planner_instructions".to_string(),
            "pure_covering_decode_instructions".to_string(),
            "pure_covering_row_assembly_instructions".to_string(),
            "result".to_string(),
            "store_instructions".to_string(),
        ]);

        assert_eq!(fields, expected);
    }
}
