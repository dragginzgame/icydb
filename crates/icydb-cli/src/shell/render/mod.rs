//! Module: shell::render
//! Responsibility: SQL reports, terminal layout, and shell output separation.
//! Does not own: SQL value formatting, canister calls, or query execution.
//! Boundary: structured SQL results -> escaped CLI text and prompt separation.

pub(crate) mod reports;

use icydb::db::sql::SqlQueryResult;
use reports::{
    SqlDdlRenderInput, render_constraint_validation_finding_line, render_count_lines,
    render_describe_output_lines, render_explain_lines, render_grouped_lines,
    render_query_rows_lines, render_show_columns_lines, render_show_constraints_lines,
    render_show_entities_lines, render_show_entities_verbose_lines, render_show_indexes_lines,
    render_show_memory_lines, render_show_relations_lines, render_show_stores_lines,
    render_show_stores_verbose_lines, render_sql_ddl_lines,
};
use std::borrow::Cow;

// Keep successful command output visually isolated so the next prompt or shell
// continuation appears after one blank separator line.
pub(super) fn finalize_successful_command_output(rendered: &str) -> String {
    let mut finalized = String::with_capacity(rendered.len().saturating_add(2));
    finalized.push_str(rendered);
    finalized.push('\n');
    finalized.push('\n');

    finalized
}

/// Render this payload into deterministic shell-friendly lines.
#[must_use]
pub(crate) fn render_lines(result: &SqlQueryResult) -> Vec<String> {
    let mut lines = match result {
        SqlQueryResult::Count { entity, row_count } => {
            render_count_lines(entity.as_str(), *row_count)
        }
        SqlQueryResult::Projection(rows) => render_query_rows_lines(rows),
        SqlQueryResult::Grouped(rows) => render_grouped_lines(rows),
        SqlQueryResult::Explain { explain, .. } => render_explain_lines(explain.as_str()),
        SqlQueryResult::Describe(output) => render_describe_output_lines(output),
        SqlQueryResult::ShowConstraints {
            entity,
            constraints,
        } => render_show_constraints_lines(entity.as_str(), constraints.as_slice()),
        SqlQueryResult::ShowIndexes { entity, indexes } => {
            render_show_indexes_lines(entity.as_str(), indexes.as_slice())
        }
        SqlQueryResult::ShowColumns(output) => render_show_columns_lines(output),
        SqlQueryResult::ShowRelations(output) => render_show_relations_lines(output),
        SqlQueryResult::ShowEntities { entities, verbose } => {
            if *verbose {
                render_show_entities_verbose_lines(entities.as_slice())
            } else {
                render_show_entities_lines(entities.as_slice())
            }
        }
        SqlQueryResult::ShowStores { stores, verbose } => {
            if *verbose {
                render_show_stores_verbose_lines(stores.as_slice())
            } else {
                render_show_stores_lines(stores.as_slice())
            }
        }
        SqlQueryResult::ShowMemory { memory } => render_show_memory_lines(memory.as_slice()),
        SqlQueryResult::Ddl {
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
    };
    // Tables already escape cells before measuring widths. Apply the same
    // idempotent policy to report metadata and preserve only owned line breaks.
    for line in &mut lines {
        if let Cow::Owned(escaped) = ic_host_text::escape::terminal_text(line) {
            *line = escaped;
        }
    }

    lines
}

/// Render this payload into one newline-separated display string.
#[must_use]
pub(crate) fn render_text(result: &SqlQueryResult) -> String {
    render_lines(result).join("\n")
}

#[cfg(test)]
mod tests;
