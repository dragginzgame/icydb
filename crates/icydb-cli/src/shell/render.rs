//! Module: shell SQL result rendering.
//! Responsibility: render decoded SQL results.
//! Does not own: canister calls, SQL routing, or query execution.
//! Boundary: converts typed SQL result values into prompt-ready text.

use icydb::db::{
    RowProjectionOutput,
    sql::{
        SqlGroupedRowsOutput, SqlQueryResult, render_grouped_lines,
        render_projection_display_rows_lines,
    },
};

pub(super) fn render_shell_text(result: SqlQueryResult) -> String {
    match result {
        SqlQueryResult::Projection(rows) => render_projection_shell_text(rows),
        SqlQueryResult::Grouped(rows) => render_grouped_shell_text(rows),
        other => other.render_text(),
    }
}

pub(super) fn render_projection_shell_text(rows: RowProjectionOutput) -> String {
    let mut rendered_rows = rows.rendered_rows();
    uppercase_null_cells(rendered_rows.as_mut_slice());

    render_projection_display_rows_lines(
        rows.columns.as_slice(),
        rendered_rows.as_slice(),
        rows.row_count,
    )
    .join("\n")
}

pub(super) fn render_grouped_shell_text(mut rows: SqlGroupedRowsOutput) -> String {
    uppercase_null_cells(rows.rows.as_mut_slice());

    render_grouped_lines(&rows).join("\n")
}

// Keep successful command output visually isolated so the next prompt or shell
// continuation appears after one blank separator line.
pub(super) fn finalize_successful_command_output(rendered: &str) -> String {
    let mut finalized = String::with_capacity(rendered.len().saturating_add(2));
    finalized.push_str(rendered);
    finalized.push('\n');
    finalized.push('\n');

    finalized
}

fn uppercase_null_cells(rows: &mut [Vec<String>]) {
    for row in rows {
        for cell in row {
            if cell == "null" {
                *cell = "NULL".to_string();
            }
        }
    }
}
