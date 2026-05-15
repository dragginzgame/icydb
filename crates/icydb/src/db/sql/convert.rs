use crate::db::sql::{
    SqlGroupedRowsOutput, SqlProjectionRows, SqlQueryResult, SqlQueryRowsOutput,
    value_render::{render_projection_rows, render_projection_value_text},
};
use icydb_core::db::{GroupedRow, SqlStatementResult};

pub(crate) fn sql_query_result_from_statement(
    result: SqlStatementResult,
    entity_name: String,
) -> SqlQueryResult {
    match result {
        SqlStatementResult::Count { row_count } => SqlQueryResult::Count {
            entity: entity_name,
            row_count,
        },
        SqlStatementResult::Projection {
            columns,
            fixed_scales,
            rows,
            row_count,
        } => {
            // Preserve projection-local display contracts such as
            // `ROUND(..., scale)` before packaging the outward shell rows.
            let rows = render_projection_rows(columns.as_slice(), fixed_scales.as_slice(), rows);

            SqlQueryResult::Projection(SqlQueryRowsOutput::from_projection(
                entity_name,
                SqlProjectionRows::new(columns, rows, row_count),
            ))
        }
        SqlStatementResult::ProjectionText {
            columns,
            rows,
            row_count,
        } => SqlQueryResult::Projection(SqlQueryRowsOutput::from_projection(
            entity_name,
            SqlProjectionRows::new(columns, rows, row_count),
        )),
        SqlStatementResult::Grouped {
            columns,
            fixed_scales,
            rows,
            row_count,
            next_cursor,
        } => SqlQueryResult::Grouped(sql_grouped_rows_output(
            entity_name,
            columns,
            fixed_scales,
            rows,
            row_count,
            next_cursor,
        )),
        SqlStatementResult::Explain(explain) => SqlQueryResult::Explain {
            entity: entity_name,
            explain,
        },
        SqlStatementResult::Describe(description) => SqlQueryResult::Describe(description),
        SqlStatementResult::ShowIndexes(indexes) => SqlQueryResult::ShowIndexes {
            entity: entity_name,
            indexes,
        },
        SqlStatementResult::ShowColumns(columns) => SqlQueryResult::ShowColumns {
            entity: entity_name,
            columns,
        },
        SqlStatementResult::ShowEntities(entities) => SqlQueryResult::ShowEntities { entities },
        SqlStatementResult::Ddl(report) => SqlQueryResult::Ddl {
            entity: entity_name,
            mutation_kind: report.mutation_kind().as_str().to_string(),
            target_index: report.target_index().to_string(),
            target_store: report.target_store().to_string(),
            field_path: report.field_path().to_vec(),
            status: report.execution_status().as_str().to_string(),
            rows_scanned: usize_to_u64_saturating(report.rows_scanned()),
            index_keys_written: usize_to_u64_saturating(report.index_keys_written()),
        },
    }
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn sql_grouped_rows_output(
    entity_name: String,
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
    rows: Vec<GroupedRow>,
    row_count: u32,
    next_cursor: Option<String>,
) -> SqlGroupedRowsOutput {
    let rows = rows
        .into_iter()
        .map(|row| {
            row.group_key()
                .iter()
                .chain(row.aggregate_values().iter())
                .enumerate()
                .map(|(index, value)| {
                    render_projection_value_text(
                        columns.get(index),
                        fixed_scales.get(index).copied().flatten(),
                        value,
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    SqlGroupedRowsOutput {
        entity: entity_name,
        columns,
        rows,
        row_count,
        next_cursor,
    }
}
