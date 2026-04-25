//! Module: db::session::sql::execute::write_returning
//! Responsibility: SQL write `RETURNING` and statement-result shaping.
//! Does not own: SQL write selection, mutation execution, or patch construction.
//! Boundary: converts already-mutated rows into the public SQL statement result shape.

use crate::{
    db::{
        PersistedRow, QueryError,
        session::sql::{
            SqlStatementResult,
            projection::{SqlProjectionPayload, projection_labels_from_fields},
        },
        sql::parser::SqlReturningProjection,
    },
    traits::{CanisterKind, EntityValue},
    value::Value,
};

/// Shape one SQL INSERT/UPDATE mutation result after the write has already run.
///
/// This helper owns only the statement-result envelope conversion for rows that
/// were returned by the mutation path. It intentionally does not select rows,
/// build patches, or perform mutation execution.
pub(in crate::db::session::sql::execute) fn sql_write_statement_result<C, E>(
    entities: Vec<E>,
    returning: Option<&SqlReturningProjection>,
) -> Result<SqlStatementResult, QueryError>
where
    C: CanisterKind,
    E: PersistedRow<Canister = C> + EntityValue,
{
    let row_count = u32::try_from(entities.len()).unwrap_or(u32::MAX);

    match returning {
        None => Ok(SqlStatementResult::Count { row_count }),
        Some(returning) => {
            // Rebuild returning rows directly here so insert/update stop
            // bouncing through one projection wrapper and one row wrapper
            // before they reach the real RETURNING shaper.
            let columns = projection_labels_from_fields(E::MODEL.fields());
            let mut rows = Vec::with_capacity(entities.len());

            for entity in entities {
                let mut row = Vec::with_capacity(E::MODEL.fields().len());

                for index in 0..E::MODEL.fields().len() {
                    let value = entity.get_value_by_index(index).ok_or_else(|| {
                        QueryError::invariant(
                            "SQL write statement projection row must include every declared field",
                        )
                    })?;
                    row.push(value);
                }

                rows.push(row);
            }

            sql_returning_statement_projection(columns, rows, row_count, returning)
        }
    }
}

/// Apply one SQL `RETURNING` projection to materialized mutation rows.
///
/// The caller supplies rows that already match the target entity column order.
/// This boundary keeps SQL write execution separate from the public statement
/// result shape used by session SQL responses.
pub(in crate::db::session::sql::execute) fn sql_returning_statement_projection(
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    row_count: u32,
    returning: &SqlReturningProjection,
) -> Result<SqlStatementResult, QueryError> {
    match returning {
        SqlReturningProjection::All => Ok(SqlProjectionPayload::new(
            columns,
            vec![None; rows.first().map_or(0, Vec::len)],
            rows,
            row_count,
        )
        .into_statement_result()),
        SqlReturningProjection::Fields(fields) => {
            let mut indices = Vec::with_capacity(fields.len());

            for field in fields {
                let index = columns
                    .iter()
                    .position(|column| column == field)
                    .ok_or_else(|| {
                        QueryError::unsupported_query(format!(
                            "SQL RETURNING field '{field}' does not exist on the target entity"
                        ))
                    })?;
                indices.push(index);
            }

            let mut projected_rows = Vec::with_capacity(rows.len());
            for row in rows {
                let mut projected = Vec::with_capacity(indices.len());
                for index in &indices {
                    let value = row.get(*index).ok_or_else(|| {
                        QueryError::invariant(
                            "SQL RETURNING projection row must align with declared columns",
                        )
                    })?;
                    projected.push(value.clone());
                }
                projected_rows.push(projected);
            }

            Ok(SqlProjectionPayload::new(
                fields.clone(),
                vec![None; fields.len()],
                projected_rows,
                row_count,
            )
            .into_statement_result())
        }
    }
}
