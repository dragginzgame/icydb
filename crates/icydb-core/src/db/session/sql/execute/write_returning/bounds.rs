//! Module: db::session::sql::execute::write_returning::bounds
//! Responsibility: SQL write `RETURNING` row-count and response-byte budget enforcement.
//! Does not own: mutation execution or public SQL statement-result projection.
//! Boundary: validates prepared mutation after-images before commit or response shaping.

use crate::{
    db::{
        schema::AcceptedEnumCatalog, session::sql::write_policy::SqlWriteReturningBounds,
        sql::parser::SqlReturningProjection,
    },
    error::InternalError,
    value::{OutputValue, Value},
};
use candid::{CandidType, ser::IDLBuilder};
use icydb_diagnostic_code::{DiagnosticFactTag, SqlWriteBoundaryCode};
use std::io::{self, Write};

use super::projection::{
    SqlReturningFieldProjection, SqlReturningProjectionRows, query_error_to_internal_invariant,
    sql_materialized_returning_projection_rows, sql_returning_output_value_row,
};

#[derive(CandidType)]
enum SqlReturningResponseSizeProbe {
    Projection(SqlReturningProjectionSizeProbe),
}

#[derive(CandidType)]
struct SqlReturningProjectionSizeProbe {
    entity: String,
    columns: Vec<String>,
    rows: Vec<Vec<OutputValue>>,
    row_count: u32,
}

// Count the maintained encoder's output without retaining another payload copy.
// Candid still owns its internal value buffer and all encoding decisions.
#[derive(Default)]
struct EncodedLength(usize);

impl Write for EncodedLength {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or(io::ErrorKind::InvalidData)?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encoded_candid_len(value: &impl CandidType) -> Result<usize, InternalError> {
    let mut length = EncodedLength::default();
    IDLBuilder::new()
        .arg(value)
        .and_then(|builder| builder.serialize(&mut length))
        .map_err(|_| InternalError::query_executor_invariant())?;
    Ok(length.0)
}

/// Validate SQL write `RETURNING` bounds for rows that are already materialized
/// in accepted-schema column order.
pub(in crate::db::session::sql::execute) fn validate_sql_materialized_returning_bounds(
    entity_name: &str,
    columns: &[String],
    rows: &[Vec<Value>],
    row_count: u32,
    returning: &SqlReturningProjection,
    enum_catalog: &AcceptedEnumCatalog,
    bounds: Option<SqlWriteReturningBounds>,
) -> Result<(), InternalError> {
    let Some(bounds) = bounds else {
        return Ok(());
    };

    validate_sql_returning_row_count(
        usize::try_from(row_count).unwrap_or(usize::MAX),
        bounds.max_rows,
    )?;

    if let Some(max_response_bytes) = bounds.max_response_bytes {
        let max_response_bytes = usize::try_from(max_response_bytes).unwrap_or(usize::MAX);
        if let SqlReturningLengthCheck::Exceeded(actual_length) =
            encoded_sql_materialized_returning_projection_response_len_check(
                entity_name,
                columns,
                rows,
                row_count,
                returning,
                enum_catalog,
                max_response_bytes,
            )?
        {
            return Err(sql_returning_response_too_large_error(
                actual_length,
                max_response_bytes,
            ));
        }

        let projected = sql_materialized_returning_projection_rows(
            enum_catalog,
            columns,
            rows,
            row_count,
            returning,
        )?;
        let payload_len = encoded_sql_returning_projection_payload_len(entity_name, projected)?;
        if payload_len > max_response_bytes {
            return Err(sql_returning_response_too_large_error(
                Some(payload_len),
                max_response_bytes,
            ));
        }
    }

    Ok(())
}

fn validate_sql_returning_row_count(
    row_count: usize,
    max_rows: Option<u32>,
) -> Result<(), InternalError> {
    let Some(max_rows) = max_rows else {
        return Ok(());
    };
    let max_rows = usize::try_from(max_rows).unwrap_or(usize::MAX);
    if row_count <= max_rows {
        return Ok(());
    }

    Err(InternalError::query_sql_write_boundary_with_facts(
        SqlWriteBoundaryCode::ReturningRowsTooMany,
        vec![
            (DiagnosticFactTag::ActualCount, row_count as u64),
            (DiagnosticFactTag::Limit, max_rows as u64),
        ],
    ))
}

fn sql_returning_response_too_large_error(
    actual_length: Option<usize>,
    max_response_bytes: usize,
) -> InternalError {
    let mut facts = Vec::with_capacity(2);
    if let Some(actual_length) = actual_length {
        facts.push((DiagnosticFactTag::ActualLength, actual_length as u64));
    }
    facts.push((DiagnosticFactTag::Limit, max_response_bytes as u64));
    InternalError::query_sql_write_boundary_with_facts(
        SqlWriteBoundaryCode::ReturningResponseTooLarge,
        facts,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlReturningLengthCheck {
    WithinLimit,
    Exceeded(Option<usize>),
}

fn encoded_sql_materialized_returning_projection_response_len_check(
    entity_name: &str,
    columns: &[String],
    rows: &[Vec<Value>],
    row_count: u32,
    returning: &SqlReturningProjection,
    enum_catalog: &AcceptedEnumCatalog,
    max_response_bytes: usize,
) -> Result<SqlReturningLengthCheck, InternalError> {
    match returning {
        SqlReturningProjection::All => {
            let base_len = encoded_empty_sql_returning_projection_payload_len(
                entity_name,
                columns.to_vec(),
                row_count,
            )?;

            encoded_sql_returning_rows_len_exceeds_max(
                base_len,
                max_response_bytes,
                rows.iter().map(|row| {
                    sql_returning_output_value_row(enum_catalog, row.clone())
                        .map_err(query_error_to_internal_invariant)
                }),
            )
        }
        SqlReturningProjection::Fields(fields) => {
            let projection = SqlReturningFieldProjection::from_fields(columns, fields)
                .map_err(query_error_to_internal_invariant)?;
            let base_len = encoded_empty_sql_returning_projection_payload_len(
                entity_name,
                projection.output_columns(),
                row_count,
            )?;

            encoded_sql_returning_rows_len_exceeds_max(
                base_len,
                max_response_bytes,
                rows.iter().map(|row| {
                    projection
                        .project_borrowed_row(row)
                        .and_then(|row| sql_returning_output_value_row(enum_catalog, row))
                        .map_err(query_error_to_internal_invariant)
                }),
            )
        }
    }
}

fn encoded_empty_sql_returning_projection_payload_len(
    entity_name: &str,
    columns: Vec<String>,
    row_count: u32,
) -> Result<usize, InternalError> {
    encoded_sql_returning_projection_payload_len(
        entity_name,
        SqlReturningProjectionRows {
            columns,
            rows: Vec::new(),
            row_count,
        },
    )
}

fn encoded_sql_returning_rows_len_exceeds_max(
    mut estimated_payload_len: usize,
    max_response_bytes: usize,
    rows: impl Iterator<Item = Result<Vec<OutputValue>, InternalError>>,
) -> Result<SqlReturningLengthCheck, InternalError> {
    if estimated_payload_len > max_response_bytes {
        return Ok(SqlReturningLengthCheck::Exceeded(Some(
            estimated_payload_len,
        )));
    }

    for row in rows {
        let row = row?;
        let row_len = encoded_candid_len(&row)?;
        let Some(next_payload_len) = estimated_payload_len.checked_add(row_len) else {
            return Ok(SqlReturningLengthCheck::Exceeded(None));
        };
        estimated_payload_len = next_payload_len;
        if estimated_payload_len > max_response_bytes {
            return Ok(SqlReturningLengthCheck::Exceeded(Some(
                estimated_payload_len,
            )));
        }
    }

    Ok(SqlReturningLengthCheck::WithinLimit)
}

fn encoded_sql_returning_projection_payload_len(
    entity_name: &str,
    projected: SqlReturningProjectionRows,
) -> Result<usize, InternalError> {
    let payload = SqlReturningResponseSizeProbe::Projection(SqlReturningProjectionSizeProbe {
        entity: entity_name.to_string(),
        columns: projected.columns,
        rows: projected.rows,
        row_count: projected.row_count,
    });
    encoded_candid_len(&payload)
}

#[cfg(test)]
mod tests {
    use super::{sql_returning_response_too_large_error, validate_sql_returning_row_count};
    use icydb_diagnostic_code::DiagnosticFactTag;

    #[test]
    fn counted_returning_lengths_match_candid_bytes_and_limit_edges() {
        use super::*;
        use candid::Encode;

        for size in [0, 127, 128, 16_383, 16_384, 1_050_000] {
            let row = vec![
                OutputValue::text("x".repeat(size)),
                OutputValue::blob(vec![7; 128]),
                OutputValue::int64(-129),
                OutputValue::boolean(true),
            ];
            let row_len = Encode!(&row).unwrap().len();
            assert_eq!(encoded_candid_len(&row).unwrap(), row_len);
            for rows in [Vec::new(), vec![row.clone()]] {
                let payload =
                    SqlReturningResponseSizeProbe::Projection(SqlReturningProjectionSizeProbe {
                        entity: "Row".into(),
                        columns: vec!["text".into(), "blob".into(), "number".into(), "flag".into()],
                        row_count: u32::try_from(rows.len()).unwrap(),
                        rows,
                    });
                assert_eq!(
                    encoded_candid_len(&payload).unwrap(),
                    Encode!(&payload).unwrap().len()
                );
            }
            let base_len = 17;
            let exact_limit = base_len + row_len;
            assert_eq!(
                encoded_sql_returning_rows_len_exceeds_max(
                    base_len,
                    exact_limit,
                    [Ok(row.clone())].into_iter()
                )
                .unwrap(),
                SqlReturningLengthCheck::WithinLimit,
            );
            assert_eq!(
                encoded_sql_returning_rows_len_exceeds_max(
                    base_len,
                    exact_limit - 1,
                    [Ok(row)].into_iter()
                )
                .unwrap(),
                SqlReturningLengthCheck::Exceeded(Some(exact_limit)),
            );
        }
    }

    #[test]
    fn encoded_length_overflow_is_fallible_and_preserves_count() {
        use super::{EncodedLength, Write, io};

        let mut length = EncodedLength(usize::MAX - 1);
        length.write_all(&[0]).unwrap();
        assert_eq!(length.0, usize::MAX);
        assert_eq!(
            length.write(&[0]).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(length.0, usize::MAX);
        length.write_all(&[]).unwrap();
        length.flush().unwrap();
    }

    #[test]
    fn selected_returning_bounds_ignore_unreturned_payloads() {
        use super::*;
        use crate::db::schema::empty_accepted_enum_catalog_for_tests;

        let catalog = empty_accepted_enum_catalog_for_tests();
        let columns = ["id", "blob", "label"].map(str::to_string);
        let rows = vec![vec![
            Value::Nat64(7),
            Value::Blob(vec![3; 64 * 1024]),
            Value::Text("name".into()),
        ]];
        let returning = SqlReturningProjection::Fields(vec!["label".into(), "id".into()]);
        let projected =
            sql_materialized_returning_projection_rows(&catalog, &columns, &rows, 1, &returning)
                .unwrap();
        assert_eq!(projected.columns, ["label", "id"]);
        assert_eq!(
            projected.rows,
            vec![vec![
                OutputValue::text("name".into()),
                OutputValue::nat64(7)
            ]]
        );
        let bounds = Some(SqlWriteReturningBounds {
            max_rows: Some(1),
            max_response_bytes: Some(4096),
        });
        validate_sql_materialized_returning_bounds(
            "Row", &columns, &rows, 1, &returning, &catalog, bounds,
        )
        .unwrap();
        let error = validate_sql_materialized_returning_bounds(
            "Row",
            &columns,
            &rows,
            1,
            &SqlReturningProjection::All,
            &catalog,
            bounds,
        )
        .unwrap_err();
        assert_eq!(
            error.diagnostic().detail(),
            Some(&icydb_diagnostic_code::DiagnosticDetail::SqlWriteBoundary {
                boundary: SqlWriteBoundaryCode::ReturningResponseTooLarge
            })
        );
    }

    #[test]
    fn returning_row_limit_error_retains_actual_count_and_limit() {
        let error = validate_sql_returning_row_count(3, Some(2))
            .expect_err("row count above returning limit should reject");

        assert_eq!(
            error.diagnostic_facts(),
            vec![
                (DiagnosticFactTag::ActualCount, 3),
                (DiagnosticFactTag::Limit, 2),
            ],
        );
    }

    #[test]
    fn returning_byte_limit_error_retains_exact_length_and_limit() {
        let error = sql_returning_response_too_large_error(Some(17), 16);

        assert_eq!(
            error.diagnostic_facts(),
            vec![
                (DiagnosticFactTag::ActualLength, 17),
                (DiagnosticFactTag::Limit, 16),
            ],
        );
    }
}
