//! Module: db::session::sql::execute::write_returning::bounds
//! Responsibility: SQL write `RETURNING` row-count and response-byte budget enforcement.
//! Does not own: mutation execution or public SQL statement-result projection.
//! Boundary: validates prepared mutation after-images before commit or response shaping.

use crate::{
    db::{
        RowProjectionOutput, schema::AcceptedEnumCatalog,
        session::sql::write_policy::SqlWriteReturningBounds, sql::parser::SqlReturningProjection,
    },
    error::InternalError,
    value::{OutputValue, Value},
};
use candid::{CandidType, ser::ValueSerializer};
use icydb_diagnostic_code::{DiagnosticFactTag, SqlWriteBoundaryCode};

use super::projection::{
    SqlReturningFieldProjection, query_error_to_internal_invariant, sql_returning_output_value_row,
};

// Serialize only value bytes: each row contributes its value encoding, never
// another DIDL header/type table. Candid remains the encoding authority.
fn encoded_candid_value_len(value: &impl CandidType) -> Result<usize, InternalError> {
    let mut serializer = ValueSerializer::new();
    value
        .idl_serialize(&mut serializer)
        .map_err(|_| InternalError::query_executor_invariant())?;
    Ok(serializer.get_result().len())
}

fn encoded_candid_vec_prefix_len(count: usize) -> Result<usize, InternalError> {
    let mut serializer = ValueSerializer::new();
    let count = u64::try_from(count).map_err(|_| InternalError::query_executor_invariant())?;
    serializer
        .write_leb128(count)
        .map_err(|_| InternalError::query_executor_invariant())?;
    Ok(serializer.get_result().len())
}

/// Validate SQL write `RETURNING` bounds for rows that are already materialized
/// in accepted-schema column order.
pub(in crate::db::session::sql::execute) fn validate_sql_materialized_returning_bounds(
    response_len: fn(RowProjectionOutput) -> candid::Result<usize>,
    entity_name: &str,
    columns: &[String],
    rows: &[Vec<Value>],
    returning: &SqlReturningProjection,
    enum_catalog: &AcceptedEnumCatalog,
    bounds: Option<SqlWriteReturningBounds>,
) -> Result<(), InternalError> {
    let Some(bounds) = bounds else {
        return Ok(());
    };

    validate_sql_returning_row_count(rows.len(), bounds.max_rows)?;

    if let Some(max_response_bytes) = bounds.max_response_bytes {
        let max_response_bytes = usize::try_from(max_response_bytes).unwrap_or(usize::MAX);
        if let SqlReturningLengthCheck::Exceeded(actual_length) =
            encoded_sql_materialized_returning_projection_response_len_check(
                response_len,
                entity_name,
                columns,
                rows,
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
    response_len: fn(RowProjectionOutput) -> candid::Result<usize>,
    entity_name: &str,
    columns: &[String],
    rows: &[Vec<Value>],
    returning: &SqlReturningProjection,
    enum_catalog: &AcceptedEnumCatalog,
    max_response_bytes: usize,
) -> Result<SqlReturningLengthCheck, InternalError> {
    let projection = match returning {
        SqlReturningProjection::All => None,
        SqlReturningProjection::Fields(fields) => Some(
            SqlReturningFieldProjection::from_fields(columns, fields)
                .map_err(query_error_to_internal_invariant)?,
        ),
    };
    let columns = projection.as_ref().map_or_else(
        || columns.to_vec(),
        SqlReturningFieldProjection::output_columns,
    );
    let empty_len = response_len(RowProjectionOutput {
        entity: entity_name.to_string(),
        columns,
        rows: Vec::new(),
        row_count: u32::try_from(rows.len()).unwrap_or(u32::MAX),
    })
    .map_err(|_| InternalError::query_executor_invariant())?;
    let prefix_len = encoded_candid_vec_prefix_len(rows.len())?;
    let empty_prefix_len = encoded_candid_vec_prefix_len(0)?;
    let Some(base_len) = empty_len
        .checked_sub(empty_prefix_len)
        .and_then(|len| len.checked_add(prefix_len))
    else {
        return Ok(SqlReturningLengthCheck::Exceeded(None));
    };
    encoded_sql_returning_rows_len_exceeds_max(
        base_len,
        max_response_bytes,
        rows.iter().map(|row| match &projection {
            None => sql_returning_output_value_row(enum_catalog, row.clone())
                .map_err(query_error_to_internal_invariant),
            Some(projection) => projection
                .project_borrowed_row(row)
                .and_then(|row| sql_returning_output_value_row(enum_catalog, row))
                .map_err(query_error_to_internal_invariant),
        }),
    )
}

fn encoded_sql_returning_rows_len_exceeds_max(
    mut response_len: usize,
    max_response_bytes: usize,
    rows: impl ExactSizeIterator<Item = Result<Vec<OutputValue>, InternalError>>,
) -> Result<SqlReturningLengthCheck, InternalError> {
    let row_count = rows.len();
    if response_len > max_response_bytes {
        return Ok(SqlReturningLengthCheck::Exceeded(
            (row_count == 0).then_some(response_len),
        ));
    }
    for (index, row) in rows.enumerate() {
        let row_len = encoded_candid_value_len(&row?)?;
        let Some(next_len) = response_len.checked_add(row_len) else {
            return Ok(SqlReturningLengthCheck::Exceeded(None));
        };
        response_len = next_len;
        if response_len > max_response_bytes {
            // A prefix proves refusal but is not the full reply's ActualLength.
            return Ok(SqlReturningLengthCheck::Exceeded(
                (index + 1 == row_count).then_some(response_len),
            ));
        }
    }
    Ok(SqlReturningLengthCheck::WithinLimit)
}

#[cfg(test)]
mod tests {
    use super::{sql_returning_response_too_large_error, validate_sql_returning_row_count};
    use icydb_diagnostic_code::DiagnosticFactTag;

    #[test]
    fn returning_bounds_match_complete_candid_frames_at_length_edges() {
        use super::*;
        use crate::db::schema::empty_accepted_enum_catalog_for_tests;

        fn projection_len(projection: RowProjectionOutput) -> candid::Result<usize> {
            candid::encode_one(projection).map(|bytes| bytes.len())
        }
        fn result_len(projection: RowProjectionOutput) -> candid::Result<usize> {
            candid::encode_one(Ok::<_, ()>(projection)).map(|bytes| bytes.len())
        }
        let catalog = empty_accepted_enum_catalog_for_tests();
        let columns = ["text", "blob", "number", "nested"].map(str::to_string);
        for (count, size) in [
            (0, 0),
            (1, 127),
            (1, 128),
            (127, 128),
            (128, 128),
            (1, 16_383),
            (1, 16_384),
            (1, 1_048_000),
            (100, 10_300),
        ] {
            let row = vec![
                Value::Text("x".repeat(size)),
                Value::Blob(vec![7; 128]),
                Value::Int64(-129),
                Value::List(vec![
                    Value::Null,
                    Value::Map(vec![(Value::Text("flag".into()), Value::Bool(true))]),
                ]),
            ];
            let rows = vec![row; count];
            let output_rows = rows
                .iter()
                .cloned()
                .map(|row| sql_returning_output_value_row(&catalog, row).unwrap())
                .collect();
            let output = RowProjectionOutput {
                entity: "Row".into(),
                columns: columns.to_vec(),
                rows: output_rows,
                row_count: u32::try_from(count).unwrap(),
            };
            for encoder in [projection_len, result_len] {
                let exact = encoder(output.clone()).unwrap();
                for limit in [exact - 1, exact, exact + 1] {
                    let result = encoded_sql_materialized_returning_projection_response_len_check(
                        encoder,
                        "Row",
                        &columns,
                        &rows,
                        &SqlReturningProjection::All,
                        &catalog,
                        limit,
                    )
                    .unwrap();
                    assert_eq!(
                        matches!(result, SqlReturningLengthCheck::WithinLimit),
                        limit >= exact
                    );
                    if let SqlReturningLengthCheck::Exceeded(Some(length)) = result {
                        assert_eq!(length, exact);
                    }
                }
            }
        }
    }

    #[test]
    fn returning_prefix_refusal_and_overflow_do_not_claim_full_length() {
        use super::*;
        assert_eq!(
            encoded_sql_returning_rows_len_exceeds_max(
                usize::MAX,
                usize::MAX,
                [Ok(vec![OutputValue::nat64(1)])].into_iter(),
            )
            .unwrap(),
            SqlReturningLengthCheck::Exceeded(None)
        );
        assert_eq!(
            encoded_sql_returning_rows_len_exceeds_max(
                0,
                0,
                [Ok(vec![OutputValue::nat64(1)]), Ok(vec![])].into_iter(),
            )
            .unwrap(),
            SqlReturningLengthCheck::Exceeded(None)
        );
        assert_eq!(
            encoded_sql_returning_rows_len_exceeds_max(1, 0, [Ok(vec![])].into_iter(),).unwrap(),
            SqlReturningLengthCheck::Exceeded(None)
        );
        assert_eq!(
            encoded_sql_returning_rows_len_exceeds_max(1, 0, [].into_iter(),).unwrap(),
            SqlReturningLengthCheck::Exceeded(Some(1))
        );
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
        let bounds = Some(SqlWriteReturningBounds {
            max_rows: Some(1),
            max_response_bytes: Some(4096),
        });
        validate_sql_materialized_returning_bounds(
            crate::db::session::sql::encoded_returning_response_len,
            "Row",
            &columns,
            &rows,
            &returning,
            &catalog,
            bounds,
        )
        .unwrap();
        let error = validate_sql_materialized_returning_bounds(
            crate::db::session::sql::encoded_returning_response_len,
            "Row",
            &columns,
            &rows,
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
