use super::{sql_returning_response_too_large_error, validate_sql_returning_row_count};
use icydb_diagnostic_code::DiagnosticFactTag;

#[test]
fn returning_bounds_match_complete_wire_frames_at_length_edges() {
    use super::*;
    use crate::db::schema::empty_accepted_enum_catalog_for_tests;

    fn projection_len(projection: RowProjectionOutput) -> Result<usize, icydb_schema::wire::Error> {
        icydb_schema::wire::encode_one(projection).map(|bytes| bytes.len())
    }
    fn result_len(projection: RowProjectionOutput) -> Result<usize, icydb_schema::wire::Error> {
        icydb_schema::wire::encode_one(Ok::<_, ()>(projection)).map(|bytes| bytes.len())
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
            .map(|row| output_row_from_runtime(&catalog, row).unwrap())
            .collect();
        let output = RowProjectionOutput {
            entity: "Row".into(),
            columns: columns.to_vec(),
            rows: output_rows,
            row_count: u32::try_from(count).unwrap(),
        };
        for encoder in [
            projection_len,
            result_len,
            crate::db::session::sql::encoded_returning_response_len,
        ] {
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
fn returning_wire_admission_checks_aggregate_resources_before_commit() {
    use super::*;
    use crate::db::schema::empty_accepted_enum_catalog_for_tests;

    let catalog = empty_accepted_enum_catalog_for_tests();
    let columns: Vec<_> = (0..1024).map(|index| format!("field_{index}")).collect();
    let rows = vec![vec![Value::Unit; columns.len()]; 256];
    let row = output_row_from_runtime(&catalog, rows[0].clone()).unwrap();
    let row_len = icydb_schema::wire::encoded_len(&row).unwrap();
    assert!(row_len * rows.len() + 16 * 1024 < icydb_schema::wire::MAX_REPLY_BYTES);

    // Every row individually admits, but the complete reply exhausts the
    // shared native resource allowance despite fitting the byte ceiling.
    assert!(
        validate_sql_materialized_returning_bounds(
            crate::db::session::sql::encoded_returning_response_len,
            "Row",
            &columns,
            &rows,
            &SqlReturningProjection::All,
            &catalog,
            Some(SqlWriteReturningBounds {
                max_rows: None,
                max_response_bytes: Some(icydb_schema::wire::MAX_REPLY_BYTES.try_into().unwrap()),
            }),
        )
        .is_err()
    );
}

#[test]
fn returning_prefix_refusal_and_overflow_do_not_claim_full_length() {
    use super::*;
    assert_eq!(
        encoded_sql_returning_rows_len_exceeds_max(
            usize::MAX,
            usize::MAX,
            [Ok(vec![OutputValue::nat64(1)])].into_iter(),
            &mut Vec::new(),
        )
        .unwrap(),
        SqlReturningLengthCheck::Exceeded(None)
    );
    assert_eq!(
        encoded_sql_returning_rows_len_exceeds_max(
            0,
            0,
            [Ok(vec![OutputValue::nat64(1)]), Ok(vec![])].into_iter(),
            &mut Vec::new(),
        )
        .unwrap(),
        SqlReturningLengthCheck::Exceeded(None)
    );
    assert_eq!(
        encoded_sql_returning_rows_len_exceeds_max(
            1,
            0,
            [Ok(vec![])].into_iter(),
            &mut Vec::new(),
        )
        .unwrap(),
        SqlReturningLengthCheck::Exceeded(None)
    );
    assert_eq!(
        encoded_sql_returning_rows_len_exceeds_max(1, 0, [].into_iter(), &mut Vec::new(),).unwrap(),
        SqlReturningLengthCheck::Exceeded(Some(1))
    );
}

#[test]
fn returning_byte_check_retains_admitted_rows_and_stops_at_refusal() {
    use super::*;
    use std::cell::Cell;

    let rows = vec![vec![OutputValue::text("returned value".into())]; 3];
    let row_len = encoded_payload_value_len(&rows[0]).unwrap();
    for (limit, retained, projected, expected) in [
        (row_len, 1, 2, SqlReturningLengthCheck::Exceeded(None)),
        (
            row_len * 2,
            2,
            3,
            SqlReturningLengthCheck::Exceeded(Some(row_len * 3)),
        ),
        (row_len * 3, 3, 3, SqlReturningLengthCheck::WithinLimit),
    ] {
        let projections = Cell::new(0);
        let mut admitted = Vec::new();
        let result = encoded_sql_returning_rows_len_exceeds_max(
            0,
            limit,
            rows.iter().cloned().map(|row| {
                projections.set(projections.get() + 1);
                Ok(row)
            }),
            &mut admitted,
        )
        .unwrap();

        assert_eq!(result, expected);
        assert_eq!(projections.get(), projected);
        assert_eq!(admitted, rows[..retained]);
    }
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
