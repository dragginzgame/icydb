//! Native SQL conversion, value semantics and exact payload tests.
use crate::db::{
    RowProjectionOutput,
    sql::{SqlGroupedRowsOutput, SqlQueryResult, sql_query_result_from_statement},
};
use crate::value::OutputValue;
use icydb_core::{
    db::{GroupedRow, SqlStatementResult},
    types::{Decimal, Float32, Float64},
};

fn text(value: &str) -> OutputValue {
    OutputValue::text(value.to_string())
}

#[test]
fn sql_query_result_from_statement_preserves_show_constraints_entity() {
    assert_eq!(
        sql_query_result_from_statement(
            SqlStatementResult::ShowConstraints(Vec::new()),
            "ExampleEntity".to_string(),
        ),
        SqlQueryResult::ShowConstraints {
            entity: "ExampleEntity".to_string(),
            constraints: Vec::new(),
        },
    );
}

#[test]
fn sql_null_rendering_distinguishes_typed_projection_values_after_wire() {
    let original_rows = vec![vec![
        OutputValue::null(),
        text("null"),
        text("NULL"),
        text("NuLl"),
        text("alice"),
        text("'null'"),
        text("0.000"),
        text(" NULL "),
    ]];
    let result = sql_query_result_from_statement(
        SqlStatementResult::Projection {
            columns: [
                "missing", "lower", "upper", "mixed", "plain", "quoted", "scale", "padded",
            ]
            .map(str::to_string)
            .to_vec(),
            fixed_scales: vec![None; 8],
            rows: original_rows.clone(),
            row_count: 1,
        },
        "User".into(),
    );
    let response: Result<SqlQueryResult, crate::Error> = Ok(result);
    let bytes =
        icydb_schema::wire::encode_one(&response).expect("projection response should encode");
    let decoded = icydb_schema::wire::decode_one::<Result<SqlQueryResult, crate::Error>>(&bytes)
        .expect("projection response should decode")
        .expect("projection response should succeed");
    let SqlQueryResult::Projection(projection) = &decoded else {
        panic!("expected projection");
    };
    assert_eq!(projection.rows, original_rows);
    assert_eq!(
        crate::db::sql::sql_projection_display_rows(&projection.rows),
        vec![
            vec![
                "NULL", "'null'", "'NULL'", "'NuLl'", "alice", "'null'", "0.000", "' NULL '"
            ]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
        ],
    );
}

#[test]
fn sql_null_rendering_distinguishes_group_keys_and_aggregate_values_after_wire() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Grouped {
            columns: vec!["key".into(), "value".into()],
            fixed_scales: vec![None, Some(3)],
            rows: vec![
                GroupedRow::new(vec![OutputValue::null()], vec![text("null")]),
                GroupedRow::new(vec![text("NULL")], vec![OutputValue::null()]),
                GroupedRow::new(
                    vec![text("alice")],
                    vec![OutputValue::decimal(Decimal::ZERO)],
                ),
            ],
            row_count: 3,
            next_cursor: Some("opaque-cursor".into()),
        },
        "User".into(),
    );
    let response: Result<SqlQueryResult, crate::Error> = Ok(result);
    let bytes = icydb_schema::wire::encode_one(&response).expect("grouped response should encode");
    let decoded = icydb_schema::wire::decode_one::<Result<SqlQueryResult, crate::Error>>(&bytes)
        .expect("grouped response should decode")
        .expect("grouped response should succeed");
    let SqlQueryResult::Grouped(grouped) = decoded else {
        panic!("expected grouped rows");
    };
    assert_eq!(
        grouped.rows,
        vec![
            vec!["NULL", "'null'"],
            vec!["'NULL'", "NULL"],
            vec!["alice", "0.000"],
        ],
    );
    assert_eq!(grouped.row_count, 3);
    assert_eq!(grouped.next_cursor.as_deref(), Some("opaque-cursor"));
}

#[test]
fn sql_query_result_from_statement_preserves_count_entity_and_row_count() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Count { row_count: 3 },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Count {
            entity: "User".to_string(),
            row_count: 3,
        },
        "public SQL packaging must preserve outward count payload identity",
    );
}

#[test]
fn sql_query_result_from_statement_preserves_text_projection_values() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Projection {
            columns: vec!["lower(name)".to_string()],
            fixed_scales: vec![None],
            rows: vec![vec![text("alice")], vec![text("bob")]],
            row_count: 2,
        },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Projection(RowProjectionOutput {
            entity: "User".to_string(),
            columns: vec!["lower(name)".to_string()],
            rows: vec![vec![text("alice")], vec![text("bob")]],
            row_count: 2,
        }),
        "public SQL packaging must preserve text projection values as semantic output values",
    );
}

#[test]
fn sql_query_result_from_statement_keeps_blob_projection_typed_until_rendering() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Projection {
            columns: vec!["thumbnail".to_string()],
            fixed_scales: vec![None],
            rows: vec![vec![OutputValue::blob(vec![0xab, 0xcd])]],
            row_count: 1,
        },
        "Blob".to_string(),
    );

    let SqlQueryResult::Projection(rows) = result else {
        panic!("blob projection should remain a projection payload");
    };
    assert_eq!(
        rows.rows,
        vec![vec![OutputValue::blob(vec![0xab, 0xcd])]],
        "SQL projection packaging should not pre-render blob payloads as hex text",
    );
    assert_eq!(
        rows.rendered_rows(),
        vec![vec!["0xabcd".to_string()]],
        "display rendering should still expose the stable hex representation when explicitly requested",
    );
}

#[test]
fn sql_query_result_blob_projection_wire_payload_stays_binary_not_hex() {
    let blob = vec![0xab; 4_096];
    let typed = SqlQueryResult::Projection(RowProjectionOutput {
        entity: "Blob".to_string(),
        columns: vec!["thumbnail".to_string()],
        rows: vec![vec![OutputValue::blob(blob.clone())]],
        row_count: 1,
    });
    let rendered = SqlQueryResult::Projection(RowProjectionOutput {
        entity: "Blob".to_string(),
        columns: vec!["thumbnail".to_string()],
        rows: vec![vec![text(
            format!("0x{}", "ab".repeat(blob.len())).as_str(),
        )]],
        row_count: 1,
    });

    let typed_len = icydb_schema::wire::encode_one(&typed)
        .expect("typed blob projection should encode")
        .len();
    let rendered_len = icydb_schema::wire::encode_one(&rendered)
        .expect("rendered blob projection should encode")
        .len();

    assert!(
        rendered_len.saturating_sub(typed_len) >= blob.len(),
        "binary blob projections should avoid the old hex-text payload expansion: typed={typed_len}, rendered={rendered_len}"
    );
}

#[test]
fn sql_query_result_from_statement_preserves_semantic_projection_value_variants() {
    let float32 = Float32::try_new(1.25).expect("finite f32");
    let float64 = Float64::try_new(2.5).expect("finite f64");
    let decimal = Decimal::new(1234, 2);
    let result = sql_query_result_from_statement(
        SqlStatementResult::Projection {
            columns: vec![
                "nat_value".to_string(),
                "int_value".to_string(),
                "decimal_value".to_string(),
                "float32_value".to_string(),
                "float64_value".to_string(),
                "optional_value".to_string(),
            ],
            fixed_scales: vec![None, None, None, None, None, None],
            rows: vec![vec![
                OutputValue::nat64(7),
                OutputValue::int64(-3),
                OutputValue::decimal(decimal),
                OutputValue::float32(float32),
                OutputValue::float64(float64),
                OutputValue::null(),
            ]],
            row_count: 1,
        },
        "Scalar".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Projection(RowProjectionOutput {
            entity: "Scalar".to_string(),
            columns: vec![
                "nat_value".to_string(),
                "int_value".to_string(),
                "decimal_value".to_string(),
                "float32_value".to_string(),
                "float64_value".to_string(),
                "optional_value".to_string(),
            ],
            rows: vec![vec![
                OutputValue::nat64(7),
                OutputValue::int64(-3),
                OutputValue::decimal(decimal),
                OutputValue::float32(float32),
                OutputValue::float64(float64),
                OutputValue::null(),
            ]],
            row_count: 1,
        }),
        "public SQL projection packaging should preserve semantic output value variants until explicit display rendering",
    );
}

#[test]
fn sql_query_result_from_statement_preserves_scalar_arithmetic_and_round_projection_rows() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Projection {
            columns: vec!["age - 1".to_string(), "ROUND(age / 3, 2)".to_string()],
            fixed_scales: vec![None, Some(2)],
            rows: vec![
                vec![
                    OutputValue::decimal(Decimal::from_i128(23).expect("23 decimal")),
                    OutputValue::decimal(Decimal::new(800, 2)),
                ],
                vec![
                    OutputValue::decimal(Decimal::from_i128(30).expect("30 decimal")),
                    OutputValue::decimal(Decimal::new(1033, 2)),
                ],
            ],
            row_count: 2,
        },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Projection(RowProjectionOutput {
            entity: "User".to_string(),
            columns: vec!["age - 1".to_string(), "ROUND(age / 3, 2)".to_string()],
            rows: vec![
                vec![
                    OutputValue::decimal(Decimal::from_i128(23).expect("23 decimal")),
                    text("8.00"),
                ],
                vec![
                    OutputValue::decimal(Decimal::from_i128(30).expect("30 decimal")),
                    text("10.33"),
                ],
            ],
            row_count: 2,
        }),
        "public SQL packaging must preserve arithmetic and ROUND projection labels and rendered decimal rows",
    );
}

#[test]
fn sql_query_result_from_statement_preserves_fixed_scale_for_zero_round_projection_rows() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Projection {
            columns: vec!["ROUND(age / 10, 3)".to_string()],
            fixed_scales: vec![Some(3)],
            rows: vec![vec![OutputValue::decimal(Decimal::ZERO)]],
            row_count: 1,
        },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Projection(RowProjectionOutput {
            entity: "User".to_string(),
            columns: vec!["ROUND(age / 10, 3)".to_string()],
            rows: vec![vec![text("0.000")]],
            row_count: 1,
        }),
        "public SQL packaging must keep ROUND projection scale even for zero values",
    );
}

#[test]
fn sql_query_result_from_statement_preserves_fixed_scale_for_aliased_round_projection_rows() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Projection {
            columns: vec!["dextrisma".to_string()],
            fixed_scales: vec![Some(3)],
            rows: vec![vec![OutputValue::decimal(
                Decimal::from_i128(16).expect("16 decimal"),
            )]],
            row_count: 1,
        },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Projection(RowProjectionOutput {
            entity: "User".to_string(),
            columns: vec!["dextrisma".to_string()],
            rows: vec![vec![text("16.000")]],
            row_count: 1,
        }),
        "public SQL packaging must preserve aliased ROUND projection scale even when the outward label no longer exposes ROUND(..., scale)",
    );
}

#[test]
fn sql_query_result_from_statement_preserves_fixed_scale_for_grouped_round_rows() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Grouped {
            columns: vec!["age".to_string(), "ROUND(AVG(age), 4)".to_string()],
            fixed_scales: vec![None, Some(4)],
            rows: vec![
                GroupedRow::new(
                    vec![OutputValue::nat64(12)],
                    vec![OutputValue::decimal(
                        Decimal::from_i128(12).expect("12 decimal"),
                    )],
                ),
                GroupedRow::new(
                    vec![OutputValue::nat64(14)],
                    vec![OutputValue::decimal(Decimal::new(142_000, 4))],
                ),
            ],
            row_count: 2,
            next_cursor: None,
        },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Grouped(SqlGroupedRowsOutput {
            entity: "User".to_string(),
            columns: vec!["age".to_string(), "ROUND(AVG(age), 4)".to_string()],
            rows: vec![
                vec!["12".to_string(), "12.0000".to_string()],
                vec!["14".to_string(), "14.2000".to_string()],
            ],
            row_count: 2,
            next_cursor: None,
        }),
        "public grouped SQL packaging must preserve fixed ROUND projection scale for grouped rows",
    );
}

#[test]
fn sql_query_result_from_statement_preserves_grouped_rows_and_cursor() {
    let result = sql_query_result_from_statement(
        SqlStatementResult::Grouped {
            columns: vec!["age".to_string(), "count(*)".to_string()],
            fixed_scales: vec![None, None],
            rows: vec![
                GroupedRow::new(vec![OutputValue::nat64(24)], vec![OutputValue::nat64(1)]),
                GroupedRow::new(vec![OutputValue::nat64(31)], vec![OutputValue::nat64(2)]),
            ],
            row_count: 2,
            next_cursor: Some("cursor:age:31".to_string()),
        },
        "User".to_string(),
    );

    assert_eq!(
        result,
        SqlQueryResult::Grouped(SqlGroupedRowsOutput {
            entity: "User".to_string(),
            columns: vec!["age".to_string(), "count(*)".to_string()],
            rows: vec![
                vec!["24".to_string(), "1".to_string()],
                vec!["31".to_string(), "2".to_string()],
            ],
            row_count: 2,
            next_cursor: Some("cursor:age:31".to_string()),
        }),
        "public SQL packaging must preserve grouped rows and outward continuation cursor",
    );
}
