use super::*;

#[test]
fn execute_sql_scalar_matrix_queries_match_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic rows for scalar matrix cases.
    seed_session_sql_entities(
        &session,
        &[
            ("scalar-matrix-a", 10),
            ("scalar-matrix-b", 20),
            ("scalar-matrix-c", 30),
            ("scalar-matrix-d", 40),
        ],
    );

    // Phase 2: execute table-driven scalar SQL cases.
    let cases = vec![
        (
            "SELECT * \
             FROM SessionSqlEntity \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
            vec![
                ("scalar-matrix-c".to_string(), 30_u64),
                ("scalar-matrix-b".to_string(), 20_u64),
            ],
        ),
        (
            "SELECT * \
             FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age ASC LIMIT 2",
            vec![
                ("scalar-matrix-b".to_string(), 20_u64),
                ("scalar-matrix-c".to_string(), 30_u64),
            ],
        ),
        (
            "SELECT DISTINCT * \
             FROM SessionSqlEntity \
             WHERE age >= 30 \
             ORDER BY age DESC",
            vec![
                ("scalar-matrix-d".to_string(), 40_u64),
                ("scalar-matrix-c".to_string(), 30_u64),
            ],
        ),
        (
            "SELECT * \
             FROM public.SessionSqlEntity \
             WHERE age < 25 \
             ORDER BY age ASC",
            vec![
                ("scalar-matrix-a".to_string(), 10_u64),
                ("scalar-matrix-b".to_string(), 20_u64),
            ],
        ),
        (
            "SELECT * \
             FROM SessionSqlEntity \
             ORDER BY age ASC LIMIT 1 OFFSET 2",
            vec![("scalar-matrix-c".to_string(), 30_u64)],
        ),
    ];

    // Phase 3: assert scalar row payload order and values for each query.
    for (sql, expected_rows) in cases {
        let actual_rows = execute_sql_name_age_rows(&session, sql);
        assert_eq!(actual_rows, expected_rows, "scalar matrix case: {sql}");
    }
}

#[test]
fn scalar_select_helper_rejects_aggregate_projection_in_current_slice() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_scalar_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(*) FROM SessionSqlEntity",
    )
    .expect_err("global aggregate SQL projection should remain lowering-gated");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "global aggregate SQL projection should fail at reduced lowering boundary",
    );
    assert!(
        err.to_string()
            .contains("scalar SELECT helper rejects global aggregate SELECT"),
        "scalar SELECT helper should preserve the dedicated aggregate-lane boundary message",
    );
}

#[test]
fn execute_sql_scalar_field_to_field_predicate_matches_expected_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic field-compare matrix.
    for (score, handle, label, expected_match) in [
        (10_u64, "mango", "apple", true),
        (20_u64, "alpha", "zebra", false),
        (30_u64, "same", "same", false),
        (40_u64, "omega", "beta", true),
    ] {
        session
            .insert(SessionDeterministicRangeEntity {
                id: Ulid::generate(),
                tier: "gold".to_string(),
                score,
                handle: handle.to_string(),
                label: label.to_string(),
            })
            .expect("deterministic range fixture insert should succeed");

        assert!(
            expected_match == (handle > label),
            "test matrix should label field-to-field compare rows correctly",
        );
    }

    // Phase 2: require field-to-field filtering to execute as a residual row comparison.
    let rows = statement_projection_rows::<SessionDeterministicRangeEntity>(
        &session,
        "SELECT label FROM SessionDeterministicRangeEntity \
         WHERE handle > label \
         ORDER BY score ASC, id ASC",
    )
    .expect("field-to-field predicate query should execute");

    assert_eq!(
        rows,
        vec![
            vec![Value::Text("apple".to_string())],
            vec![Value::Text("beta".to_string())],
        ],
        "field-to-field runtime filtering should keep only rows whose left field exceeds the right field",
    );
}

#[test]
fn execute_sql_scalar_field_to_field_equality_widens_mixed_numeric_fields() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one mixed signed/unsigned equality matrix.
    for (label, left_score, right_score) in [
        ("equal-a", 7_u64, 7_i64),
        ("equal-b", 12_u64, 12_i64),
        ("not-equal", 9_u64, 8_i64),
        ("negative-right", 4_u64, -4_i64),
    ] {
        session
            .insert(SessionSqlMixedNumericCompareEntity {
                id: Ulid::generate(),
                label: label.to_string(),
                left_score,
                right_score,
            })
            .expect("mixed numeric compare fixture insert should succeed");
    }

    // Phase 2: require field-to-field equality to widen mixed numeric fields
    // instead of failing strict runtime coercion.
    let rows = statement_projection_rows::<SessionSqlMixedNumericCompareEntity>(
        &session,
        "SELECT label FROM SessionSqlMixedNumericCompareEntity \
         WHERE left_score = right_score \
         ORDER BY label ASC",
    )
    .expect("mixed numeric field equality query should execute");

    assert_eq!(
        rows,
        vec![
            vec![Value::Text("equal-a".to_string())],
            vec![Value::Text("equal-b".to_string())],
        ],
        "mixed numeric field equality should widen before residual comparison instead of failing strict coercion",
    );
}

#[test]
fn execute_sql_scalar_field_to_field_matches_fluent_runtime_result() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic field-compare matrix used by both
    // execution surfaces.
    for (score, handle, label) in [
        (10_u64, "mango", "apple"),
        (20_u64, "alpha", "zebra"),
        (30_u64, "same", "same"),
        (40_u64, "omega", "beta"),
    ] {
        session
            .insert(SessionDeterministicRangeEntity {
                id: Ulid::generate(),
                tier: "gold".to_string(),
                score,
                handle: handle.to_string(),
                label: label.to_string(),
            })
            .expect("deterministic range fixture insert should succeed");
    }

    // Phase 2: execute the SQL and fluent surfaces over the same predicate.
    let sql_rows = statement_projection_rows::<SessionDeterministicRangeEntity>(
        &session,
        "SELECT label FROM SessionDeterministicRangeEntity \
         WHERE handle > label \
         ORDER BY score ASC, id ASC",
    )
    .expect("field-to-field SQL query should execute");

    let fluent_labels = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(crate::db::FieldRef::new("handle").gt_field("label"))
        .order_by("score")
        .order_by("id")
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("field-to-field fluent query should execute")
        .into_iter()
        .map(|row| Value::Text(row.entity_ref().label.clone()))
        .collect::<Vec<_>>();

    let sql_labels = sql_rows
        .into_iter()
        .map(|mut row| {
            row.pop()
                .expect("single-column SQL field-to-field projection should contain one value")
        })
        .collect::<Vec<_>>();

    assert_eq!(
        sql_labels, fluent_labels,
        "field-to-field fluent execution should stay aligned with SQL runtime rows",
    );
}

#[test]
fn execute_sql_scalar_not_between_matches_fluent_runtime_result() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic range matrix for SQL and fluent parity.
    seed_session_sql_entities(
        &session,
        &[
            ("not-between-a", 10),
            ("not-between-b", 20),
            ("not-between-c", 30),
            ("not-between-d", 40),
        ],
    );

    // Phase 2: execute the SQL and fluent surfaces over the same outside-range predicate.
    let sql_rows = execute_sql_name_age_rows(
        &session,
        "SELECT * \
         FROM SessionSqlEntity \
         WHERE age NOT BETWEEN 20 AND 30 \
         ORDER BY age ASC",
    );

    let fluent_rows = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FieldRef::new("age").not_between(20_u64, 30_u64))
        .order_by("age")
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("fluent NOT BETWEEN query should execute")
        .into_iter()
        .map(|row| (row.entity_ref().name.clone(), row.entity_ref().age))
        .collect::<Vec<_>>();

    assert_eq!(
        sql_rows, fluent_rows,
        "NOT BETWEEN should lower to the same bounded outside-range predicate on SQL and fluent surfaces",
    );
    assert_eq!(
        sql_rows,
        vec![
            ("not-between-a".to_string(), 10_u64),
            ("not-between-d".to_string(), 40_u64),
        ],
        "NOT BETWEEN should keep only rows outside the inclusive bounds",
    );
}

#[test]
fn execute_sql_scalar_symmetric_compare_forms_match_canonical_results() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic scalar matrix for symmetric compare forms.
    seed_session_sql_entities(
        &session,
        &[("symmetric-a", 5), ("symmetric-b", 10), ("symmetric-c", 20)],
    );

    // Phase 2: require literal-leading compares to match the canonical field-first form.
    let canonical_rows = execute_sql_name_age_rows(
        &session,
        "SELECT * FROM SessionSqlEntity WHERE age > 5 ORDER BY age ASC",
    );
    let symmetric_rows = execute_sql_name_age_rows(
        &session,
        "SELECT * FROM SessionSqlEntity WHERE 5 < age ORDER BY age ASC",
    );

    assert_eq!(
        symmetric_rows, canonical_rows,
        "literal-leading symmetric compares should normalize to the same canonical field-first predicate",
    );

    // Phase 3: require swapped field equality to match the canonical field order.
    reset_indexed_session_sql_store();
    let indexed = indexed_sql_session();
    for (score, handle, label) in [
        (10_u64, "same", "same"),
        (20_u64, "alpha", "zebra"),
        (30_u64, "omega", "omega"),
    ] {
        indexed
            .insert(SessionDeterministicRangeEntity {
                id: Ulid::generate(),
                tier: "gold".to_string(),
                score,
                handle: handle.to_string(),
                label: label.to_string(),
            })
            .expect("deterministic range fixture insert should succeed");
    }

    let canonical_eq = statement_projection_rows::<SessionDeterministicRangeEntity>(
        &indexed,
        "SELECT label FROM SessionDeterministicRangeEntity \
         WHERE handle = label \
         ORDER BY score ASC, id ASC",
    )
    .expect("canonical field equality query should execute");
    let swapped_eq = statement_projection_rows::<SessionDeterministicRangeEntity>(
        &indexed,
        "SELECT label FROM SessionDeterministicRangeEntity \
         WHERE label = handle \
         ORDER BY score ASC, id ASC",
    )
    .expect("swapped field equality query should execute");

    assert_eq!(
        swapped_eq, canonical_eq,
        "swapped field equality should normalize to the same canonical compare-fields predicate",
    );
}

#[test]
fn execute_sql_scalar_field_to_field_same_field_compare_keeps_all_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic same-field compare matrix.
    for (score, label) in [(10_u64, "same-a"), (20_u64, "same-b"), (30_u64, "same-c")] {
        session
            .insert(SessionDeterministicRangeEntity {
                id: Ulid::generate(),
                tier: "gold".to_string(),
                score,
                handle: label.to_string(),
                label: label.to_string(),
            })
            .expect("deterministic range fixture insert should succeed");
    }

    // Phase 2: require same-field equality to behave as a normal residual
    // compare instead of tripping a special-case path.
    let rows = statement_projection_rows::<SessionDeterministicRangeEntity>(
        &session,
        "SELECT label FROM SessionDeterministicRangeEntity \
         WHERE score = score \
         ORDER BY score ASC, id ASC",
    )
    .expect("same-field compare query should execute");

    assert_eq!(
        rows,
        vec![
            vec![Value::Text("same-a".to_string())],
            vec![Value::Text("same-b".to_string())],
            vec![Value::Text("same-c".to_string())],
        ],
        "same-field compare should keep every seeded row when both sides resolve to the same value",
    );
}

#[test]
fn execute_sql_scalar_field_to_field_invalid_type_compare_rejects_semantically() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_scalar_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT * FROM SessionSqlEntity WHERE name > age",
    )
    .expect_err("text-vs-numeric field ordering should fail schema validation");

    assert!(
        err.to_string()
            .contains("operator Gt against field 'age' is not valid for field 'name'"),
        "invalid type compare should preserve the incompatible field-ordering boundary message",
    );
}

#[test]
fn execute_sql_scalar_literal_leading_mixed_type_compare_still_rejects_semantically() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_scalar_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT * FROM SessionSqlEntity WHERE '5' < age",
    )
    .expect_err(
        "literal-leading mixed-type compare should fail schema validation after normalization",
    );

    assert!(
        err.to_string().contains("field 'age'"),
        "literal-leading normalization should not hide the existing invalid field-vs-literal type error",
    );
}

#[test]
fn execute_sql_scalar_field_to_field_bool_ordering_rejects_semantically() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_scalar_select_for_tests::<SessionSqlBoolCompareEntity>(
        &session,
        "SELECT * FROM SessionSqlBoolCompareEntity WHERE active > archived",
    )
    .expect_err("ordered bool field compare should fail schema validation");

    assert!(
        err.to_string()
            .contains("operator Gt against field 'archived' is not valid for field 'active'",),
        "bool ordering should stay fail-closed instead of silently widening predicate semantics",
    );
}

#[test]
fn execute_sql_scalar_field_to_field_unknown_field_rejects_at_field_resolution() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_scalar_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT * FROM SessionSqlEntity WHERE age = unknown_field",
    )
    .expect_err("unknown right-hand field should fail field resolution");

    assert!(
        err.to_string().contains("unknown field 'unknown_field'"),
        "missing compare field should stay a field-resolution error instead of a parser error",
    );
}
