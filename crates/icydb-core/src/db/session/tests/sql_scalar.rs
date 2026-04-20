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
fn execute_sql_scalar_in_trailing_comma_matches_canonical_rows() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("list-matrix-a", 10),
            ("list-matrix-b", 20),
            ("list-matrix-c", 30),
            ("list-matrix-d", 40),
        ],
    );

    let trailing_rows = execute_sql_name_age_rows(
        &session,
        "SELECT * \
         FROM SessionSqlEntity \
         WHERE age IN (20, 30,) \
         ORDER BY age ASC",
    );
    let canonical_rows = execute_sql_name_age_rows(
        &session,
        "SELECT * \
         FROM SessionSqlEntity \
         WHERE age IN (20, 30) \
         ORDER BY age ASC",
    );

    assert_eq!(
        trailing_rows, canonical_rows,
        "IN with one trailing comma should execute as the same canonical membership filter",
    );
}

#[test]
fn execute_sql_scalar_searched_case_where_matches_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic scalar WHERE matrix for searched CASE.
    seed_session_sql_entities(
        &session,
        &[
            ("where-case-a", 10),
            ("where-case-b", 20),
            ("where-case-c", 30),
            ("where-case-d", 40),
        ],
    );

    // Phase 2: require searched CASE WHERE to flow through the same scalar
    // expression seam as the other admitted clause positions.
    let rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE CASE WHEN age >= 30 THEN TRUE ELSE age = 20 END \
         ORDER BY age ASC",
    )
    .expect("searched CASE WHERE query should execute");

    assert_eq!(
        rows,
        vec![
            vec![Value::Text("where-case-b".to_string())],
            vec![Value::Text("where-case-c".to_string())],
            vec![Value::Text("where-case-d".to_string())],
        ],
        "searched CASE WHERE should evaluate row predicates through the unified scalar expression seam",
    );
}

#[test]
fn execute_sql_scalar_affine_numeric_where_compare_matches_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic scalar WHERE matrix for affine numeric
    // compare normalization onto the existing predicate lane.
    seed_session_sql_entities(
        &session,
        &[
            ("where-affine-a", 10),
            ("where-affine-b", 20),
            ("where-affine-c", 30),
            ("where-affine-d", 40),
        ],
    );

    // Phase 2: require one simple field-plus-literal compare to execute as
    // the same canonical filter as the equivalent direct field threshold.
    let rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE age + 1 >= 21 \
         ORDER BY age ASC",
    )
    .expect("affine numeric WHERE query should execute");

    assert_eq!(
        rows,
        vec![
            vec![Value::Text("where-affine-b".to_string())],
            vec![Value::Text("where-affine-c".to_string())],
            vec![Value::Text("where-affine-d".to_string())],
        ],
        "simple field-plus-literal WHERE compares should execute through the same canonical filter as the equivalent direct field threshold",
    );
}

#[test]
fn execute_sql_scalar_coalesce_and_nullif_where_matches_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic scalar WHERE matrix for value-selection
    // functions on the shared expression-owned WHERE seam.
    seed_session_sql_entities(
        &session,
        &[
            ("where-nullfn-a", 10),
            ("where-nullfn-b", 20),
            ("where-nullfn-c", 30),
            ("where-nullfn-d", 40),
        ],
    );

    // Phase 2: require nested NULLIF/COALESCE evaluation to stay correct even
    // when the derived predicate falls back to one residual runtime filter.
    let rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE COALESCE(NULLIF(age, 20), 99) = 99 \
         ORDER BY age ASC",
    )
    .expect("COALESCE/NULLIF WHERE query should execute");

    assert_eq!(
        rows,
        vec![vec![Value::Text("where-nullfn-b".to_string())]],
        "nested NULLIF/COALESCE WHERE should evaluate through the scalar expression seam",
    );
}

#[test]
fn execute_sql_scalar_unary_text_wrapped_value_selection_where_matches_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    seed_nullable_session_sql_entities(
        &session,
        &[
            ("alpha", Some(" Ally ")),
            ("bravo", None),
            ("charlie", Some("Chief")),
        ],
    );

    let coalesce_rows = statement_projection_rows::<SessionNullableSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionNullableSqlEntity \
         WHERE LOWER(TRIM(COALESCE(nickname, name))) = 'ally' \
         ORDER BY name ASC",
    )
    .expect("unary text wrapped COALESCE WHERE query should execute");
    let nullif_rows = statement_projection_rows::<SessionNullableSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionNullableSqlEntity \
         WHERE LOWER(NULLIF(name, 'alpha')) IS NULL \
         ORDER BY name ASC",
    )
    .expect("unary text wrapped NULLIF WHERE query should execute");

    assert_eq!(
        coalesce_rows,
        vec![vec![Value::Text("alpha".to_string())]],
        "LOWER(TRIM(COALESCE(...))) WHERE should evaluate through the scalar expression seam",
    );
    assert_eq!(
        nullif_rows,
        vec![vec![Value::Text("alpha".to_string())]],
        "LOWER(NULLIF(...)) WHERE should preserve NULL semantics through the scalar expression seam",
    );
}

#[test]
fn execute_sql_scalar_text_transform_where_matches_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("alpha", 10),
            ("alpine", 20),
            ("bravo", 30),
            ("charlie", 40),
        ],
    );

    let rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE REPLACE(name, 'a', 'A') = 'AlphA' \
         ORDER BY age ASC",
    )
    .expect("text transform WHERE query should execute");

    assert_eq!(
        rows,
        vec![vec![Value::Text("alpha".to_string())]],
        "text transform WHERE should evaluate the admitted shared scalar function family through the scalar residual filter seam",
    );
}

#[test]
fn execute_sql_scalar_text_predicate_wrapped_transform_where_matches_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("alpha", 10),
            ("alpine", 20),
            ("bravo", 30),
            ("charlie", 40),
        ],
    );

    let rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE STARTS_WITH(REPLACE(name, 'a', 'A'), 'Al') \
         ORDER BY age ASC",
    )
    .expect("text predicate wrapped transform WHERE query should execute");

    assert_eq!(
        rows,
        vec![
            vec![Value::Text("alpha".to_string())],
            vec![Value::Text("alpine".to_string())],
        ],
        "text predicate wrapped transform WHERE should evaluate the admitted shared scalar text predicate family through the scalar residual filter seam",
    );
}

#[test]
fn execute_sql_scalar_text_predicate_expression_arguments_where_match_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("alpha", 10),
            ("alpine", 20),
            ("bravo", 30),
            ("charlie", 40),
        ],
    );

    let rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al')) \
         ORDER BY age ASC",
    )
    .expect("text predicate expression arguments WHERE query should execute");

    assert_eq!(
        rows,
        vec![
            vec![Value::Text("alpha".to_string())],
            vec![Value::Text("alpine".to_string())],
        ],
        "text predicate expression arguments WHERE should evaluate through the scalar residual filter seam",
    );
}

#[test]
fn execute_sql_scalar_constant_null_test_where_matches_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("alpha", 10),
            ("alpine", 20),
            ("bravo", 30),
            ("charlie", 40),
        ],
    );

    for (sql, expected_rows, context) in [
        (
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE NULLIF('alpha', 'alpha') IS NULL \
             ORDER BY age ASC",
            vec![
                vec![Value::Text("alpha".to_string())],
                vec![Value::Text("alpine".to_string())],
                vec![Value::Text("bravo".to_string())],
                vec![Value::Text("charlie".to_string())],
            ],
            "constant null-test WHERE that folds to TRUE",
        ),
        (
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE NULLIF('alpha', 'alpha') IS NOT NULL \
             ORDER BY age ASC",
            Vec::new(),
            "constant null-test WHERE that folds to FALSE",
        ),
    ] {
        let rows = statement_projection_rows::<SessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} query should execute: {err:?}"));

        assert_eq!(
            rows, expected_rows,
            "{context} should preserve the folded boolean row semantics through the scalar query path",
        );
    }
}

#[test]
fn execute_sql_scalar_wrapped_like_and_ilike_where_match_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("alpha", 10),
            ("alpine", 20),
            ("bravo", 30),
            ("charlie", 40),
        ],
    );

    for (sql, context) in [
        (
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE REPLACE(name, 'a', 'A') LIKE 'Al%' \
             ORDER BY age ASC",
            "wrapped LIKE target WHERE query",
        ),
        (
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE REPLACE(name, 'a', 'A') ILIKE 'al%' \
             ORDER BY age ASC",
            "wrapped ILIKE target WHERE query",
        ),
    ] {
        let rows = statement_projection_rows::<SessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should execute: {err:?}"));

        assert_eq!(
            rows,
            vec![
                vec![Value::Text("alpha".to_string())],
                vec![Value::Text("alpine".to_string())],
            ],
            "wrapped LIKE/ILIKE target WHERE should evaluate through the scalar residual filter seam",
        );
    }
}

#[test]
fn execute_sql_scalar_searched_case_where_null_boolean_context_matches_canonical_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic scalar WHERE matrix that exercises NULL
    // boolean-context behavior through searched CASE.
    seed_session_sql_entities(
        &session,
        &[
            ("where-case-null-a", 10),
            ("where-case-null-b", 20),
            ("where-case-null-c", 30),
            ("where-case-null-d", 40),
        ],
    );

    // Phase 2: compare NULL-condition and NULL-result CASE filters against
    // their direct canonical boolean forms.
    let null_condition_rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE CASE WHEN NULL THEN TRUE ELSE age = 20 END \
         ORDER BY age ASC",
    )
    .expect("searched CASE WHERE with NULL condition should execute");
    let direct_age_rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE age = 20 \
         ORDER BY age ASC",
    )
    .expect("direct age-equality WHERE should execute");
    let null_result_rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE CASE WHEN age >= 30 THEN TRUE END \
         ORDER BY age ASC",
    )
    .expect("searched CASE WHERE with NULL result should execute");
    let direct_threshold_rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE age >= 30 \
         ORDER BY age ASC",
    )
    .expect("direct threshold WHERE should execute");

    assert_eq!(
        null_condition_rows, direct_age_rows,
        "NULL searched-CASE conditions in WHERE should behave like false and fall through to the ELSE branch",
    );
    assert_eq!(
        null_result_rows, direct_threshold_rows,
        "NULL searched-CASE results in WHERE should filter rows the same way as the equivalent direct boolean predicate",
    );
}

#[test]
fn execute_sql_scalar_not_searched_case_where_null_semantics_match_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic scalar WHERE matrix for NOT-over-CASE.
    seed_session_sql_entities(
        &session,
        &[
            ("where-not-case-a", 10),
            ("where-not-case-b", 20),
            ("where-not-case-c", 30),
            ("where-not-case-d", 40),
        ],
    );

    // Phase 2: require NOT to preserve NULL searched-CASE semantics instead
    // of treating unknown as false too early.
    let rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE NOT CASE WHEN age >= 30 THEN TRUE END \
         ORDER BY age ASC",
    )
    .expect("NOT searched CASE WHERE query should execute");

    assert_eq!(
        rows,
        Vec::<Vec<Value>>::new(),
        "NOT searched CASE WHERE should filter both true and NULL rows instead of collapsing NULL to false before negation",
    );
}

#[test]
fn execute_sql_scalar_combined_boolean_searched_case_where_matches_canonical_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic scalar WHERE matrix for composed
    // boolean expressions that include searched CASE.
    seed_session_sql_entities(
        &session,
        &[
            ("where-case-and-a", 10),
            ("where-case-and-b", 20),
            ("where-case-and-c", 30),
            ("where-case-and-d", 40),
        ],
    );

    // Phase 2: compare one composed searched-CASE boolean filter against its
    // equivalent direct boolean predicate.
    let case_rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE (CASE WHEN age >= 30 THEN TRUE END) AND age >= 20 \
         ORDER BY age ASC",
    )
    .expect("composed searched CASE WHERE query should execute");
    let canonical_rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE age >= 30 \
         ORDER BY age ASC",
    )
    .expect("canonical composed boolean WHERE query should execute");

    assert_eq!(
        case_rows, canonical_rows,
        "searched CASE should preserve NULL through composed boolean expressions until the final WHERE truth collapse",
    );
}

#[test]
fn execute_sql_scalar_null_boolean_and_true_filters_all_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic scalar WHERE matrix for literal NULL
    // boolean composition.
    seed_session_sql_entities(
        &session,
        &[
            ("where-null-and-a", 10),
            ("where-null-and-b", 20),
            ("where-null-and-c", 30),
            ("where-null-and-d", 40),
        ],
    );

    // Phase 2: require literal NULL boolean composition to collapse only at
    // the final WHERE truth boundary.
    let rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE NULL AND TRUE \
         ORDER BY age ASC",
    )
    .expect("NULL AND TRUE WHERE query should execute");

    assert_eq!(
        rows,
        Vec::<Vec<Value>>::new(),
        "NULL AND TRUE should evaluate to unknown and filter every row",
    );
}

#[test]
fn execute_sql_scalar_not_null_filters_all_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic scalar WHERE matrix for NOT NULL.
    seed_session_sql_entities(
        &session,
        &[
            ("where-not-null-a", 10),
            ("where-not-null-b", 20),
            ("where-not-null-c", 30),
            ("where-not-null-d", 40),
        ],
    );

    // Phase 2: require NOT NULL in WHERE to stay unknown and filter rows.
    let rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE NOT NULL \
         ORDER BY age ASC",
    )
    .expect("NOT NULL WHERE query should execute");

    assert_eq!(
        rows,
        Vec::<Vec<Value>>::new(),
        "NOT NULL should remain unknown in WHERE and filter every row",
    );
}

#[test]
fn execute_sql_scalar_case_with_null_true_branch_keeps_else_rows_only() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic scalar WHERE matrix for mixed CASE
    // branches that return NULL in boolean context.
    seed_session_sql_entities(
        &session,
        &[
            ("where-case-null-branch-a", 10),
            ("where-case-null-branch-b", 20),
            ("where-case-null-branch-c", 30),
            ("where-case-null-branch-d", 40),
        ],
    );

    // Phase 2: require searched CASE to keep NULL branch results until the
    // final WHERE truth collapse.
    let rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE CASE WHEN age >= 30 THEN NULL ELSE TRUE END \
         ORDER BY age ASC",
    )
    .expect("mixed NULL/TRUE searched CASE WHERE query should execute");

    assert_eq!(
        rows,
        vec![
            vec![Value::Text("where-case-null-branch-a".to_string())],
            vec![Value::Text("where-case-null-branch-b".to_string())],
        ],
        "searched CASE should drop rows whose selected branch returns NULL and keep rows whose selected branch returns TRUE",
    );
}

#[test]
fn execute_sql_scalar_eq_null_on_non_nullable_field_returns_no_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic scalar WHERE matrix for NULL compare
    // spelling over a non-nullable field.
    seed_session_sql_entities(
        &session,
        &[
            ("where-eq-null-a", 10),
            ("where-eq-null-b", 20),
            ("where-eq-null-c", 30),
            ("where-eq-null-d", 40),
        ],
    );

    // Phase 2: require equality-to-NULL over the current non-nullable field
    // surface to return no rows.
    let rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE age = NULL \
         ORDER BY age ASC",
    )
    .expect("field equals NULL WHERE query should execute on the non-nullable fixture");

    assert_eq!(
        rows,
        Vec::<Vec<Value>>::new(),
        "field equals NULL should keep no rows on the current non-nullable fixture",
    );
}

#[test]
fn execute_sql_scalar_is_not_null_differs_from_ne_null_on_non_nullable_field() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic scalar WHERE matrix for contrasting
    // null-test and inequality-to-NULL spellings on a non-nullable field.
    seed_session_sql_entities(
        &session,
        &[
            ("where-is-not-null-a", 10),
            ("where-is-not-null-b", 20),
            ("where-is-not-null-c", 30),
            ("where-is-not-null-d", 40),
        ],
    );

    // Phase 2: require IS NOT NULL to keep the seeded rows while != NULL
    // still behaves like unknown-at-WHERE and drops everything.
    let is_not_null_rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE age IS NOT NULL \
         ORDER BY age ASC",
    )
    .expect("field IS NOT NULL WHERE query should execute on the non-nullable fixture");
    let ne_null_rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlEntity \
         WHERE age != NULL \
         ORDER BY age ASC",
    )
    .expect("field != NULL WHERE query should execute on the non-nullable fixture");

    assert_eq!(
        is_not_null_rows,
        vec![
            vec![Value::Text("where-is-not-null-a".to_string())],
            vec![Value::Text("where-is-not-null-b".to_string())],
            vec![Value::Text("where-is-not-null-c".to_string())],
            vec![Value::Text("where-is-not-null-d".to_string())],
        ],
        "IS NOT NULL should keep all rows on the current non-nullable fixture",
    );
    assert_eq!(
        ne_null_rows,
        Vec::<Vec<Value>>::new(),
        "!= NULL should stay distinct from IS NOT NULL and keep no rows on the current non-nullable fixture",
    );
}

#[test]
fn execute_sql_scalar_nullable_field_distinguishes_null_tests_from_null_compares() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic nullable-field matrix so the live SQL
    // session path proves null-test and compare-to-NULL spellings apart on
    // persisted nullable data.
    seed_nullable_session_sql_entities(
        &session,
        &[
            ("nullable-null-a", None),
            ("nullable-null-b", Some("bravo")),
            ("nullable-null-c", None),
            ("nullable-null-d", Some("delta")),
        ],
    );

    // Phase 2: execute the four relevant NULL spellings against the same
    // nullable field and keep the row-shape contract explicit.
    let is_null_rows = statement_projection_rows::<SessionNullableSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionNullableSqlEntity \
         WHERE nickname IS NULL \
         ORDER BY name ASC",
    )
    .expect("nullable-field IS NULL WHERE query should execute");
    let eq_null_rows = statement_projection_rows::<SessionNullableSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionNullableSqlEntity \
         WHERE nickname = NULL \
         ORDER BY name ASC",
    )
    .expect("nullable-field = NULL WHERE query should execute");
    let is_not_null_rows = statement_projection_rows::<SessionNullableSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionNullableSqlEntity \
         WHERE nickname IS NOT NULL \
         ORDER BY name ASC",
    )
    .expect("nullable-field IS NOT NULL WHERE query should execute");
    let ne_null_rows = statement_projection_rows::<SessionNullableSqlEntity>(
        &session,
        "SELECT name \
         FROM SessionNullableSqlEntity \
         WHERE nickname != NULL \
         ORDER BY name ASC",
    )
    .expect("nullable-field != NULL WHERE query should execute");

    assert_eq!(
        is_null_rows,
        vec![
            vec![Value::Text("nullable-null-a".to_string())],
            vec![Value::Text("nullable-null-c".to_string())],
        ],
        "IS NULL should keep the persisted rows whose nullable field is actually NULL",
    );
    assert_eq!(
        eq_null_rows,
        Vec::<Vec<Value>>::new(),
        "= NULL should still behave like unknown-at-WHERE and keep no rows",
    );
    assert_eq!(
        is_not_null_rows,
        vec![
            vec![Value::Text("nullable-null-b".to_string())],
            vec![Value::Text("nullable-null-d".to_string())],
        ],
        "IS NOT NULL should keep the persisted rows whose nullable field is present",
    );
    assert_eq!(
        ne_null_rows,
        Vec::<Vec<Value>>::new(),
        "!= NULL should stay distinct from IS NOT NULL and keep no rows",
    );
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
        .order_term(crate::db::asc("score"))
        .order_term(crate::db::asc("id"))
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
        .order_term(crate::db::asc("age"))
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
fn execute_sql_scalar_field_bound_between_and_not_between_match_fluent_results() {
    reset_session_sql_store();
    let session = sql_session();

    for (label, score, min_score, max_score, expected_between) in [
        ("field-bound-a", 15_u64, 10_u64, 20_u64, true),
        ("field-bound-b", 10_u64, 10_u64, 20_u64, true),
        ("field-bound-c", 20_u64, 10_u64, 20_u64, true),
        ("field-bound-d", 9_u64, 10_u64, 20_u64, false),
        ("field-bound-e", 21_u64, 10_u64, 20_u64, false),
    ] {
        session
            .insert(SessionSqlFieldBoundRangeEntity {
                id: Ulid::generate(),
                label: label.to_string(),
                score,
                min_score,
                max_score,
            })
            .expect("field-bound range fixture insert should succeed");

        assert_eq!(
            expected_between,
            score >= min_score && score <= max_score,
            "test matrix should label field-bound range rows correctly",
        );
    }

    let between_rows = statement_projection_rows::<SessionSqlFieldBoundRangeEntity>(
        &session,
        "SELECT label FROM SessionSqlFieldBoundRangeEntity \
         WHERE score BETWEEN min_score AND max_score \
         ORDER BY label ASC",
    )
    .expect("field-bound BETWEEN query should execute");
    let not_between_rows = statement_projection_rows::<SessionSqlFieldBoundRangeEntity>(
        &session,
        "SELECT label FROM SessionSqlFieldBoundRangeEntity \
         WHERE score NOT BETWEEN min_score AND max_score \
         ORDER BY label ASC",
    )
    .expect("field-bound NOT BETWEEN query should execute");
    let fluent_between_rows = session
        .load::<SessionSqlFieldBoundRangeEntity>()
        .filter(crate::db::FieldRef::new("score").between_fields("min_score", "max_score"))
        .order_term(crate::db::asc("label"))
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("fluent field-bound BETWEEN query should execute")
        .into_iter()
        .map(|row| vec![Value::Text(row.entity_ref().label.clone())])
        .collect::<Vec<_>>();
    let fluent_not_between_rows = session
        .load::<SessionSqlFieldBoundRangeEntity>()
        .filter(crate::db::FieldRef::new("score").not_between_fields("min_score", "max_score"))
        .order_term(crate::db::asc("label"))
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("fluent field-bound NOT BETWEEN query should execute")
        .into_iter()
        .map(|row| vec![Value::Text(row.entity_ref().label.clone())])
        .collect::<Vec<_>>();

    assert_eq!(
        between_rows, fluent_between_rows,
        "field-bound BETWEEN should lower to the same bounded compare-fields range predicate on SQL and fluent surfaces",
    );
    assert_eq!(
        not_between_rows, fluent_not_between_rows,
        "field-bound NOT BETWEEN should lower to the same bounded compare-fields outside-range predicate on SQL and fluent surfaces",
    );
    assert_eq!(
        between_rows,
        vec![
            vec![Value::Text("field-bound-a".to_string())],
            vec![Value::Text("field-bound-b".to_string())],
            vec![Value::Text("field-bound-c".to_string())],
        ],
        "field-bound BETWEEN should keep rows inside the inclusive sibling-field bounds",
    );
    assert_eq!(
        not_between_rows,
        vec![
            vec![Value::Text("field-bound-d".to_string())],
            vec![Value::Text("field-bound-e".to_string())],
        ],
        "field-bound NOT BETWEEN should keep rows outside the inclusive sibling-field bounds",
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
fn execute_sql_scalar_float_field_decimal_literal_order_compare_matches_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_float_compare_entities(
        &session,
        &[
            ("float-compare-a", 0.10),
            ("float-compare-b", 0.20),
            ("float-compare-c", 0.25),
        ],
    );

    let rows = statement_projection_rows::<SessionSqlFloatCompareEntity>(
        &session,
        "SELECT label \
         FROM SessionSqlFloatCompareEntity \
         WHERE dodge_chance >= 0.20 \
         ORDER BY dodge_chance ASC, label ASC",
    )
    .expect("float-field decimal-literal ordered compare query should execute");

    assert_eq!(
        rows,
        vec![
            vec![Value::Text("float-compare-b".to_string())],
            vec![Value::Text("float-compare-c".to_string())],
        ],
        "float-backed ordered compares should widen one decimal-looking SQL literal instead of failing strict literal validation",
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
fn execute_sql_scalar_is_true_false_and_is_not_true_false_match_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    for (label, active, archived) in [
        ("bool-a", true, false),
        ("bool-b", false, false),
        ("bool-c", true, true),
    ] {
        session
            .insert(SessionSqlBoolCompareEntity {
                id: Ulid::generate(),
                label: label.to_string(),
                active,
                archived,
            })
            .expect("bool compare fixture insert should succeed");
    }

    let true_rows = statement_projection_rows::<SessionSqlBoolCompareEntity>(
        &session,
        "SELECT label FROM SessionSqlBoolCompareEntity WHERE active IS TRUE ORDER BY label ASC",
    )
    .expect("IS TRUE query should execute");
    let false_rows = statement_projection_rows::<SessionSqlBoolCompareEntity>(
        &session,
        "SELECT label FROM SessionSqlBoolCompareEntity WHERE active IS FALSE ORDER BY label ASC",
    )
    .expect("IS FALSE query should execute");
    let not_true_rows = statement_projection_rows::<SessionSqlBoolCompareEntity>(
        &session,
        "SELECT label FROM SessionSqlBoolCompareEntity WHERE active IS NOT TRUE ORDER BY label ASC",
    )
    .expect("IS NOT TRUE query should execute");
    let not_false_rows = statement_projection_rows::<SessionSqlBoolCompareEntity>(
        &session,
        "SELECT label FROM SessionSqlBoolCompareEntity WHERE active IS NOT FALSE ORDER BY label ASC",
    )
    .expect("IS NOT FALSE query should execute");

    assert_eq!(
        true_rows,
        vec![
            vec![Value::Text("bool-a".to_string())],
            vec![Value::Text("bool-c".to_string())],
        ],
        "IS TRUE should keep rows whose bool field is true",
    );
    assert_eq!(
        false_rows,
        vec![vec![Value::Text("bool-b".to_string())]],
        "IS FALSE should keep rows whose bool field is false",
    );
    assert_eq!(
        not_true_rows,
        vec![vec![Value::Text("bool-b".to_string())]],
        "IS NOT TRUE should keep rows whose bool field is not true",
    );
    assert_eq!(
        not_false_rows,
        vec![
            vec![Value::Text("bool-a".to_string())],
            vec![Value::Text("bool-c".to_string())],
        ],
        "IS NOT FALSE should keep rows whose bool field is not false",
    );
}

#[test]
fn execute_sql_scalar_searched_case_where_bool_field_matches_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic bool-field matrix for searched CASE
    // conditions that read a bare boolean field leaf.
    for (label, active, archived) in [
        ("bool-case-a", true, false),
        ("bool-case-b", false, false),
        ("bool-case-c", true, true),
    ] {
        session
            .insert(SessionSqlBoolCompareEntity {
                id: Ulid::generate(),
                label: label.to_string(),
                active,
                archived,
            })
            .expect("bool searched CASE fixture insert should succeed");
    }

    // Phase 2: require searched CASE to admit boolean field conditions and
    // preserve their row-filter behavior through the shared WHERE seam.
    let rows = statement_projection_rows::<SessionSqlBoolCompareEntity>(
        &session,
        "SELECT label \
         FROM SessionSqlBoolCompareEntity \
         WHERE CASE WHEN active THEN FALSE ELSE TRUE END \
         ORDER BY label ASC",
    )
    .expect("searched CASE bool-field WHERE query should execute");

    assert_eq!(
        rows,
        vec![vec![Value::Text("bool-case-b".to_string())]],
        "searched CASE should admit bare boolean field conditions in WHERE and keep only rows whose ELSE branch returns true",
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
