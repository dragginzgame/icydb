use super::*;
use crate::db::{
    SqlDeleteExposurePolicy, SqlDeletePolicyContext, SqlPublicBoundedDeletePlan,
    SqlPublicPrimaryKeyDeletePlan, SqlValidatedDeletePlan, classify_sql_delete_policy,
};

type NameAgeRows = Vec<(String, u64)>;

// Seed the canonical minor/adult delete fixture used by the ordered delete
// boundary checks in this file.
fn seed_delete_minor_fixture(session: &DbSession<SessionSqlCanister>) {
    seed_session_sql_entities(
        session,
        &[("first-minor", 16), ("second-minor", 17), ("adult", 42)],
    );
}

// Seed the canonical offset-aware delete fixture used by the ordered delete
// window checks in this file.
fn seed_delete_offset_fixture(session: &DbSession<SessionSqlCanister>) {
    seed_session_sql_entities(
        session,
        &[
            ("first-minor", 16),
            ("second-minor", 17),
            ("third-minor", 18),
            ("adult", 42),
        ],
    );
}

// Run one SQL DELETE statement with explicit `RETURNING name, age` and return
// the deleted rows as `(name, age)` tuples in response order.
fn execute_sql_delete_returning_name_age_rows(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> NameAgeRows {
    let returning_sql = format!("{sql} RETURNING name, age");

    statement_projection_rows::<SessionSqlEntity>(session, returning_sql.as_str())
        .unwrap_or_else(|err| {
            panic!("DELETE SQL statement execution should execute with RETURNING: {err:?}")
        })
        .into_iter()
        .map(|row| {
            let [Value::Text(name), Value::Nat64(age)] = row.as_slice() else {
                panic!("DELETE RETURNING name, age should preserve two-column value rows");
            };
            (name.clone(), *age)
        })
        .collect::<Vec<_>>()
}

// Load the remaining rows after a delete through one stable age-ordered
// session surface.
fn remaining_session_name_age_rows(session: &DbSession<SessionSqlCanister>) -> NameAgeRows {
    execute_sql_name_age_rows(session, "SELECT * FROM SessionSqlEntity ORDER BY age ASC")
}

const PUBLIC_DELETE_WRITE_ROWS: [(u64, &str, u64); 4] = [
    (1, "Ada", 21),
    (2, "Bea", 22),
    (3, "Cid", 23),
    (4, "Dee", 24),
];

fn seed_public_delete_write_entities(session: &DbSession<SessionSqlCanister>) {
    for (id, name, age) in PUBLIC_DELETE_WRITE_ROWS {
        session
            .insert(SessionSqlWriteEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("public DELETE fixture insert should succeed");
    }
}

fn public_delete_write_rows(session: &DbSession<SessionSqlCanister>) -> Vec<Vec<Value>> {
    statement_projection_rows::<SessionSqlWriteEntity>(
        session,
        "SELECT id, name, age FROM SessionSqlWriteEntity ORDER BY id ASC",
    )
    .expect("post-delete public SQL projection should succeed")
}

fn public_delete_fixture_row(id: u64) -> Vec<Value> {
    let Some((_, name, age)) = PUBLIC_DELETE_WRITE_ROWS
        .iter()
        .find(|(fixture_id, _, _)| *fixture_id == id)
    else {
        panic!("unknown public DELETE fixture id {id}");
    };

    vec![
        Value::Nat64(id),
        Value::Text((*name).to_string()),
        Value::Nat64(*age),
    ]
}

fn public_delete_fixture_rows(ids: &[u64]) -> Vec<Vec<Value>> {
    ids.iter().copied().map(public_delete_fixture_row).collect()
}

fn public_primary_key_delete_plan(sql: &str) -> SqlPublicPrimaryKeyDeletePlan {
    let report = classify_sql_delete_policy(
        sql,
        SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
        SqlDeletePolicyContext::new(&["id"]),
    )
    .expect("public primary-key DELETE SQL should parse");

    let Some(SqlValidatedDeletePlan::PublicPrimaryKeyOnly(plan)) = report.plan else {
        panic!("public primary-key DELETE SQL should produce a primary-key plan");
    };

    plan
}

fn public_bounded_delete_plan_with_row_cap(
    sql: &str,
    max_returning_rows: Option<u32>,
) -> SqlPublicBoundedDeletePlan {
    let report = classify_sql_delete_policy(
        sql,
        SqlDeleteExposurePolicy::PublicBoundedDeterministic,
        SqlDeletePolicyContext {
            primary_key_fields: &["id"],
            max_public_bounded_limit: 10,
            max_returning_rows,
            max_returning_response_bytes: None,
        },
    )
    .expect("public bounded DELETE SQL should parse");

    let Some(SqlValidatedDeletePlan::PublicBoundedDeterministic(plan)) = report.plan else {
        panic!("public bounded DELETE SQL should produce a bounded plan");
    };

    plan
}

fn public_bounded_delete_plan_with_caps(
    sql: &str,
    max_staged_rows: Option<u32>,
    max_returning_rows: Option<u32>,
) -> SqlPublicBoundedDeletePlan {
    let mut plan = public_bounded_delete_plan_with_row_cap(sql, max_returning_rows);
    let mut execution_bounds = plan.execution_bounds();
    execution_bounds.max_staged_rows = max_staged_rows;
    plan.set_execution_bounds_for_tests(execution_bounds);

    plan
}

fn execute_public_bounded_delete_count(
    session: &DbSession<SessionSqlCanister>,
    plan: &SqlPublicBoundedDeletePlan,
    context: &str,
) -> u32 {
    let payload = session
        .execute_validated_sql_public_bounded_delete::<SessionSqlWriteEntity>(plan)
        .unwrap_or_else(|err| panic!("{context} should execute: {err:?}"));
    let SqlStatementResult::Count { row_count } = payload else {
        panic!("{context} should return a count payload");
    };

    row_count
}

fn assert_public_bounded_delete_count_succeeds(
    context: &str,
    sql: &str,
    max_staged_rows: u32,
    expected_row_count: u32,
    remaining_ids: &[u64],
) {
    reset_session_sql_store();
    let session = sql_session();
    seed_public_delete_write_entities(&session);
    let plan = public_bounded_delete_plan_with_caps(sql, Some(max_staged_rows), None);

    assert_eq!(
        execute_public_bounded_delete_count(&session, &plan, context),
        expected_row_count,
        "{context} should preserve affected-row count",
    );
    assert_eq!(
        public_delete_write_rows(&session),
        public_delete_fixture_rows(remaining_ids),
        "{context} should preserve persisted rows",
    );
}

fn assert_public_bounded_delete_rejects_without_mutation(
    context: &str,
    sql: &str,
    max_staged_rows: Option<u32>,
    max_returning_rows: Option<u32>,
    expected_boundary: SqlWriteBoundaryCode,
) {
    reset_session_sql_store();
    let session = sql_session();
    seed_public_delete_write_entities(&session);
    let plan = public_bounded_delete_plan_with_caps(sql, max_staged_rows, max_returning_rows);

    let err = session
        .execute_validated_sql_public_bounded_delete::<SessionSqlWriteEntity>(&plan)
        .expect_err(context);

    assert_sql_write_boundary_detail(err, expected_boundary);
    assert_eq!(
        public_delete_write_rows(&session),
        public_delete_fixture_rows(&[1, 2, 3, 4]),
        "{context} should reject before mutating rows",
    );
}

// Run one SQL DELETE statement through unified statement and return only the
// affected-row count from the traditional mutation result surface.
fn execute_sql_statement_delete_count(session: &DbSession<SessionSqlCanister>, sql: &str) -> u32 {
    let payload = execute_sql_statement_for_tests::<SessionSqlEntity>(session, sql)
        .unwrap_or_else(|err| panic!("DELETE SQL statement execution should execute: {err:?}"));

    match payload {
        SqlStatementResult::Count { row_count } => row_count,
        other => {
            panic!("DELETE SQL statement execution should return count payload, got {other:?}")
        }
    }
}

#[test]
fn execute_validated_sql_public_primary_key_delete_plan_deletes_one_row() {
    reset_session_sql_store();
    let session = sql_session();
    seed_public_delete_write_entities(&session);
    let plan = public_primary_key_delete_plan("DELETE FROM SessionSqlWriteEntity WHERE id = 1");

    let payload = session
        .execute_validated_sql_public_primary_key_delete::<SessionSqlWriteEntity>(&plan)
        .expect("validated public primary-key DELETE should execute");

    let SqlStatementResult::Count { row_count } = payload else {
        panic!("validated public primary-key DELETE should return a count payload");
    };
    assert_eq!(row_count, 1);
    assert_eq!(
        public_delete_write_rows(&session),
        public_delete_fixture_rows(&[2, 3, 4]),
    );
}

#[test]
fn execute_validated_sql_public_bounded_delete_characterizes_exact_staged_bounds() {
    assert_public_bounded_delete_count_succeeds(
        "empty DELETE at zero staged bound",
        "DELETE FROM SessionSqlWriteEntity WHERE age > 99 ORDER BY id LIMIT 1",
        0,
        0,
        &[1, 2, 3, 4],
    );
    assert_public_bounded_delete_count_succeeds(
        "single selected row at exact staged bound",
        "DELETE FROM SessionSqlWriteEntity WHERE age >= 21 ORDER BY id LIMIT 1",
        1,
        1,
        &[2, 3, 4],
    );
    assert_public_bounded_delete_count_succeeds(
        "limit-windowed rows at exact staged bound",
        "DELETE FROM SessionSqlWriteEntity WHERE age >= 21 ORDER BY id LIMIT 2",
        2,
        2,
        &[3, 4],
    );
}

#[test]
fn execute_validated_sql_public_bounded_delete_characterizes_over_bound_atomicity() {
    assert_public_bounded_delete_rejects_without_mutation(
        "one selected row over zero staged bound",
        "DELETE FROM SessionSqlWriteEntity WHERE age >= 21 ORDER BY id LIMIT 1",
        Some(0),
        None,
        SqlWriteBoundaryCode::StagedRowsTooMany,
    );
    assert_public_bounded_delete_rejects_without_mutation(
        "two selected rows over one-row staged bound",
        "DELETE FROM SessionSqlWriteEntity WHERE age >= 21 ORDER BY id LIMIT 2",
        Some(1),
        None,
        SqlWriteBoundaryCode::StagedRowsTooMany,
    );
}

#[test]
fn execute_validated_sql_public_bounded_delete_returning_characterizes_order_and_caps() {
    reset_session_sql_store();
    let session = sql_session();
    seed_public_delete_write_entities(&session);
    let plan = public_bounded_delete_plan_with_caps(
        "DELETE FROM SessionSqlWriteEntity \
         WHERE age >= 21 ORDER BY id ASC LIMIT 2 RETURNING id",
        Some(2),
        Some(2),
    );

    let payload = session
        .execute_validated_sql_public_bounded_delete::<SessionSqlWriteEntity>(&plan)
        .expect("exactly bounded DELETE RETURNING should execute");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
        ..
    } = payload
    else {
        panic!("DELETE RETURNING should return projection payload");
    };

    assert_eq!(columns, ["id"]);
    assert_eq!(
        rows,
        vec![vec![output(Value::Nat64(1))], vec![output(Value::Nat64(2))],],
        "DELETE RETURNING should preserve ordered candidate output",
    );
    assert_eq!(row_count, 2);
    assert_eq!(
        public_delete_write_rows(&session),
        public_delete_fixture_rows(&[3, 4]),
    );
}

#[test]
fn execute_validated_sql_public_bounded_delete_returning_characterizes_limit_precedence() {
    let sql = "DELETE FROM SessionSqlWriteEntity \
               WHERE age >= 21 ORDER BY id LIMIT 2 RETURNING id";

    assert_public_bounded_delete_rejects_without_mutation(
        "RETURNING row cap alone uses current DELETE staged-row boundary",
        sql,
        Some(10),
        Some(1),
        SqlWriteBoundaryCode::StagedRowsTooMany,
    );
    assert_public_bounded_delete_rejects_without_mutation(
        "combined staged and RETURNING row cap uses staged-row boundary",
        sql,
        Some(1),
        Some(1),
        SqlWriteBoundaryCode::StagedRowsTooMany,
    );
}

#[test]
fn execute_validated_sql_public_bounded_delete_count_rejects_bound_before_commit() {
    reset_session_sql_store();
    let session = sql_session();
    seed_public_delete_write_entities(&session);
    let mut plan = public_bounded_delete_plan_with_row_cap(
        "DELETE FROM SessionSqlWriteEntity WHERE age >= 21 ORDER BY id LIMIT 2",
        None,
    );
    let mut execution_bounds = plan.execution_bounds();
    execution_bounds.max_staged_rows = Some(1);
    plan.set_execution_bounds_for_tests(execution_bounds);

    let err = session
        .execute_validated_sql_public_bounded_delete::<SessionSqlWriteEntity>(&plan)
        .expect_err("stricter validated DELETE bound should reject before commit");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::StagedRowsTooMany);
    assert_eq!(
        public_delete_write_rows(&session),
        public_delete_fixture_rows(&[1, 2, 3, 4]),
    );
}

#[test]
fn execute_validated_sql_public_bounded_delete_returning_rejects_row_cap_before_commit() {
    reset_session_sql_store();
    let session = sql_session();
    seed_public_delete_write_entities(&session);
    let plan = public_bounded_delete_plan_with_row_cap(
        "DELETE FROM SessionSqlWriteEntity WHERE age >= 21 ORDER BY id LIMIT 2 RETURNING id",
        Some(1),
    );

    let err = session
        .execute_validated_sql_public_bounded_delete::<SessionSqlWriteEntity>(&plan)
        .expect_err("stricter validated DELETE RETURNING row cap should reject before commit");

    assert_sql_write_boundary_detail(err, SqlWriteBoundaryCode::StagedRowsTooMany);
    assert_eq!(
        public_delete_write_rows(&session),
        public_delete_fixture_rows(&[1, 2, 3, 4]),
    );
}

#[test]
fn execute_sql_public_bounded_delete_derives_context_and_deletes_limited_rows() {
    reset_session_sql_store();
    let session = sql_session();
    seed_public_delete_write_entities(&session);

    let payload = session
        .execute_sql_public_bounded_delete::<SessionSqlWriteEntity>(
            "DELETE FROM SessionSqlWriteEntity WHERE age >= 21 ORDER BY id LIMIT 2",
        )
        .expect("schema-derived public bounded DELETE should execute");

    let SqlStatementResult::Count { row_count } = payload else {
        panic!("schema-derived public bounded DELETE should return a count payload");
    };
    assert_eq!(row_count, 2);
    assert_eq!(
        public_delete_write_rows(&session),
        public_delete_fixture_rows(&[3, 4]),
    );
}

#[test]
fn execute_sql_public_primary_key_delete_rejects_non_pk_where_without_mutation() {
    reset_session_sql_store();
    let session = sql_session();
    seed_public_delete_write_entities(&session);

    session
        .execute_sql_public_primary_key_delete::<SessionSqlWriteEntity>(
            "DELETE FROM SessionSqlWriteEntity WHERE age = 21",
        )
        .expect_err("public primary-key DELETE should reject non-primary-key predicates");

    assert_eq!(
        public_delete_write_rows(&session),
        public_delete_fixture_rows(&[1, 2, 3, 4]),
    );
}

#[test]
fn execute_sql_delete_ordered_window_matrix_honors_delete_shape() {
    let cases = [
        (
            "ordered limit",
            "minor",
            "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1",
            vec![("first-minor".to_string(), 16)],
            vec![("second-minor".to_string(), 17), ("adult".to_string(), 42)],
        ),
        (
            "ordered offset then limit",
            "offset",
            "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1 OFFSET 1",
            vec![("second-minor".to_string(), 17)],
            vec![
                ("first-minor".to_string(), 16),
                ("third-minor".to_string(), 18),
                ("adult".to_string(), 42),
            ],
        ),
        (
            "single-table alias",
            "minor",
            "DELETE FROM SessionSqlEntity alias \
             WHERE alias.age < 20 \
             ORDER BY alias.age ASC LIMIT 1",
            vec![("first-minor".to_string(), 16)],
            vec![("second-minor".to_string(), 17), ("adult".to_string(), 42)],
        ),
    ];

    for (context, fixture, sql, expected_deleted, expected_remaining) in cases {
        reset_session_sql_store();
        let session = sql_session();

        match fixture {
            "minor" => seed_delete_minor_fixture(&session),
            "offset" => seed_delete_offset_fixture(&session),
            _ => unreachable!("delete ordered window matrix uses fixed fixtures"),
        }

        let deleted = execute_sql_delete_returning_name_age_rows(&session, sql);
        let remaining = remaining_session_name_age_rows(&session);

        assert_eq!(
            deleted, expected_deleted,
            "{context} should preserve deleted-row ordering",
        );
        assert_eq!(
            remaining, expected_remaining,
            "{context} should preserve remaining-row semantics",
        );
    }
}

#[test]
fn execute_sql_delete_ulid_string_literal_predicate_removes_matching_row() {
    reset_session_sql_store();
    let session = sql_session();
    let target_id = Ulid::from_u128(9_921);
    let other_id = Ulid::from_u128(9_922);
    let sql = format!("DELETE FROM SessionSqlEntity WHERE id = '{target_id}'");

    session
        .insert(SessionSqlEntity {
            id: target_id,
            name: "delete-target".to_string(),
            age: 21,
        })
        .expect("target ULID delete seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: other_id,
            name: "delete-other".to_string(),
            age: 22,
        })
        .expect("other ULID delete seed insert should succeed");

    let row_count = execute_sql_statement_delete_count(&session, sql.as_str());
    let remaining = remaining_session_name_age_rows(&session);

    assert_eq!(row_count, 1, "quoted ULID delete should affect one row");
    assert_eq!(remaining, vec![("delete-other".to_string(), 22)]);
}

#[test]
fn scalar_select_helper_rejects_delete_lane_on_typed_entity_surface() {
    reset_session_sql_store();
    let session = sql_session();

    for sql in [
        "DELETE FROM SessionSqlEntity WHERE age < 20",
        "DELETE FROM SessionSqlEntity WHERE age < 20 RETURNING id",
    ] {
        let err = execute_scalar_select_for_tests::<SessionSqlEntity>(&session, sql)
            .expect_err("scalar SELECT helper DELETE should stay off the entity-response surface");

        assert_runtime_unsupported_query_execution_diagnostic(
            err,
            "scalar SELECT helper DELETE should preserve the unsupported-lane diagnostic",
        );
    }
}

#[test]
fn delete_returning_structural_row_bound_rejects_before_commit() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[("first-minor", 16), ("second-minor", 17), ("adult", 42)],
    );

    let compiled = session
        .compile_sql_update::<SessionSqlEntity>(
            "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC RETURNING name",
        )
        .expect("DELETE RETURNING should compile through the update surface");
    let crate::db::session::sql::CompiledSqlCommand::Delete { query, .. } = compiled else {
        panic!("DELETE RETURNING should compile to the delete command lane");
    };
    let typed_query = Query::<SessionSqlEntity>::from_inner(query.as_ref().clone());
    let (plan, _) = session
        .cached_prepared_query_plan_for_entity::<SessionSqlEntity>(&typed_query)
        .expect("DELETE RETURNING query plan should prepare");
    let result = session
        .delete_executor::<SessionSqlEntity>()
        .execute_structural_projection_with_bounds(
            plan,
            crate::db::executor::DeleteProjectionBounds::max_rows(1),
            |_| Ok(()),
        );
    let Err(err) = result else {
        panic!("row-bound DELETE RETURNING should reject before commit");
    };

    assert_sql_write_boundary_detail(
        QueryError::execute(err),
        SqlWriteBoundaryCode::StagedRowsTooMany,
    );
    assert_eq!(
        remaining_session_name_age_rows(&session),
        vec![
            ("first-minor".to_string(), 16),
            ("second-minor".to_string(), 17),
            ("adult".to_string(), 42),
        ],
    );
}

#[test]
fn fluent_delete_returns_count_without_materializing_deleted_rows() {
    reset_session_sql_store();
    let session = sql_session();
    seed_delete_minor_fixture(&session);

    let row_count = session
        .delete::<SessionSqlEntity>()
        .order_term(crate::db::asc("age"))
        .limit(1)
        .execute()
        .expect("fluent delete should return count payload");
    let remaining = remaining_session_name_age_rows(&session);

    assert_eq!(row_count, 1);
    assert_eq!(
        remaining,
        vec![("second-minor".to_string(), 17), ("adult".to_string(), 42)],
        "fluent delete should still honor ordered delete semantics while returning count only",
    );
}

#[test]
fn execute_sql_statement_delete_returns_count_without_returning() {
    reset_session_sql_store();
    let session = sql_session();
    seed_delete_minor_fixture(&session);

    let row_count = execute_sql_statement_delete_count(
        &session,
        "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1",
    );
    let remaining = remaining_session_name_age_rows(&session);

    assert_eq!(row_count, 1, "bare DELETE should return affected-row count");
    assert_eq!(
        remaining,
        vec![("second-minor".to_string(), 17), ("adult".to_string(), 42)],
        "bare DELETE should still apply the ordered delete window",
    );
}

#[test]
fn execute_sql_statement_delete_returning_projection_matrix_projects_deleted_rows() {
    for (sql, expect_full_row, context) in [
        (
            "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1 RETURNING name, age",
            false,
            "DELETE RETURNING field list",
        ),
        (
            "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1 RETURNING *",
            true,
            "DELETE RETURNING star",
        ),
    ] {
        reset_session_sql_store();
        let session = sql_session();
        seed_delete_minor_fixture(&session);

        let rows = statement_projection_rows::<SessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should return deleted rows: {err:?}"));
        let remaining = remaining_session_name_age_rows(&session);

        assert_eq!(rows.len(), 1, "{context} should emit one deleted row");

        if expect_full_row {
            assert_eq!(
                rows[0].len(),
                3,
                "{context} should preserve full entity field width",
            );
            assert!(
                matches!(rows[0][0], Value::Ulid(_)),
                "{context} should preserve the generated primary key slot",
            );
            assert_eq!(
                rows[0][1..],
                [Value::Text("first-minor".to_string()), Value::Nat64(16),],
                "{context} should preserve the deleted name and age in field order",
            );
        } else {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("first-minor".to_string()),
                    Value::Nat64(16)
                ]],
                "{context} should project only the requested deleted-row fields",
            );
        }

        assert_eq!(
            remaining,
            vec![("second-minor".to_string(), 17), ("adult".to_string(), 42)],
            "{context} should preserve delete side effects",
        );
    }
}

#[test]
fn execute_sql_delete_searched_case_where_matches_expected_deleted_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic delete matrix for the searched-CASE
    // scalar WHERE seam shared with load execution.
    seed_session_sql_entities(
        &session,
        &[
            ("delete-case-a", 10),
            ("delete-case-b", 20),
            ("delete-case-c", 30),
            ("delete-case-d", 40),
        ],
    );

    // Phase 2: require delete post-access filtering to preserve the same
    // searched-CASE row semantics as scalar load execution.
    let deleted = execute_sql_delete_returning_name_age_rows(
        &session,
        "DELETE FROM SessionSqlEntity \
         WHERE CASE WHEN age >= 30 THEN TRUE ELSE age = 20 END \
         ORDER BY age ASC",
    );
    let remaining = remaining_session_name_age_rows(&session);

    assert_eq!(
        deleted,
        vec![
            ("delete-case-b".to_string(), 20),
            ("delete-case-c".to_string(), 30),
            ("delete-case-d".to_string(), 40),
        ],
        "searched CASE delete WHERE should keep the same row admission semantics as scalar load execution",
    );
    assert_eq!(
        remaining,
        vec![("delete-case-a".to_string(), 10)],
        "searched CASE delete WHERE should leave only the non-matching row behind",
    );
}

#[test]
fn execute_sql_delete_wrapped_like_and_ilike_where_match_expected_deleted_rows() {
    let seed_rows = [
        ("alpha", 10_u64),
        ("alpine", 20_u64),
        ("bravo", 30_u64),
        ("charlie", 40_u64),
    ];

    for (sql, context) in [
        (
            "DELETE FROM SessionSqlEntity \
             WHERE REPLACE(name, 'a', 'A') LIKE 'Al%' \
             ORDER BY age ASC",
            "wrapped LIKE delete WHERE query",
        ),
        (
            "DELETE FROM SessionSqlEntity \
             WHERE REPLACE(name, 'a', 'A') ILIKE 'al%' \
             ORDER BY age ASC",
            "wrapped ILIKE delete WHERE query",
        ),
    ] {
        reset_session_sql_store();
        let session = sql_session();
        seed_session_sql_entities(&session, &seed_rows);

        let deleted = execute_sql_delete_returning_name_age_rows(&session, sql);
        let remaining = remaining_session_name_age_rows(&session);

        assert_eq!(
            deleted,
            vec![("alpha".to_string(), 10), ("alpine".to_string(), 20)],
            "{context} should preserve the widened wrapped LIKE/ILIKE delete row semantics",
        );
        assert_eq!(
            remaining,
            vec![("bravo".to_string(), 30), ("charlie".to_string(), 40)],
            "{context} should preserve delete side effects for the non-matching rows",
        );
    }
}

#[test]
fn execute_sql_delete_text_predicate_expression_arguments_where_match_expected_deleted_rows() {
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

    let deleted = execute_sql_delete_returning_name_age_rows(
        &session,
        "DELETE FROM SessionSqlEntity \
         WHERE STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al')) \
         ORDER BY age ASC",
    );
    let remaining = remaining_session_name_age_rows(&session);

    assert_eq!(
        deleted,
        vec![("alpha".to_string(), 10), ("alpine".to_string(), 20)],
        "text predicate expression arguments delete WHERE should preserve the widened residual-filter row semantics",
    );
    assert_eq!(
        remaining,
        vec![("bravo".to_string(), 30), ("charlie".to_string(), 40)],
        "text predicate expression arguments delete WHERE should leave only the non-matching rows behind",
    );
}

#[test]
fn execute_sql_delete_compare_boolean_constant_where_match_expected_deleted_rows() {
    let seed_rows = [
        ("alpha", 10_u64),
        ("alpine", 20_u64),
        ("bravo", 30_u64),
        ("charlie", 40_u64),
    ];

    for (sql, expected_deleted, expected_remaining, context) in [
        (
            "DELETE FROM SessionSqlEntity \
             WHERE name = TRIM('alpha') OR NULLIF('alpha', 'alpha') IS NOT NULL \
             ORDER BY age ASC",
            vec![("alpha".to_string(), 10_u64)],
            vec![
                ("alpine".to_string(), 20_u64),
                ("bravo".to_string(), 30_u64),
                ("charlie".to_string(), 40_u64),
            ],
            "compare OR FALSE delete WHERE query",
        ),
        (
            "DELETE FROM SessionSqlEntity \
             WHERE name = TRIM('alpha') OR NULLIF('alpha', 'alpha') IS NULL \
             ORDER BY age ASC",
            vec![
                ("alpha".to_string(), 10_u64),
                ("alpine".to_string(), 20_u64),
                ("bravo".to_string(), 30_u64),
                ("charlie".to_string(), 40_u64),
            ],
            vec![],
            "compare OR TRUE delete WHERE query",
        ),
    ] {
        reset_session_sql_store();
        let session = sql_session();
        seed_session_sql_entities(&session, &seed_rows);

        let deleted = execute_sql_delete_returning_name_age_rows(&session, sql);
        let remaining = remaining_session_name_age_rows(&session);

        assert_eq!(
            deleted, expected_deleted,
            "{context} should preserve the boolean-simplified delete row semantics",
        );
        assert_eq!(
            remaining, expected_remaining,
            "{context} should preserve the expected remaining rows after delete",
        );
    }
}

#[test]
fn execute_sql_delete_casefold_text_predicate_boolean_constant_where_match_expected_deleted_rows() {
    let seed_rows = [
        ("alpha", 10_u64),
        ("alpine", 20_u64),
        ("bravo", 30_u64),
        ("charlie", 40_u64),
    ];

    for (sql, expected_deleted, expected_remaining, context) in [
        (
            "DELETE FROM SessionSqlEntity \
             WHERE STARTS_WITH(LOWER(name), TRIM('AL')) \
               OR NULLIF('alpha', 'alpha') IS NOT NULL \
             ORDER BY age ASC",
            vec![
                ("alpha".to_string(), 10_u64),
                ("alpine".to_string(), 20_u64),
            ],
            vec![
                ("bravo".to_string(), 30_u64),
                ("charlie".to_string(), 40_u64),
            ],
            "casefold text predicate OR FALSE delete WHERE query",
        ),
        (
            "DELETE FROM SessionSqlEntity \
             WHERE STARTS_WITH(LOWER(name), TRIM('AL')) \
               OR NULLIF('alpha', 'alpha') IS NULL \
             ORDER BY age ASC",
            vec![
                ("alpha".to_string(), 10_u64),
                ("alpine".to_string(), 20_u64),
                ("bravo".to_string(), 30_u64),
                ("charlie".to_string(), 40_u64),
            ],
            vec![],
            "casefold text predicate OR TRUE delete WHERE query",
        ),
    ] {
        reset_session_sql_store();
        let session = sql_session();
        seed_session_sql_entities(&session, &seed_rows);

        let deleted = execute_sql_delete_returning_name_age_rows(&session, sql);
        let remaining = remaining_session_name_age_rows(&session);

        assert_eq!(
            deleted, expected_deleted,
            "{context} should preserve the boolean-simplified casefold delete row semantics",
        );
        assert_eq!(
            remaining, expected_remaining,
            "{context} should preserve the expected remaining rows after delete",
        );
    }
}

#[test]
fn execute_sql_delete_casefold_compare_boolean_constant_where_match_expected_deleted_rows() {
    let seed_rows = [
        ("alpha", 10_u64),
        ("alpine", 20_u64),
        ("bravo", 30_u64),
        ("charlie", 40_u64),
    ];

    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &seed_rows);

    let deleted = execute_sql_delete_returning_name_age_rows(
        &session,
        "DELETE FROM SessionSqlEntity \
         WHERE LOWER(name) = TRIM('ALPHA') \
           OR NULLIF('alpha', 'alpha') IS NOT NULL \
         ORDER BY age ASC",
    );
    let remaining = remaining_session_name_age_rows(&session);

    assert_eq!(
        deleted,
        vec![("alpha".to_string(), 10_u64)],
        "casefold compare OR FALSE delete WHERE should preserve the recovered casefold compare row semantics",
    );
    assert_eq!(
        remaining,
        vec![
            ("alpine".to_string(), 20_u64),
            ("bravo".to_string(), 30_u64),
            ("charlie".to_string(), 40_u64),
        ],
        "casefold compare OR FALSE delete WHERE should preserve the expected remaining rows",
    );
}

#[test]
fn execute_sql_delete_matrix_queries_match_deleted_and_remaining_rows() {
    // Phase 1: define one shared seed dataset and table-driven DELETE cases.
    let seed_rows = [
        ("delete-matrix-a", 10_u64),
        ("delete-matrix-b", 20_u64),
        ("delete-matrix-c", 30_u64),
        ("delete-matrix-d", 40_u64),
    ];
    let cases = vec![
        (
            "DELETE FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age ASC LIMIT 1",
            vec![("delete-matrix-b".to_string(), 20_u64)],
            vec![
                ("delete-matrix-a".to_string(), 10_u64),
                ("delete-matrix-c".to_string(), 30_u64),
                ("delete-matrix-d".to_string(), 40_u64),
            ],
        ),
        (
            "DELETE FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age DESC LIMIT 2",
            vec![
                ("delete-matrix-d".to_string(), 40_u64),
                ("delete-matrix-c".to_string(), 30_u64),
            ],
            vec![
                ("delete-matrix-a".to_string(), 10_u64),
                ("delete-matrix-b".to_string(), 20_u64),
            ],
        ),
        (
            "DELETE FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age ASC LIMIT 1 OFFSET 1",
            vec![("delete-matrix-c".to_string(), 30_u64)],
            vec![
                ("delete-matrix-a".to_string(), 10_u64),
                ("delete-matrix-b".to_string(), 20_u64),
                ("delete-matrix-d".to_string(), 40_u64),
            ],
        ),
        (
            "DELETE FROM SessionSqlEntity \
             WHERE age >= 100 \
             ORDER BY age ASC LIMIT 1",
            vec![],
            vec![
                ("delete-matrix-a".to_string(), 10_u64),
                ("delete-matrix-b".to_string(), 20_u64),
                ("delete-matrix-c".to_string(), 30_u64),
                ("delete-matrix-d".to_string(), 40_u64),
            ],
        ),
    ];

    // Phase 2: execute each DELETE case from a fresh seeded store.
    for (sql, expected_deleted, expected_remaining) in cases {
        reset_session_sql_store();
        let session = sql_session();
        seed_session_sql_entities(&session, &seed_rows);

        let deleted_rows = execute_sql_delete_returning_name_age_rows(&session, sql);
        let remaining_rows = remaining_session_name_age_rows(&session);

        assert_eq!(
            deleted_rows, expected_deleted,
            "delete matrix deleted rows: {sql}"
        );
        assert_eq!(
            remaining_rows, expected_remaining,
            "delete matrix remaining rows: {sql}",
        );
    }
}
