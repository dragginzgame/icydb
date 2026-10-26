//! Authored SQL field names are checked against the accepted catalog before effects.

use super::*;
use crate::db::{MutationJobError, MutationJobId, QueryError, schema::ValidateError};

#[test]
fn sql_scope_paths_never_retarget_unknown_record_paths_to_primary_keys() {
    let session = initialize();
    insert_row(&session, 1, "before", "group-a");
    let read = "SELECT id, common FROM PlannerRow ORDER BY id";
    let before = projection_rows(&session, read);
    for path in ["owner.PlannerRow.id", "owner.p.id"] {
        for sql in [
            format!("SELECT {path} FROM PlannerRow p"),
            format!("SELECT id FROM PlannerRow p WHERE {path} = 1"),
            format!("UPDATE PlannerRow p SET common = 'changed' WHERE {path} = 1"),
            format!("DELETE FROM PlannerRow p WHERE {path} = 1"),
        ] {
            let result = if sql.starts_with("SELECT ") {
                session.execute_trusted_sql_query(&sql)
            } else if sql.starts_with("UPDATE ") {
                session.execute_trusted_sql_exact_update(&sql, 1)
            } else {
                session.execute_trusted_sql_mutation(&sql)
            };
            let error = result.expect_err(&sql);
            let unknown = match &error {
                QueryError::Validate(error) => matches!(
                    error.as_ref(), ValidateError::UnknownField { field }
                    if field == "owner" || field == path
                ),
                QueryError::Plan(_) => error
                    .query_field_context()
                    .is_some_and(|(_, field)| field == "owner" || field == path),
                _ => false,
            };
            assert!(unknown, "{sql}: {error:?}");
            assert_eq!(projection_rows(&session, read), before);
        }
    }
}

#[test]
fn sql_write_fields_validate_public_update_plans_against_the_same_catalog() {
    use crate::db::{
        query::preparation::with_preparation_work,
        schema::AcceptedRowLayoutRuntimeContract,
        session::sql::{
            SqlUpdateExposurePolicy, SqlValidatedUpdatePlan, classify_sql_update_policy_for_entity,
            with_accepted_sql_update_policy_context,
        },
        sql_statement_dispatch,
    };

    let session = initialize();
    insert_row(&session, 1, "before", "group-a");
    let read = "SELECT id, common FROM PlannerRow ORDER BY id";
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let descriptor =
        AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot()).unwrap();
    for (exposure, selector) in [
        (
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
            "WHERE id = 1",
        ),
        (
            SqlUpdateExposurePolicy::PublicBoundedDeterministic,
            "WHERE id >= 0 ORDER BY id LIMIT 1",
        ),
    ] {
        for field in ["typo", "common"] {
            let before = projection_rows(&session, read);
            let sql = format!("UPDATE PlannerRow SET {field} = 'after' {selector}");
            let dispatch = sql_statement_dispatch(&sql).unwrap();
            let plan = with_accepted_sql_update_policy_context(&descriptor, |context| {
                with_preparation_work(|work| {
                    classify_sql_update_policy_for_entity(
                        &dispatch,
                        ENTITY_NAME,
                        exposure,
                        context,
                        work,
                    )
                })
            })
            .unwrap()
            .unwrap();
            let result = match plan {
                SqlValidatedUpdatePlan::PublicPrimaryKeyOnly(plan) => {
                    session.execute_validated_sql_public_primary_key_update(&plan)
                }
                SqlValidatedUpdatePlan::PublicBoundedDeterministic(plan) => {
                    session.execute_validated_sql_public_bounded_update(&plan)
                }
                SqlValidatedUpdatePlan::TrustedExact(_) => {
                    panic!("public exposure must produce a public plan");
                }
            };
            if field == "typo" {
                assert_unknown_write_field(result.unwrap_err(), field);
                assert_eq!(projection_rows(&session, read), before);
            } else {
                result.unwrap();
                assert_eq!(
                    projection_rows(&session, read),
                    vec![vec![
                        OutputValue::nat64(1),
                        OutputValue::text("after".into())
                    ]]
                );
            }
        }
    }
}

fn assert_unknown_write_field(error: QueryError, expected: &str) {
    assert!(matches!(
        error,
        QueryError::Validate(error)
            if matches!(*error, ValidateError::UnknownField { ref field } if field == expected)
    ));
}

#[test]
fn sql_write_fields_reject_unknown_update_targets_before_effects() {
    let session = sql_not_null::initialize_nullable_rows();
    let read = "SELECT id, status, qty FROM PlannerRow ORDER BY id";
    let before = projection_rows(&session, read);
    for value in ["'text'", "7", "NULL", "DEFAULT"] {
        for selector in ["id = 1", "id = 99", "id >= 0"] {
            let sql = format!(
                "UPDATE PlannerRow SET status = 'changed', typo = {value} WHERE {selector}"
            );
            assert_unknown_write_field(
                session
                    .execute_trusted_sql_exact_update(&sql, 2)
                    .unwrap_err(),
                "typo",
            );
            assert_eq!(projection_rows(&session, read), before);
        }
        let sql = format!("UPDATE PlannerRow SET typo = {value} WHERE id >= 0 ORDER BY id LIMIT 1");
        assert_unknown_write_field(
            session.execute_trusted_sql_prefix_update(&sql).unwrap_err(),
            "typo",
        );
        assert_eq!(projection_rows(&session, read), before);
    }
    session
        .execute_trusted_sql_exact_update(
            "UPDATE PlannerRow SET status = NULL, qty = 8 WHERE id = 1",
            1,
        )
        .unwrap();
    assert_eq!(
        projection_rows(&session, read),
        std::iter::once(vec![
            OutputValue::nat64(1),
            OutputValue::null(),
            OutputValue::nat64(8)
        ])
        .chain(before.into_iter().skip(1))
        .collect::<Vec<_>>()
    );
}

#[test]
fn sql_write_fields_reject_unknown_insert_columns_before_required_fields_or_source_reads() {
    let session = sql_not_null::initialize_nullable_rows();
    let read = "SELECT id FROM PlannerRow ORDER BY id";
    let before = projection_rows(&session, read);
    for sql in [
        "INSERT INTO PlannerRow (id, marked, typo) VALUES (1, FALSE, 'text')",
        "INSERT INTO PlannerRow (id, marked, typo) VALUES (1, FALSE, 7)",
        "INSERT INTO PlannerRow (id, marked, typo) VALUES (1, FALSE, NULL)",
        "INSERT INTO PlannerRow (id, marked, typo) VALUES (1, FALSE, DEFAULT)",
        "INSERT INTO PlannerRow (typo) VALUES (DEFAULT)",
        "INSERT INTO PlannerRow (id, marked, typo) SELECT id, marked, status FROM PlannerRow",
        "INSERT INTO PlannerRow (id, marked, typo) SELECT id, marked, status FROM PlannerRow WHERE id = 99",
        "INSERT INTO PlannerRow (id, marked, typo, typo) VALUES (1, FALSE, 7, 8)",
    ] {
        assert_unknown_write_field(
            session.execute_trusted_sql_mutation(sql).unwrap_err(),
            "typo",
        );
        assert_eq!(projection_rows(&session, read), before);
    }
    session.execute_trusted_sql_mutation("INSERT INTO PlannerRow (id, marked, status) VALUES (7, FALSE, NULL), (8, TRUE, 'text')").unwrap();
    assert_eq!(
        projection_rows(&session, read),
        before
            .into_iter()
            .chain([vec![OutputValue::nat64(7)], vec![OutputValue::nat64(8)]])
            .collect::<Vec<_>>()
    );
}

#[test]
fn sql_write_fields_reject_unknown_resumable_targets_without_persisting_jobs() {
    let session = initialize_journaled();
    insert_row(&session, 1, "before", "group-a");
    let read = "SELECT id, common FROM PlannerRow ORDER BY id";
    let before = projection_rows(&session, read);
    let inventory = session.progress_job_inventory().unwrap();
    for (index, value) in ["'text'", "NULL", "DEFAULT"].into_iter().enumerate() {
        let job = MutationJobId::try_from_bytes([u8::try_from(index + 1).unwrap(); 32]).unwrap();
        assert_eq!(
            session.start_trusted_sql_mutation_job(
                job,
                &format!("UPDATE PlannerRow SET typo = {value} WHERE id >= 0")
            ),
            Err(MutationJobError::IneligibleIntent),
        );
        assert_eq!(session.progress_job_inventory().unwrap(), inventory);
        assert_eq!(projection_rows(&session, read), before);
    }
    let job = MutationJobId::try_from_bytes([8; 32]).unwrap();
    assert!(
        session
            .start_trusted_sql_mutation_job(
                job,
                "UPDATE PlannerRow SET common = 'after' WHERE id >= 0"
            )
            .is_ok()
    );
    assert_eq!(projection_rows(&session, read), before);
}

#[test]
fn sql_public_delete_prepares_aliases_before_policy_and_execution() {
    use crate::db::sql_statement_dispatch;

    let session = initialize();
    insert_row(&session, 2, "second", "group-a");

    for (bounded, returning) in [(false, false), (false, true), (true, false), (true, true)] {
        for (target, qualifier) in [
            ("PlannerRow", ""),
            ("PlannerRow", "PlannerRow."),
            ("PlannerRow p", "p."),
            ("PlannerRow P", "p."),
            ("PlannerRow common", "common."),
        ] {
            insert_row(&session, 1, "first", "group-a");
            let window = if bounded {
                format!(" ORDER BY {qualifier}id ASC LIMIT 1")
            } else {
                String::new()
            };
            let output = if returning {
                format!(" RETURNING {qualifier}id, {qualifier}common")
            } else {
                String::new()
            };
            let sql = format!("DELETE FROM {target} WHERE {qualifier}id = 1{window}{output}");
            let dispatch = sql_statement_dispatch(&sql).unwrap();
            let result = if bounded {
                session.execute_sql_public_bounded_delete(&dispatch)
            } else {
                session.execute_sql_public_primary_key_delete(&dispatch)
            };
            match result.unwrap() {
                SqlStatementResult::Count { row_count } => {
                    assert!(!returning);
                    assert_eq!(row_count, 1);
                }
                SqlStatementResult::Projection {
                    rows, row_count, ..
                } => {
                    assert!(returning);
                    assert_eq!(row_count, 1);
                    assert_eq!(
                        rows,
                        vec![vec![
                            OutputValue::nat64(1),
                            OutputValue::text("first".into())
                        ]]
                    );
                }
                _ => panic!("delete must return its count or requested projection"),
            }
            let rows = projection_rows(&session, "SELECT id, common FROM PlannerRow ORDER BY id");
            assert_eq!(
                rows,
                vec![vec![
                    OutputValue::nat64(2),
                    OutputValue::text("second".to_string())
                ]]
            );
        }
    }
}

#[test]
fn sql_public_delete_rejects_unprepared_inputs_without_writes() {
    use crate::db::sql_statement_dispatch;

    let session = initialize();
    insert_row(&session, 1, "first", "group-a");
    for bounded in [false, true] {
        let read = "SELECT id, common FROM PlannerRow ORDER BY id";
        let before = projection_rows(&session, read);
        for predicate in ["other.id = 1", "p.id = $1"] {
            let window = if bounded {
                " ORDER BY p.id ASC LIMIT 1"
            } else {
                ""
            };
            let sql = format!("DELETE FROM PlannerRow p WHERE {predicate}{window}");
            let result = sql_statement_dispatch(&sql).and_then(|dispatch| {
                if bounded {
                    session.execute_sql_public_bounded_delete(&dispatch)
                } else {
                    session.execute_sql_public_primary_key_delete(&dispatch)
                }
            });
            assert!(result.is_err());
            assert_eq!(projection_rows(&session, read), before);
        }
    }
}

#[test]
fn sql_insert_aliases_normalize_columns_and_returning_on_cold_and_resident_commands() {
    use crate::db::sql_statement_dispatch;

    let session = sql_not_null::initialize_nullable_rows();
    for (target, qualifier) in [
        ("PlannerRow p", "p."),
        ("PlannerRow AS P", "p."),
        ("PlannerRow status", "status."),
        ("PlannerRow", "PlannerRow."),
        ("app.PlannerRow", "app.PlannerRow."),
        ("PlannerRow", ""),
    ] {
        for returning in ["", " RETURNING *", "fields"] {
            let suffix = if returning == "fields" {
                format!(" RETURNING {qualifier}id, {qualifier}status")
            } else {
                returning.to_string()
            };
            let sql = format!(
                "INSERT INTO {target} ({qualifier}id, {qualifier}marked, {qualifier}status) VALUES (7, FALSE, 'new'), (8, TRUE, NULL){suffix}"
            );
            let dispatch = sql_statement_dispatch(&sql).unwrap();
            for resident in [false, true] {
                let result = if resident {
                    session.execute_trusted_sql_mutation_dispatch(&dispatch)
                } else {
                    session.execute_trusted_sql_mutation(&sql)
                }
                .unwrap();
                match result {
                    SqlStatementResult::Count { row_count } => {
                        assert!(returning.is_empty());
                        assert_eq!(row_count, 2);
                    }
                    SqlStatementResult::Projection {
                        columns,
                        rows,
                        row_count,
                        ..
                    } => {
                        assert!(!returning.is_empty());
                        assert_eq!(row_count, 2);
                        if returning == "fields" {
                            assert_eq!(columns, ["id", "status"]);
                            assert_eq!(
                                rows,
                                [
                                    vec![OutputValue::nat64(7), OutputValue::text("new".into())],
                                    vec![OutputValue::nat64(8), OutputValue::null()],
                                ]
                            );
                        }
                    }
                    _ => panic!("INSERT must return its count or requested rows"),
                }
                assert_eq!(
                    projection_rows(
                        &session,
                        "SELECT id, status FROM PlannerRow WHERE id >= 7 ORDER BY id"
                    ),
                    [
                        vec![OutputValue::nat64(7), OutputValue::text("new".into())],
                        vec![OutputValue::nat64(8), OutputValue::null()],
                    ]
                );
                session
                    .execute_trusted_sql_mutation("DELETE FROM PlannerRow WHERE id >= 7")
                    .unwrap();
            }
        }
    }
}

#[test]
fn sql_insert_aliases_keep_select_source_scope_and_reject_wrong_qualifiers_before_writes() {
    let session = sql_not_null::initialize_nullable_rows();
    // Preserve the source's Nat64 family instead of testing arithmetic coercion.
    session
        .execute_trusted_sql_exact_update("UPDATE PlannerRow SET qty = 7 WHERE id = 1", 1)
        .unwrap();
    for target in ["PlannerRow t", "PlannerRow AS T"] {
        for _ in 0..2 {
            let sql = format!(
                "INSERT INTO {target} (t.id, t.marked, t.status) SELECT s.qty, s.marked, s.status FROM PlannerRow s WHERE s.id = 1 RETURNING t.id, t.status"
            );
            let SqlStatementResult::Projection {
                rows, row_count, ..
            } = session.execute_trusted_sql_mutation(&sql).unwrap()
            else {
                panic!("INSERT SELECT should return requested rows");
            };
            assert_eq!(row_count, 1);
            assert_eq!(
                rows,
                [vec![
                    OutputValue::nat64(7),
                    OutputValue::text("archived".into())
                ]]
            );
            session
                .execute_trusted_sql_mutation("DELETE FROM PlannerRow WHERE id = 7")
                .unwrap();
        }
    }
    let before = projection_rows(&session, "SELECT id, status FROM PlannerRow ORDER BY id");
    for sql in [
        "INSERT INTO PlannerRow t (other.id, t.marked) VALUES (7, FALSE)",
        "INSERT INTO PlannerRow t (t.id, t.marked) VALUES (7, FALSE) RETURNING other.status",
        "INSERT INTO PlannerRow t (t.id, t.marked) SELECT t.id, s.marked FROM PlannerRow s WHERE s.id = 99",
        "INSERT INTO PlannerRow t (t.id, t.marked) SELECT $1, s.marked FROM PlannerRow s WHERE s.id = 1",
    ] {
        assert!(session.execute_trusted_sql_mutation(sql).is_err());
        assert_eq!(
            projection_rows(&session, "SELECT id, status FROM PlannerRow ORDER BY id"),
            before
        );
    }
}

#[test]
fn sql_insert_select_bounds_source_output_before_staging_and_preserves_authored_windows() {
    use icydb_diagnostic_code::{DiagnosticDetail, SqlWriteBoundaryCode};

    let session = sql_not_null::initialize_nullable_rows();
    session
        .execute_trusted_sql_mutation("DELETE FROM PlannerRow")
        .unwrap();
    for range in [1..=100, 101..=102] {
        let values = range
            .map(|id| format!("({id}, {}, FALSE)", id + 1000))
            .collect::<Vec<_>>()
            .join(", ");
        session
            .execute_trusted_sql_mutation(&format!(
                "INSERT INTO PlannerRow (id, qty, marked) VALUES {values}"
            ))
            .unwrap();
    }
    let before = projection_rows(&session, "SELECT id FROM PlannerRow ORDER BY id");
    // The 102nd projection would fail if the source ran past the overflow row.
    let source = "SELECT CASE WHEN id = 102 THEN 1 / 0 ELSE qty END, marked FROM PlannerRow WHERE id <= 102 ORDER BY id";
    let error = session
        .execute_trusted_sql_mutation(&format!("INSERT INTO PlannerRow (id, marked) {source}"))
        .unwrap_err();
    assert_eq!(
        error.diagnostic().detail(),
        Some(&DiagnosticDetail::SqlWriteBoundary {
            boundary: SqlWriteBoundaryCode::StagedRowsTooMany,
        })
    );
    assert_eq!(
        projection_rows(&session, "SELECT id FROM PlannerRow ORDER BY id"),
        before
    );
    for (window, expected) in [("LIMIT 0", 0), ("LIMIT 2 OFFSET 1", 2), ("LIMIT 100", 100)] {
        let result = session.execute_trusted_sql_mutation(&format!(
            "INSERT INTO PlannerRow (id, marked) SELECT qty, marked FROM PlannerRow WHERE id <= 102 ORDER BY id {window}"
        )).unwrap();
        assert!(matches!(result, SqlStatementResult::Count { row_count } if row_count == expected));
        let inserted = projection_rows(
            &session,
            "SELECT id FROM PlannerRow WHERE id >= 1000 ORDER BY id",
        );
        assert_eq!(inserted.len(), expected as usize);
        if expected == 2 {
            assert_eq!(
                inserted,
                [
                    vec![OutputValue::nat64(1002)],
                    vec![OutputValue::nat64(1003)]
                ]
            );
        }
        session
            .execute_trusted_sql_mutation("DELETE FROM PlannerRow WHERE id >= 1000")
            .unwrap();
    }
}

#[test]
fn positional_insert_uses_all_unmanaged_fields_without_width_inference() {
    use crate::db::{sql::parser::SqlParseError, sql_shared::SqlSyntaxErrorKind};
    use icydb_diagnostic_code::SqlWriteBoundaryCode;

    let session = sql_not_null::initialize_nullable_rows();
    let read = "SELECT id, status, peer, qty, other, flag, marked FROM PlannerRow ORDER BY id";
    let before = projection_rows(&session, read);
    let tuple_error = QueryError::from_sql_parse_error(SqlParseError::invalid_syntax(
        SqlSyntaxErrorKind::InsertValuesTupleLengthMismatch,
    ));
    for sql in [
        "INSERT INTO PlannerRow VALUES (7, TRUE)",
        "INSERT INTO PlannerRow VALUES (7, 'a', 'b', 1, 2, NULL, TRUE), (8, FALSE)",
    ] {
        let error = session.execute_trusted_sql_mutation(sql).unwrap_err();
        assert_eq!(error.diagnostic_code(), tuple_error.diagnostic_code());
        assert_eq!(error.diagnostic(), tuple_error.diagnostic());
        assert_eq!(projection_rows(&session, read), before);
    }
    let error = session
        .execute_trusted_sql_mutation(
            "INSERT INTO PlannerRow SELECT id + 10, marked FROM PlannerRow",
        )
        .unwrap_err();
    assert_eq!(
        error.diagnostic_code(),
        QueryError::sql_write_boundary(SqlWriteBoundaryCode::InsertSelectWidthMismatch,)
            .diagnostic_code()
    );
    assert_eq!(projection_rows(&session, read), before);

    session
        .execute_trusted_sql_mutation(
            "INSERT INTO PlannerRow VALUES (7, 'first', 'second', 8, 12, NULL, TRUE)",
        )
        .unwrap();
    session.execute_trusted_sql_mutation(
        "INSERT INTO PlannerRow SELECT qty, status, peer, qty, other, flag, marked FROM PlannerRow WHERE id = 7",
    ).unwrap();
    session
        .execute_trusted_sql_mutation("INSERT INTO PlannerRow (id, marked) VALUES (9, FALSE)")
        .unwrap();
    let rows = projection_rows(&session, read);
    assert_eq!(&rows[..before.len()], before);
    for (row, id) in rows[before.len()..before.len() + 2].iter().zip([7, 8]) {
        assert_eq!(
            row,
            &vec![
                OutputValue::nat64(id),
                OutputValue::text("first".into()),
                OutputValue::text("second".into()),
                OutputValue::nat64(8),
                OutputValue::nat64(12),
                OutputValue::null(),
                OutputValue::boolean(true)
            ]
        );
    }
    assert_eq!(
        rows.last().unwrap(),
        &vec![
            OutputValue::nat64(9),
            OutputValue::null(),
            OutputValue::null(),
            OutputValue::null(),
            OutputValue::null(),
            OutputValue::null(),
            OutputValue::boolean(false)
        ]
    );
}

#[test]
fn reserved_constraint_rejection_preserves_later_not_null_publication() {
    use icydb_diagnostic_code::ErrorCode;

    let session = sql_not_null::initialize_nullable_rows();
    let read = "SELECT id FROM PlannerRow ORDER BY id";
    let before = projection_rows(&session, read);
    for sql in [
        "ALTER TABLE PlannerRow ADD CONSTRAINT __icydb_not_null_8 CHECK (TRUE) NOT VALID EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2",
        "CREATE UNIQUE INDEX __icydb_not_null_8 ON PlannerRow (id)",
        "CREATE INDEX __icydb_not_null_8 ON PlannerRow (id)",
    ] {
        let error = session.execute_admin_sql_ddl(sql).unwrap_err();
        assert_eq!(
            error.diagnostic().error_code(),
            ErrorCode::SCHEMA_DDL_VALIDATION_FAILED
        );
        assert_eq!(projection_rows(&session, read), before);
    }
    // Expected version 1 proves each rejected authoring route left the head alone.
    session.execute_admin_sql_ddl(
        "ALTER TABLE PlannerRow ADD COLUMN score nat64 NOT NULL DEFAULT 7 EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2"
    ).unwrap();
    assert_eq!(
        projection_rows(&session, "SELECT score FROM PlannerRow ORDER BY id"),
        vec![vec![OutputValue::nat64(7)]; before.len()]
    );
}
