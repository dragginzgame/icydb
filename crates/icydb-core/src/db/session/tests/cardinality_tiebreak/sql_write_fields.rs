//! Authored SQL field names are checked against the accepted catalog before effects.

use super::*;
use crate::db::{MutationJobError, MutationJobId, QueryError, schema::ValidateError};

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
