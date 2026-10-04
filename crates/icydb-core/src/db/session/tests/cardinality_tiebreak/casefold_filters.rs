//! Casefold filters share truth across optimized, expression and durable scopes.

use super::{
    DynamicQuery, ENTITY_NAME, FieldRef, OutputValue, SqlStatementResult, asc, initialize,
    initialize_journaled, insert_row, projection_rows,
};
use crate::db::{
    MutationJobAdvanceRequest, MutationJobId, MutationJobIdempotencyKey, MutationJobStatus,
};

#[test]
fn casefold_filters_reads_counts_and_expression_controls_agree() {
    let session = initialize();
    for (id, name) in [
        (1, "Alice"),
        (2, "aLan"),
        (3, "bob"),
        (4, "İSTANBUL"),
        (5, "ΟΣ"),
        (6, ""),
    ] {
        insert_row(&session, id, name, "group-a");
    }
    for capacity in [0, 4 * 1024 * 1024] {
        session.clear_shared_query_cache_for_tests(capacity);
        for (predicate, ids) in [
            ("common ILIKE 'Al%'", vec![1, 2]),
            ("common NOT ILIKE 'Al%'", vec![3, 4, 5, 6]),
            ("LOWER(common) = 'ALICE'", vec![1]),
            ("'ALICE' = LOWER(common)", vec![1]),
            ("LOWER(common) <> 'ALICE'", vec![2, 3, 4, 5, 6]),
            ("LOWER(common) IN ('ALICE', 'BOB')", vec![1, 3]),
            ("LOWER(common) NOT IN ('ALICE', 'BOB')", vec![2, 4, 5, 6]),
            ("LOWER(common) > 'BOB'", vec![4, 5]),
            ("'BOB' < LOWER(common)", vec![4, 5]),
            ("LOWER(common) LIKE 'AL%'", vec![1, 2]),
            ("STARTS_WITH(LOWER(common), 'AL')", vec![1, 2]),
            ("ENDS_WITH(LOWER(common), 'ICE')", vec![1]),
            ("CONTAINS(LOWER(common), 'LI')", vec![1]),
            ("LOWER(common) = 'ΟΣ'", vec![5]),
            ("common ILIKE 'İS%'", vec![4]),
            ("common LIKE 'Al%'", vec![1]),
        ] {
            let expected = ids
                .iter()
                .map(|id| vec![OutputValue::nat64(*id)])
                .collect::<Vec<_>>();
            for _ in 0..2 {
                for condition in [
                    predicate.to_string(),
                    format!("({predicate}) OR id + 0 = 999"),
                ] {
                    assert_eq!(
                        projection_rows(
                            &session,
                            &format!("SELECT id FROM PlannerRow WHERE {condition} ORDER BY id")
                        ),
                        expected,
                        "{condition}",
                    );
                }
                let count = format!("SELECT COUNT(*) FROM PlannerRow WHERE {predicate}");
                let SqlStatementResult::Projection { rows, .. } = session
                    .execute_trusted_sql_query(&count)
                    .unwrap_or_else(|error| panic!("{count}: {error:?}"))
                else {
                    panic!("count should project");
                };
                assert_eq!(
                    rows,
                    vec![vec![OutputValue::nat64(ids.len() as u64)]],
                    "COUNT: {predicate}",
                );
            }
        }
    }
}

#[test]
fn casefold_filters_scalar_lower_and_ilike_conditions_preserve_semantics() {
    let session = initialize();
    for (id, name) in [(1, "Alice"), (2, "aLan"), (3, "bob")] {
        insert_row(&session, id, name, "group-a");
    }
    for sql in [
        "SELECT common, COUNT(*) FROM PlannerRow WHERE LOWER(common) = 'ALICE' GROUP BY common LIMIT 10",
        "SELECT common, COUNT(*) FROM PlannerRow GROUP BY common HAVING LOWER(common) = 'ALICE' LIMIT 10",
    ] {
        let SqlStatementResult::Grouped { rows, .. } = session
            .execute_trusted_sql_query(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        else {
            panic!("grouped filter should return grouped rows");
        };
        assert_eq!(rows.len(), 1, "{sql}");
        assert_eq!(rows[0].group_key(), &[OutputValue::text("Alice".into())]);
        assert_eq!(rows[0].aggregate_values(), &[OutputValue::nat64(1)]);
    }
    for predicate in [
        "TRIM(common) ILIKE 'Al%'",
        "STARTS_WITH(LOWER(TRIM(common)), 'AL')",
    ] {
        assert_eq!(
            projection_rows(
                &session,
                &format!("SELECT id FROM PlannerRow WHERE {predicate} ORDER BY id")
            ),
            vec![vec![OutputValue::nat64(1)], vec![OutputValue::nat64(2)]],
        );
    }
    assert_eq!(
        projection_rows(
            &session,
            "SELECT id FROM PlannerRow WHERE UPPER(common) = 'ALICE'"
        ),
        vec![vec![OutputValue::nat64(1)]],
    );
    // LOWER retains its scalar transform; a scalar CASE condition still gives
    // explicit ILIKE its casefold meaning outside the WHERE context.
    let sql = "SELECT LOWER(common), CASE WHEN common ILIKE 'Al%' THEN 1 ELSE 0 END AS folded FROM PlannerRow WHERE id = 1";
    let SqlStatementResult::Projection { rows, .. } = session
        .execute_trusted_sql_query(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
    else {
        panic!("scalar should project");
    };
    assert_eq!(
        rows,
        vec![vec![
            OutputValue::text("alice".into()),
            OutputValue::int64(1)
        ]],
    );
}

#[test]
fn casefold_filters_public_fluent_pages_agree_with_trusted_reads() {
    let session = initialize();
    for (id, name) in [(1, "Alice"), (2, "aLan"), (3, "bob")] {
        insert_row(&session, id, name, "group-a");
    }
    let query = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("common").text_eq_ci("ALICE"))
        .select(["id"])
        .order_by(asc("id"))
        .limit(10);
    for capacity in [0, 4 * 1024 * 1024] {
        session.clear_shared_query_cache_for_tests(capacity);
        for _ in 0..2 {
            let public = session.execute_public_live_page(&query, None).unwrap();
            let trusted = session.execute_trusted_live_page(&query, None).unwrap();
            assert_eq!(public.rows, trusted.rows);
            assert_eq!(public.rows, vec![vec![OutputValue::nat64(1)]]);
            assert!(public.continuation.is_none());
            assert!(trusted.continuation.is_none());
        }
    }
    // A secondary expression range does not supply primary-key ordering;
    // the public sort-admission boundary must still reject that request.
    let prefix = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("common").text_starts_with_ci("AL"))
        .select(["id"])
        .order_by(asc("id"))
        .limit(10);
    assert_eq!(
        session
            .execute_public_live_page(&prefix, None)
            .unwrap_err()
            .diagnostic()
            .error_code(),
        icydb_diagnostic_code::ErrorCode::QUERY_READ_SORT_REQUIRES_MATERIALIZATION,
    );
}

#[test]
fn casefold_filters_preserve_null_unknown_across_both_read_routes() {
    let session = super::sql_not_null::initialize_nullable_rows();
    for (predicate, ids) in [
        ("status ILIKE 'A%'", vec![1, 2]),
        ("status NOT ILIKE 'A%'", vec![4, 5]),
        ("LOWER(status) = 'ARCHIVED'", vec![1]),
        ("LOWER(status) <> 'ARCHIVED'", vec![2, 4, 5]),
        ("LOWER(status) IN ('ARCHIVED', NULL)", vec![1]),
        ("LOWER(status) NOT IN ('ARCHIVED', NULL)", vec![]),
        ("STARTS_WITH(LOWER(status), 'A')", vec![1, 2]),
    ] {
        let expected = ids
            .iter()
            .map(|id| vec![OutputValue::nat64(*id)])
            .collect::<Vec<_>>();
        for condition in [
            predicate.to_string(),
            format!("({predicate}) OR id + 0 = 999"),
        ] {
            assert_eq!(
                projection_rows(
                    &session,
                    &format!("SELECT id FROM PlannerRow WHERE {condition} ORDER BY id")
                ),
                expected,
                "{condition}",
            );
        }
    }
}

#[test]
fn casefold_filters_resumable_update_matches_exact_and_replays_receipts() {
    let session = initialize_journaled();
    for (id, name) in [(1, "Alice"), (2, "aLan"), (3, "bob")] {
        insert_row(&session, id, name, "group-a");
    }
    for (identity, predicate, ids) in [
        (81_u8, "common ILIKE 'Al%'", vec![1, 2]),
        (82, "LOWER(common) = 'ALICE'", vec![1]),
        (83, "STARTS_WITH(LOWER(common), 'AL')", vec![1, 2]),
        (84, "LOWER(common) IN ('ALICE', 'BOB')", vec![1, 3]),
        (85, "common NOT ILIKE 'Al%'", vec![3]),
    ] {
        let sql = format!("UPDATE PlannerRow SET rare = 'marked' WHERE {predicate}");
        let expected = ids
            .iter()
            .map(|id| vec![OutputValue::nat64(*id)])
            .collect::<Vec<_>>();
        let exact = session.execute_trusted_sql_exact_update(&sql, 3).unwrap();
        assert!(
            matches!(exact, SqlStatementResult::Count { row_count } if row_count == u32::try_from(ids.len()).unwrap())
        );
        assert_eq!(
            projection_rows(
                &session,
                "SELECT id FROM PlannerRow WHERE rare = 'marked' ORDER BY id"
            ),
            expected
        );
        session
            .execute_trusted_sql_exact_update(
                "UPDATE PlannerRow SET rare = 'group-a' WHERE id > 0",
                3,
            )
            .unwrap();

        let job_id = MutationJobId::try_from_bytes([identity; 32]).unwrap();
        let mut state = session
            .start_trusted_sql_mutation_job(job_id, &sql)
            .unwrap();
        for _ in 0..8 {
            let request = MutationJobAdvanceRequest::new(
                job_id,
                state.sequence,
                MutationJobIdempotencyKey::new(format!("casefold-{identity}-{}", state.sequence))
                    .unwrap(),
            );
            let receipt = session.advance_trusted_mutation_job(&request).unwrap();
            assert_eq!(
                session.advance_trusted_mutation_job(&request).unwrap(),
                receipt
            );
            state = session.mutation_job_state(job_id).unwrap();
            if state.status == MutationJobStatus::Completed {
                break;
            }
        }
        assert_eq!(state.status, MutationJobStatus::Completed, "{predicate}");
        assert_eq!(state.rows_updated_total, ids.len() as u64);
        assert_eq!(
            projection_rows(
                &session,
                "SELECT id FROM PlannerRow WHERE rare = 'marked' ORDER BY id"
            ),
            expected,
            "resumable: {predicate}"
        );
        session
            .acknowledge_mutation_job(job_id, state.sequence)
            .unwrap();
        session
            .execute_trusted_sql_exact_update(
                "UPDATE PlannerRow SET rare = 'group-a' WHERE id > 0",
                3,
            )
            .unwrap();
    }
}
