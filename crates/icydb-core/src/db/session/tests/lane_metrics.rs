use super::*;

// Seed one deterministic filtered-order fixture that matches the warmed
// `active = true ORDER BY age, id` fluent load family.
fn seed_filtered_order_age_fixture(session: &DbSession<SessionSqlCanister>) {
    seed_filtered_indexed_session_sql_entities(
        session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "bristle", true, 30),
            (9_204, "broom", false, 40),
            (9_205, "charlie", true, 50),
        ],
    );
}

// Seed one deterministic field-bound range fixture whose `BETWEEN` predicate
// stays residual and therefore must use the direct filtered data-row lane.
fn seed_field_bound_lane_fixture(session: &DbSession<SessionSqlCanister>) {
    for (id, label, score, min_score, max_score) in [
        (9_301, "field-bound-a", 15_u64, 10_u64, 20_u64),
        (9_302, "field-bound-b", 10_u64, 10_u64, 20_u64),
        (9_303, "field-bound-c", 20_u64, 10_u64, 20_u64),
        (9_304, "field-bound-d", 9_u64, 10_u64, 20_u64),
        (9_305, "field-bound-e", 21_u64, 10_u64, 20_u64),
    ] {
        session
            .insert(SessionSqlFieldBoundRangeEntity {
                id: Ulid::from_u128(id),
                label: label.to_string(),
                score,
                min_score,
                max_score,
            })
            .expect("field-bound lane fixture insert should succeed");
    }
}

#[test]
fn fluent_lane_metrics_mark_filtered_order_age_loads_direct_filtered_data_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the same filtered ordered family used by the warmed
    // `active = true ORDER BY age, id` fluent load slice.
    seed_filtered_order_age_fixture(&session);

    // Phase 2: execute the fluent load under lane metrics so the executor
    // contract proves whether this family stays on the direct data-row lane.
    let (rows, metrics) = crate::db::with_scalar_materialization_lane_metrics(|| {
        session
            .load::<FilteredIndexedSessionSqlEntity>()
            .filter(crate::db::FieldRef::new("active").eq(true))
            .order_by("age")
            .order_by("id")
            .limit(3)
            .execute()
            .and_then(crate::db::LoadQueryResult::into_rows)
    });
    let rows = rows.expect("filtered age-ordered fluent load should execute");
    let ids = rows.iter().map(|row| row.id().key()).collect::<Vec<_>>();

    assert_eq!(
        ids,
        vec![
            Ulid::from_u128(9_202),
            Ulid::from_u128(9_203),
            Ulid::from_u128(9_205),
        ],
        "filtered age-ordered fluent loads should preserve the guarded active=true ordered window",
    );
    assert_eq!(
        metrics.direct_data_row_path_hits, 0,
        "filtered age-ordered fluent loads should not claim the residual-free direct lane",
    );
    assert_eq!(
        metrics.direct_filtered_data_row_path_hits, 1,
        "filtered age-ordered fluent loads should stay on the direct filtered data-row lane",
    );
    assert_eq!(
        metrics.kernel_data_row_path_hits, 0,
        "filtered age-ordered fluent loads should not fall back to kernel data-row envelopes",
    );
    assert_eq!(
        metrics.kernel_full_row_retained_path_hits, 0,
        "filtered age-ordered fluent loads should not allocate retained full-row envelopes",
    );
    assert_eq!(
        metrics.kernel_slots_only_path_hits, 0,
        "filtered age-ordered fluent loads should not allocate slot-only envelopes",
    );
}

#[test]
fn fluent_lane_metrics_mark_field_bound_between_loads_direct_filtered_data_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic field-bound range fixture whose `BETWEEN`
    // predicate stays residual after planning.
    seed_field_bound_lane_fixture(&session);

    // Phase 2: execute the fluent field-bound range load under lane metrics so
    // the executor contract proves whether it stays on the direct filtered lane.
    let (rows, metrics) = crate::db::with_scalar_materialization_lane_metrics(|| {
        session
            .load::<SessionSqlFieldBoundRangeEntity>()
            .filter(crate::db::FieldRef::new("score").between_fields("min_score", "max_score"))
            .order_by("id")
            .limit(3)
            .execute()
            .and_then(crate::db::LoadQueryResult::into_rows)
    });
    let rows = rows.expect("field-bound BETWEEN fluent load should execute");
    let labels = rows
        .iter()
        .map(|row| row.entity_ref().label.clone())
        .collect::<Vec<_>>();

    assert_eq!(
        labels,
        vec![
            "field-bound-a".to_string(),
            "field-bound-b".to_string(),
            "field-bound-c".to_string(),
        ],
        "field-bound BETWEEN fluent loads should preserve the expected bounded row set",
    );
    assert_eq!(
        metrics.direct_data_row_path_hits, 0,
        "field-bound BETWEEN fluent loads should not use the residual-free direct lane",
    );
    assert_eq!(
        metrics.direct_filtered_data_row_path_hits, 1,
        "field-bound BETWEEN fluent loads should stay on the direct filtered data-row lane",
    );
    assert_eq!(
        metrics.kernel_data_row_path_hits, 0,
        "field-bound BETWEEN fluent loads should not fall back to kernel data-row envelopes",
    );
    assert_eq!(
        metrics.kernel_full_row_retained_path_hits, 0,
        "field-bound BETWEEN fluent loads should not allocate retained full-row envelopes",
    );
    assert_eq!(
        metrics.kernel_slots_only_path_hits, 0,
        "field-bound BETWEEN fluent loads should not allocate slot-only envelopes",
    );
}
