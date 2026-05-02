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

// Seed one deterministic filtered composite-order fixture that matches the
// route-ordered `active = true AND tier = 'gold' ORDER BY handle, id` family.
fn seed_filtered_handle_route_fixture(session: &DbSession<SessionSqlCanister>) {
    seed_filtered_composite_indexed_session_sql_entities(
        session,
        &[
            (9_401, "amber", true, "gold", "amber-handle", 10),
            (9_402, "bravo", true, "gold", "bravo-handle", 20),
            (9_403, "charlie", false, "gold", "charlie-handle", 30),
            (9_404, "delta", true, "silver", "delta-handle", 40),
            (9_405, "echo", true, "gold", "echo-handle", 50),
        ],
    );
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
            .order_term(crate::db::asc("age"))
            .order_term(crate::db::asc("id"))
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
            .order_term(crate::db::asc("id"))
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

#[test]
fn fluent_lane_metrics_mark_filtered_handle_route_loads_direct_data_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the filtered composite route family that mirrors the
    // account-shaped active-plus-tier ordered handle window.
    seed_filtered_handle_route_fixture(&session);

    // Phase 2: execute the fluent load under lane metrics so the executor
    // contract proves whether a route-ordered filtered handle family already
    // stays on the residual-free direct data-row lane.
    let (rows, metrics) = crate::db::with_scalar_materialization_lane_metrics(|| {
        session
            .load::<FilteredIndexedSessionSqlEntity>()
            .filter(crate::db::FieldRef::new("active").eq(true))
            .filter(crate::db::FieldRef::new("tier").eq("gold"))
            .order_term(crate::db::asc("handle"))
            .order_term(crate::db::asc("id"))
            .limit(2)
            .execute()
            .and_then(crate::db::LoadQueryResult::into_rows)
    });
    let rows = rows.expect("filtered handle route fluent load should execute");
    let handles = rows
        .iter()
        .map(|row| row.entity_ref().handle.clone())
        .collect::<Vec<_>>();

    assert_eq!(
        handles,
        vec!["amber-handle".to_string(), "bravo-handle".to_string()],
        "filtered handle route loads should preserve the active gold ordered handle window",
    );
    assert_eq!(
        metrics.direct_data_row_path_hits, 1,
        "route-ordered filtered handle loads should stay on the direct data-row lane",
    );
    assert_eq!(
        metrics.direct_filtered_data_row_path_hits, 0,
        "route-ordered filtered handle loads should not require residual scan-time filtering",
    );
    assert_eq!(
        metrics.kernel_data_row_path_hits, 0,
        "route-ordered filtered handle loads should not fall back to kernel data-row envelopes",
    );
    assert_eq!(
        metrics.kernel_full_row_retained_path_hits, 0,
        "route-ordered filtered handle loads should not allocate retained full-row envelopes",
    );
    assert_eq!(
        metrics.kernel_slots_only_path_hits, 0,
        "route-ordered filtered handle loads should not allocate slot-only envelopes",
    );
}

#[test]
fn fluent_route_ordered_direct_data_row_loads_cap_rows_scanned_to_offset_plus_limit() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the same route-ordered filtered handle family used by the
    // direct raw-row account perf slice.
    seed_filtered_handle_route_fixture(&session);

    // Phase 2: capture executor scan metrics for one cursorless ordered page.
    // This direct lane now owns the same `offset + limit` early-stop contract
    // the retained-slot collector already used, so route-ordered loads should
    // stop after reading the final needed candidate while returning only the
    // post-offset page.
    let (rows, rows_scanned) =
        capture_rows_scanned_for_entity(FilteredIndexedSessionSqlEntity::PATH, || {
            session
                .load::<FilteredIndexedSessionSqlEntity>()
                .filter(crate::db::FieldRef::new("active").eq(true))
                .filter(crate::db::FieldRef::new("tier").eq("gold"))
                .order_term(crate::db::asc("handle"))
                .order_term(crate::db::asc("id"))
                .offset(1)
                .limit(2)
                .execute()
                .and_then(crate::db::LoadQueryResult::into_rows)
        });
    let rows = rows.expect("route-ordered filtered handle load should execute");
    let handles = rows
        .iter()
        .map(|row| row.entity_ref().handle.clone())
        .collect::<Vec<_>>();

    assert_eq!(
        handles,
        vec!["bravo-handle".to_string(), "echo-handle".to_string()],
        "route-ordered filtered handle load should still return the post-offset bounded page",
    );
    assert_eq!(
        rows_scanned, 3,
        "route-ordered direct data-row loads should stop scanning at offset+limit",
    );
}
