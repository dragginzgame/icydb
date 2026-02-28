use super::*;

const SECONDARY_INDEX_ORDER_ROWS: [(u128, u32, u32); 5] = [
    (8801, 7, 40),
    (8802, 7, 10),
    (8803, 7, 30),
    (8804, 7, 20),
    (8805, 8, 50),
];
const SECONDARY_SINGLE_STEP_STRICT_ROWS: [(u128, u32, u32); 5] = [
    (8831, 7, 10),
    (8832, 7, 20),
    (8833, 7, 30),
    (8834, 7, 40),
    (8835, 8, 50),
];
const SECONDARY_SINGLE_STEP_MISSING_OK_ROWS: [(u128, u32, u32); 5] = [
    (8841, 7, 10),
    (8842, 7, 20),
    (8843, 7, 30),
    (8844, 7, 40),
    (8845, 8, 50),
];
const SECONDARY_STALE_ID_ROWS: [(u128, u32, u32); 5] = [
    (8851, 7, 10),
    (8852, 7, 20),
    (8853, 7, 30),
    (8854, 7, 40),
    (8855, 8, 50),
];
const SECONDARY_STALE_FIELD_ROWS: [(u128, u32, u32); 5] = [
    (8_261, 7, 10),
    (8_262, 7, 20),
    (8_263, 7, 30),
    (8_264, 7, 40),
    (8_265, 8, 50),
];

fn assert_secondary_index_order_shape(descending: bool, explicit_pk_tie_break: bool, label: &str) {
    seed_pushdown_entities(&SECONDARY_INDEX_ORDER_ROWS);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let group_seven = u32_eq_predicate("group", 7);

    assert_aggregate_parity_for_query(
        &load,
        || {
            let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(group_seven.clone());
            let query = if descending {
                query.order_by_desc("rank")
            } else {
                query.order_by("rank")
            };
            let query = if explicit_pk_tie_break {
                query.order_by_desc("id")
            } else {
                query
            };

            query.offset(1).limit(2)
        },
        label,
    );
}

fn assert_secondary_id_extrema_single_step(
    rows: &[(u128, u32, u32)],
    consistency: MissingRowPolicy,
    expected_min: u128,
    expected_max: u128,
    label: &str,
) {
    seed_pushdown_entities(rows);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let (min_asc, scanned_min_asc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_min(secondary_group_rank_order_plan(
                consistency,
                crate::db::query::plan::OrderDirection::Asc,
                2,
            ))
            .expect("secondary single-step MIN ASC should succeed")
        });
    let (max_desc, scanned_max_desc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_max(secondary_group_rank_order_plan(
                consistency,
                crate::db::query::plan::OrderDirection::Desc,
                2,
            ))
            .expect("secondary single-step MAX DESC should succeed")
        });

    assert_eq!(
        min_asc.map(|id| id.key()),
        Some(Ulid::from_u128(expected_min))
    );
    assert_eq!(
        max_desc.map(|id| id.key()),
        Some(Ulid::from_u128(expected_max))
    );
    assert_eq!(
        scanned_min_asc, 3,
        "{label} MIN ASC should scan exactly offset + 1 keys"
    );
    assert_eq!(
        scanned_max_desc, 3,
        "{label} MAX DESC should scan exactly offset + 1 keys"
    );
}

fn seed_stale_secondary_rows(rows: &[(u128, u32, u32)], stale_ids: &[u128]) {
    seed_pushdown_entities(rows);
    for stale_id in stale_ids {
        remove_pushdown_row_data(*stale_id);
    }
}

fn assert_secondary_id_extrema_missing_ok_stale_fallback(
    rows: &[(u128, u32, u32)],
    stale_ids: &[u128],
) {
    seed_stale_secondary_rows(rows, stale_ids);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let expected_min_asc = load
        .execute(secondary_group_rank_order_plan(
            MissingRowPolicy::Ignore,
            crate::db::query::plan::OrderDirection::Asc,
            0,
        ))
        .expect("stale-leading MIN ASC baseline execute should succeed")
        .ids()
        .into_iter()
        .min();
    let expected_max_desc = load
        .execute(secondary_group_rank_order_plan(
            MissingRowPolicy::Ignore,
            crate::db::query::plan::OrderDirection::Desc,
            0,
        ))
        .expect("stale-leading MAX DESC baseline execute should succeed")
        .ids()
        .into_iter()
        .max();
    let (min_asc, scanned_min_asc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_min(secondary_group_rank_order_plan(
                MissingRowPolicy::Ignore,
                crate::db::query::plan::OrderDirection::Asc,
                0,
            ))
            .expect("stale-leading secondary MIN ASC should succeed")
        });
    let (max_desc, scanned_max_desc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_max(secondary_group_rank_order_plan(
                MissingRowPolicy::Ignore,
                crate::db::query::plan::OrderDirection::Desc,
                0,
            ))
            .expect("stale-leading secondary MAX DESC should succeed")
        });

    assert_eq!(
        min_asc, expected_min_asc,
        "stale-leading MIN ASC should preserve materialized parity"
    );
    assert_eq!(
        max_desc, expected_max_desc,
        "stale-leading MAX DESC should preserve materialized parity"
    );
    assert!(
        scanned_min_asc >= 2,
        "stale-leading MIN ASC should scan past bounded probe and retry unbounded"
    );
    assert!(
        scanned_max_desc >= 2,
        "stale-leading MAX DESC should scan past bounded probe and retry unbounded"
    );
}

fn assert_secondary_id_extrema_strict_stale_corruption(
    rows: &[(u128, u32, u32)],
    stale_ids: &[u128],
) {
    seed_stale_secondary_rows(rows, stale_ids);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let min_err = load
        .aggregate_min(secondary_group_rank_order_plan(
            MissingRowPolicy::Error,
            crate::db::query::plan::OrderDirection::Asc,
            0,
        ))
        .expect_err("strict secondary MIN should fail when leading key is stale");
    let max_err = load
        .aggregate_max(secondary_group_rank_order_plan(
            MissingRowPolicy::Error,
            crate::db::query::plan::OrderDirection::Desc,
            0,
        ))
        .expect_err("strict secondary MAX should fail when leading key is stale");

    assert_eq!(
        min_err.class,
        crate::error::ErrorClass::Corruption,
        "strict secondary MIN stale-leading key should classify as corruption"
    );
    assert_eq!(
        max_err.class,
        crate::error::ErrorClass::Corruption,
        "strict secondary MAX stale-leading key should classify as corruption"
    );
}

fn assert_secondary_field_extrema_missing_ok_stale_fallback(
    rows: &[(u128, u32, u32)],
    stale_ids: &[u128],
    target_field: &str,
) {
    seed_stale_secondary_rows(rows, stale_ids);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let expected_min_by = expected_min_by_rank_id(
        &load
            .execute(secondary_group_rank_order_plan(
                MissingRowPolicy::Ignore,
                crate::db::query::plan::OrderDirection::Asc,
                0,
            ))
            .expect("missing-ok field MIN baseline execute should succeed"),
    );
    let expected_max_by = expected_max_by_rank_id(
        &load
            .execute(secondary_group_rank_order_plan(
                MissingRowPolicy::Ignore,
                crate::db::query::plan::OrderDirection::Desc,
                0,
            ))
            .expect("missing-ok field MAX baseline execute should succeed"),
    );
    let (min_by, scanned_min_by) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_min_by(
                secondary_group_rank_order_plan(
                    MissingRowPolicy::Ignore,
                    crate::db::query::plan::OrderDirection::Asc,
                    0,
                ),
                target_field,
            )
            .expect("missing-ok field MIN should succeed")
        });
    let (max_by, scanned_max_by) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_max_by(
                secondary_group_rank_order_plan(
                    MissingRowPolicy::Ignore,
                    crate::db::query::plan::OrderDirection::Desc,
                    0,
                ),
                target_field,
            )
            .expect("missing-ok field MAX should succeed")
        });

    assert_eq!(
        min_by, expected_min_by,
        "missing-ok field MIN should preserve materialized parity under stale-leading keys"
    );
    assert_eq!(
        max_by, expected_max_by,
        "missing-ok field MAX should preserve materialized parity under stale-leading keys"
    );
    assert!(
        scanned_min_by >= 2,
        "missing-ok field MIN should scan past bounded probe and retry unbounded"
    );
    assert!(
        scanned_max_by >= 2,
        "missing-ok field MAX should scan past bounded probe and retry unbounded"
    );
}

fn assert_secondary_field_extrema_strict_stale_corruption(
    rows: &[(u128, u32, u32)],
    stale_ids: &[u128],
    target_field: &str,
) {
    seed_stale_secondary_rows(rows, stale_ids);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let min_err = load
        .aggregate_min_by(
            secondary_group_rank_order_plan(
                MissingRowPolicy::Error,
                crate::db::query::plan::OrderDirection::Asc,
                0,
            ),
            target_field,
        )
        .expect_err("strict field MIN should fail when leading key is stale");
    let max_err = load
        .aggregate_max_by(
            secondary_group_rank_order_plan(
                MissingRowPolicy::Error,
                crate::db::query::plan::OrderDirection::Desc,
                0,
            ),
            target_field,
        )
        .expect_err("strict field MAX should fail when leading key is stale");

    assert_eq!(
        min_err.class,
        crate::error::ErrorClass::Corruption,
        "strict field MIN stale-leading key should classify as corruption"
    );
    assert_eq!(
        max_err.class,
        crate::error::ErrorClass::Corruption,
        "strict field MAX stale-leading key should classify as corruption"
    );
}

#[test]
fn aggregate_parity_secondary_index_order_shape() {
    assert_secondary_index_order_shape(false, false, "secondary-index order shape");
}

#[test]
fn aggregate_parity_secondary_index_order_shape_desc_with_explicit_pk_tie_break() {
    assert_secondary_index_order_shape(
        true,
        true,
        "secondary-index order shape DESC with explicit PK tie-break",
    );
}

#[test]
fn aggregate_exists_secondary_index_window_preserves_missing_ok_scan_safety() {
    seed_pushdown_entities(&[
        (8811, 7, 10),
        (8812, 7, 20),
        (8813, 7, 30),
        (8814, 7, 40),
        (8815, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let group_seven = u32_eq_predicate("group", 7);

    let (exists, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_exists(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(group_seven.clone())
                .order_by("rank")
                .offset(2)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("secondary-index EXISTS window plan should build"),
        )
        .expect("secondary-index EXISTS window should succeed")
    });

    assert!(
        exists,
        "secondary-index EXISTS window should find a matching row"
    );
    assert_eq!(
        scanned, 5,
        "secondary-index EXISTS window should keep full prefix scan budget under Ignore safety"
    );
}

#[test]
fn aggregate_exists_secondary_index_strict_missing_surfaces_corruption_error() {
    seed_pushdown_entities(&[(8821, 7, 10), (8822, 7, 20), (8823, 7, 30)]);
    remove_pushdown_row_data(8821);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let mut logical_plan = crate::db::query::plan::AccessPlannedQuery::new(
        crate::db::access::AccessPath::IndexPrefix {
            index: PUSHDOWN_PARITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Error,
    );
    logical_plan.order = Some(crate::db::query::plan::OrderSpec {
        fields: vec![
            (
                "rank".to_string(),
                crate::db::query::plan::OrderDirection::Asc,
            ),
            (
                "id".to_string(),
                crate::db::query::plan::OrderDirection::Asc,
            ),
        ],
    });
    let strict_plan =
        crate::db::executor::ExecutablePlan::<PushdownParityEntity>::new(logical_plan);

    let err = load
        .aggregate_exists(strict_plan)
        .expect_err("strict secondary-index aggregate should fail when row is missing");

    assert_eq!(
        err.class,
        crate::error::ErrorClass::Corruption,
        "strict secondary-index aggregate missing row should classify as corruption"
    );
}

#[test]
fn aggregate_secondary_index_extrema_strict_single_step_scans_offset_plus_one() {
    assert_secondary_id_extrema_single_step(
        &SECONDARY_SINGLE_STEP_STRICT_ROWS,
        MissingRowPolicy::Error,
        8833,
        8832,
        "strict secondary",
    );
}

#[test]
fn aggregate_secondary_index_extrema_missing_ok_clean_single_step_scans_offset_plus_one() {
    assert_secondary_id_extrema_single_step(
        &SECONDARY_SINGLE_STEP_MISSING_OK_ROWS,
        MissingRowPolicy::Ignore,
        8843,
        8842,
        "missing-ok secondary",
    );
}

#[test]
fn aggregate_field_extrema_secondary_index_eligible_shape_locks_scan_budget() {
    seed_pushdown_entities(&[
        (8_281, 7, 10),
        (8_282, 7, 20),
        (8_283, 7, 30),
        (8_284, 7, 40),
        (8_285, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let (min_by_asc, scanned_min_by_asc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_min_by(
                secondary_group_rank_order_plan(
                    MissingRowPolicy::Ignore,
                    crate::db::query::plan::OrderDirection::Asc,
                    0,
                ),
                "rank",
            )
            .expect("missing-ok secondary MIN(field) eligible shape should succeed")
        });
    let (max_by_desc, scanned_max_by_desc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_max_by(
                secondary_group_rank_order_plan(
                    MissingRowPolicy::Ignore,
                    crate::db::query::plan::OrderDirection::Desc,
                    0,
                ),
                "rank",
            )
            .expect("missing-ok secondary MAX(field) eligible shape should succeed")
        });

    assert_eq!(
        min_by_asc.map(|id| id.key()),
        Some(Ulid::from_u128(8_281)),
        "missing-ok secondary MIN(field) eligible shape should return the first ordered candidate"
    );
    assert_eq!(
        max_by_desc.map(|id| id.key()),
        Some(Ulid::from_u128(8_284)),
        "missing-ok secondary MAX(field) eligible shape should return the first ordered DESC candidate"
    );
    assert_eq!(
        scanned_min_by_asc, 4,
        "missing-ok secondary MIN(field) eligible shape should scan the full group window under current contract"
    );
    assert_eq!(
        scanned_max_by_desc, 4,
        "missing-ok secondary MAX(field) eligible shape should scan the full group window under current contract"
    );
}

#[test]
fn aggregate_secondary_index_extrema_missing_ok_stale_leading_probe_falls_back() {
    assert_secondary_id_extrema_missing_ok_stale_fallback(&SECONDARY_STALE_ID_ROWS, &[8851, 8854]);
}

#[test]
fn aggregate_secondary_index_extrema_strict_stale_leading_surfaces_corruption_error() {
    assert_secondary_id_extrema_strict_stale_corruption(&SECONDARY_STALE_ID_ROWS, &[8851, 8854]);
}

#[test]
fn aggregate_field_extrema_missing_ok_stale_leading_probe_falls_back() {
    assert_secondary_field_extrema_missing_ok_stale_fallback(
        &SECONDARY_STALE_FIELD_ROWS,
        &[8_261, 8_264],
        "rank",
    );
}

#[test]
fn aggregate_field_extrema_strict_stale_leading_surfaces_corruption_error() {
    assert_secondary_field_extrema_strict_stale_corruption(
        &SECONDARY_STALE_FIELD_ROWS,
        &[8_261, 8_264],
        "rank",
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn aggregate_field_terminal_error_classification_matrix() {
    seed_pushdown_entities(&[(8_291, 7, 10), (8_292, 7, 20), (8_293, 7, 30)]);
    let pushdown_load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let unknown_field_min_error = pushdown_load
        .aggregate_min_by(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("unknown-field MIN(field) plan should build"),
            "missing_field",
        )
        .expect_err("unknown field MIN(field) should fail");
    let unknown_field_median_error = pushdown_load
        .aggregate_median_by(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("unknown-field MEDIAN(field) plan should build"),
            "missing_field",
        )
        .expect_err("unknown field MEDIAN(field) should fail");
    let unknown_field_count_distinct_error = pushdown_load
        .aggregate_count_distinct_by(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("unknown-field COUNT_DISTINCT(field) plan should build"),
            "missing_field",
        )
        .expect_err("unknown field COUNT_DISTINCT(field) should fail");
    let unknown_field_min_max_error = pushdown_load
        .aggregate_min_max_by(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("unknown-field MIN_MAX(field) plan should build"),
            "missing_field",
        )
        .expect_err("unknown field MIN_MAX(field) should fail");
    let non_numeric_error = pushdown_load
        .aggregate_sum_by(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("non-numeric SUM(field) plan should build"),
            "label",
        )
        .expect_err("non-numeric SUM(field) should fail");
    remove_pushdown_row_data(8_291);
    let strict_stale_error = pushdown_load
        .aggregate_min_by(
            secondary_group_rank_order_plan(
                MissingRowPolicy::Error,
                crate::db::query::plan::OrderDirection::Asc,
                0,
            ),
            "rank",
        )
        .expect_err("strict stale-leading MIN(field) should fail");
    let strict_stale_median_error = pushdown_load
        .aggregate_median_by(
            secondary_group_rank_order_plan(
                MissingRowPolicy::Error,
                crate::db::query::plan::OrderDirection::Asc,
                0,
            ),
            "rank",
        )
        .expect_err("strict stale-leading MEDIAN(field) should fail");
    let strict_stale_count_distinct_error = pushdown_load
        .aggregate_count_distinct_by(
            secondary_group_rank_order_plan(
                MissingRowPolicy::Error,
                crate::db::query::plan::OrderDirection::Asc,
                0,
            ),
            "rank",
        )
        .expect_err("strict stale-leading COUNT_DISTINCT(field) should fail");
    let strict_stale_min_max_error = pushdown_load
        .aggregate_min_max_by(
            secondary_group_rank_order_plan(
                MissingRowPolicy::Error,
                crate::db::query::plan::OrderDirection::Asc,
                0,
            ),
            "rank",
        )
        .expect_err("strict stale-leading MIN_MAX(field) should fail");

    seed_phase_entities(&[(8_294, 10), (8_295, 20)]);
    let phase_load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let non_orderable_min_error = phase_load
        .aggregate_min_by(
            Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("non-orderable MIN(field) plan should build"),
            "tags",
        )
        .expect_err("non-orderable MIN(field) should fail");
    let non_orderable_median_error = phase_load
        .aggregate_median_by(
            Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("non-orderable MEDIAN(field) plan should build"),
            "tags",
        )
        .expect_err("non-orderable MEDIAN(field) should fail");
    let non_orderable_min_max_error = phase_load
        .aggregate_min_max_by(
            Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("non-orderable MIN_MAX(field) plan should build"),
            "tags",
        )
        .expect_err("non-orderable MIN_MAX(field) should fail");

    assert_eq!(
        unknown_field_min_error.class,
        ErrorClass::Unsupported,
        "unknown field MIN(field) should classify as Unsupported"
    );
    assert_eq!(
        unknown_field_median_error.class,
        ErrorClass::Unsupported,
        "unknown field MEDIAN(field) should classify as Unsupported"
    );
    assert_eq!(
        unknown_field_count_distinct_error.class,
        ErrorClass::Unsupported,
        "unknown field COUNT_DISTINCT(field) should classify as Unsupported"
    );
    assert_eq!(
        unknown_field_min_max_error.class,
        ErrorClass::Unsupported,
        "unknown field MIN_MAX(field) should classify as Unsupported"
    );
    assert_eq!(
        non_orderable_min_error.class,
        ErrorClass::Unsupported,
        "non-orderable MIN(field) should classify as Unsupported"
    );
    assert_eq!(
        non_orderable_median_error.class,
        ErrorClass::Unsupported,
        "non-orderable MEDIAN(field) should classify as Unsupported"
    );
    assert_eq!(
        non_orderable_min_max_error.class,
        ErrorClass::Unsupported,
        "non-orderable MIN_MAX(field) should classify as Unsupported"
    );
    assert_eq!(
        non_numeric_error.class,
        ErrorClass::Unsupported,
        "non-numeric SUM(field) should classify as Unsupported"
    );
    assert_eq!(
        strict_stale_error.class,
        ErrorClass::Corruption,
        "strict stale-leading MIN(field) should classify as Corruption"
    );
    assert_eq!(
        strict_stale_median_error.class,
        ErrorClass::Corruption,
        "strict stale-leading MEDIAN(field) should classify as Corruption"
    );
    assert_eq!(
        strict_stale_count_distinct_error.class,
        ErrorClass::Corruption,
        "strict stale-leading COUNT_DISTINCT(field) should classify as Corruption"
    );
    assert_eq!(
        strict_stale_min_max_error.class,
        ErrorClass::Corruption,
        "strict stale-leading MIN_MAX(field) should classify as Corruption"
    );
}

#[test]
fn aggregate_field_extrema_negative_lock_distinct_and_offset_shapes_avoid_single_step_probe() {
    seed_pushdown_entities(&[
        (8_301, 7, 10),
        (8_302, 7, 20),
        (8_303, 7, 30),
        (8_304, 7, 40),
        (8_305, 7, 50),
        (8_306, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let (distinct_min, scanned_distinct_min) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_min_by(
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .filter(u32_eq_predicate("group", 7))
                    .distinct()
                    .order_by("rank")
                    .plan()
                    .map(crate::db::executor::ExecutablePlan::from)
                    .expect("distinct MIN(field) plan should build"),
                "rank",
            )
            .expect("distinct MIN(field) should succeed")
        });
    let (offset_max, scanned_offset_max) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_max_by(
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .filter(u32_eq_predicate("group", 7))
                    .order_by("rank")
                    .offset(2)
                    .plan()
                    .map(crate::db::executor::ExecutablePlan::from)
                    .expect("offset MAX(field) plan should build"),
                "rank",
            )
            .expect("offset MAX(field) should succeed")
        });

    assert_eq!(
        distinct_min.map(|id| id.key()),
        Some(Ulid::from_u128(8_301)),
        "distinct MIN(field) should preserve canonical parity"
    );
    assert_eq!(
        offset_max.map(|id| id.key()),
        Some(Ulid::from_u128(8_305)),
        "offset MAX(field) should preserve canonical parity"
    );
    assert!(
        scanned_distinct_min >= 2,
        "distinct MIN(field) should not collapse to single-step probe"
    );
    assert!(
        scanned_offset_max >= 3,
        "offset MAX(field) should remain bounded by window traversal, not single-step probe"
    );
}
