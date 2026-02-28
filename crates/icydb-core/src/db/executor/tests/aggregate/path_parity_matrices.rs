use super::*;

const DISTINCT_ASC_ROWS: [u128; 6] = [8301, 8302, 8303, 8304, 8305, 8306];
const DISTINCT_DESC_ROWS: [u128; 6] = [8401, 8402, 8403, 8404, 8405, 8406];
const FIELD_DISTINCT_ASC_ROWS: [(u128, u32, u32); 6] = [
    (8_201, 7, 40),
    (8_202, 7, 10),
    (8_203, 7, 20),
    (8_204, 7, 20),
    (8_205, 7, 30),
    (8_206, 8, 99),
];
const FIELD_DISTINCT_DESC_ROWS: [(u128, u32, u32); 6] = [
    (8_211, 7, 40),
    (8_212, 7, 10),
    (8_213, 7, 20),
    (8_214, 7, 20),
    (8_215, 7, 30),
    (8_216, 8, 99),
];

#[derive(Clone, Copy)]
enum CompositeTerminal {
    Count,
    Exists,
}

#[derive(Debug, PartialEq)]
enum CompositeTerminalResult {
    Count(u32),
    Exists(bool),
}

fn run_composite_terminal(
    load: &LoadExecutor<PhaseEntity>,
    plan: ExecutablePlan<PhaseEntity>,
    terminal: CompositeTerminal,
) -> Result<CompositeTerminalResult, InternalError> {
    match terminal {
        CompositeTerminal::Count => load
            .aggregate_count(plan)
            .map(CompositeTerminalResult::Count),
        CompositeTerminal::Exists => load
            .aggregate_exists(plan)
            .map(CompositeTerminalResult::Exists),
    }
}

fn build_phase_composite_plan(
    order_field: &str,
    first: Vec<Ulid>,
    second: Vec<Ulid>,
) -> ExecutablePlan<PhaseEntity> {
    let access = crate::db::access::AccessPlan::Union(vec![
        crate::db::access::AccessPlan::path(crate::db::access::AccessPath::ByKeys(first)),
        crate::db::access::AccessPlan::path(crate::db::access::AccessPath::ByKeys(second)),
    ]);
    let mut logical_plan = crate::db::query::plan::AccessPlannedQuery::new(
        crate::db::access::AccessPath::FullScan,
        MissingRowPolicy::Ignore,
    );
    logical_plan.access = access;
    logical_plan.order = Some(crate::db::query::plan::OrderSpec {
        fields: vec![(
            order_field.to_string(),
            crate::db::query::plan::OrderDirection::Asc,
        )],
    });

    crate::db::executor::ExecutablePlan::<PhaseEntity>::new(logical_plan)
}

fn phase_rows_with_base(base: u128) -> [(u128, u32); 6] {
    [
        (base, 10),
        (base.saturating_add(1), 20),
        (base.saturating_add(2), 30),
        (base.saturating_add(3), 40),
        (base.saturating_add(4), 50),
        (base.saturating_add(5), 60),
    ]
}

fn composite_key_sets_with_base(base: u128) -> (Vec<Ulid>, Vec<Ulid>) {
    let first = [0u128, 1, 2, 3]
        .into_iter()
        .map(|offset| Ulid::from_u128(base.saturating_add(offset)))
        .collect();
    let second = [2u128, 3, 4, 5]
        .into_iter()
        .map(|offset| Ulid::from_u128(base.saturating_add(offset)))
        .collect();

    (first, second)
}

fn assert_composite_terminal_direct_path_scan_does_not_exceed_fallback(
    rows: &[(u128, u32)],
    first: Vec<Ulid>,
    second: Vec<Ulid>,
    terminal: CompositeTerminal,
    label: &str,
) {
    seed_phase_entities(rows);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let direct_plan = build_phase_composite_plan("id", first.clone(), second.clone());
    assert!(
        ExecutionKernel::is_streaming_access_shape_safe::<PhaseEntity, _>(direct_plan.as_inner()),
        "direct composite {label} shape should be streaming-safe"
    );
    assert!(
        matches!(
            direct_plan.explain().access,
            ExplainAccessPath::Union(_) | ExplainAccessPath::Intersection(_)
        ),
        "direct {label} shape should compile to a composite access path"
    );

    let fallback_plan = build_phase_composite_plan("label", first, second);
    assert!(
        !ExecutionKernel::is_streaming_access_shape_safe::<PhaseEntity, _>(
            fallback_plan.as_inner()
        ),
        "fallback composite {label} shape should be streaming-unsafe"
    );
    assert!(
        matches!(
            fallback_plan.explain().access,
            ExplainAccessPath::Union(_) | ExplainAccessPath::Intersection(_)
        ),
        "fallback {label} shape should still compile to a composite access path"
    );

    let (direct_result, direct_scanned) =
        capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
            run_composite_terminal(&load, direct_plan, terminal)
                .expect("direct composite terminal should succeed")
        });
    let (fallback_result, fallback_scanned) =
        capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
            run_composite_terminal(&load, fallback_plan, terminal)
                .expect("fallback composite terminal should succeed")
        });

    assert_eq!(
        direct_result, fallback_result,
        "composite direct/fallback {label} should preserve parity"
    );
    assert!(
        direct_scanned <= fallback_scanned,
        "composite direct {label} should not scan more rows than fallback for equivalent composite filter"
    );
}

fn assert_distinct_parity_for_simple_rows(rows: &[u128], descending: bool, label: &str) {
    seed_simple_entities(rows);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let predicate = Predicate::Or(vec![
        id_in_predicate(&[rows[0], rows[1], rows[2], rows[3]]),
        id_in_predicate(&[rows[2], rows[3], rows[4], rows[5]]),
    ]);
    assert_aggregate_parity_for_query(
        &load,
        || {
            let query = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .distinct();
            if descending {
                query.order_by_desc("id").offset(1).limit(3)
            } else {
                query.order_by("id").offset(1).limit(3)
            }
        },
        label,
    );
}

fn assert_distinct_field_terminal_parity(rows: &[(u128, u32, u32)], descending: bool, label: &str) {
    seed_pushdown_entities(rows);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let predicate = Predicate::Or(vec![
        id_in_predicate(&[rows[0].0, rows[1].0, rows[2].0, rows[3].0]),
        id_in_predicate(&[rows[2].0, rows[3].0, rows[4].0, rows[5].0]),
    ]);

    assert_field_aggregate_parity_for_query(
        &load,
        || {
            let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .distinct();
            if descending {
                query.order_by_desc("id").offset(1).limit(4)
            } else {
                query.order_by("id").offset(1).limit(4)
            }
        },
        label,
    );
}

#[test]
fn aggregate_parity_ordered_page_window_desc() {
    seed_simple_entities(&[8201, 8202, 8203, 8204, 8205, 8206, 8207, 8208]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .order_by_desc("id")
                .offset(1)
                .limit(4)
        },
        "ordered DESC page window",
    );
}

#[test]
fn aggregate_parity_by_id_and_by_ids_paths() {
    seed_simple_entities(&[8601, 8602, 8603, 8604]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || Query::<SimpleEntity>::new(MissingRowPolicy::Ignore).by_id(Ulid::from_u128(8602)),
        "by_id path",
    );

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore).by_ids([
                Ulid::from_u128(8604),
                Ulid::from_u128(8601),
                Ulid::from_u128(8604),
            ])
        },
        "by_ids path",
    );
}

#[test]
fn aggregate_parity_by_id_window_shape() {
    seed_simple_entities(&[8611]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_id(Ulid::from_u128(8611))
                .order_by("id")
                .offset(1)
                .limit(1)
        },
        "by_id windowed shape",
    );
}

#[test]
fn aggregate_by_id_windowed_count_scans_one_candidate_key() {
    seed_simple_entities(&[8621]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_count(
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_id(Ulid::from_u128(8621))
                .order_by("id")
                .offset(1)
                .limit(1)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("by_id windowed COUNT plan should build"),
        )
        .expect("by_id windowed COUNT should succeed")
    });

    assert_eq!(count, 0, "offset window should exclude the only row");
    assert_eq!(
        scanned, 1,
        "single-key windowed COUNT should scan only one candidate key"
    );
}

#[test]
fn aggregate_by_id_strict_missing_surfaces_corruption_error() {
    seed_simple_entities(&[8631]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let err = load
        .aggregate_exists(
            Query::<SimpleEntity>::new(MissingRowPolicy::Error)
                .by_id(Ulid::from_u128(8632))
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("strict by_id EXISTS plan should build"),
        )
        .expect_err("strict by_id aggregate should fail when row is missing");

    assert_eq!(
        err.class,
        crate::error::ErrorClass::Corruption,
        "strict by_id aggregate missing row should classify as corruption"
    );
}

#[test]
fn aggregate_parity_by_ids_window_shape_with_duplicates() {
    seed_simple_entities(&[8641, 8642, 8643, 8644, 8645]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_ids([
                    Ulid::from_u128(8645),
                    Ulid::from_u128(8642),
                    Ulid::from_u128(8642),
                    Ulid::from_u128(8644),
                    Ulid::from_u128(8641),
                ])
                .order_by("id")
                .offset(1)
                .limit(2)
        },
        "by_ids windowed + duplicates shape",
    );
}

#[test]
fn aggregate_by_ids_count_dedups_before_windowing() {
    seed_simple_entities(&[8651, 8652, 8653, 8654, 8655]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_count(
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_ids([
                    Ulid::from_u128(8654),
                    Ulid::from_u128(8652),
                    Ulid::from_u128(8652),
                    Ulid::from_u128(8651),
                ])
                .order_by("id")
                .offset(1)
                .limit(1)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("by_ids dedup COUNT plan should build"),
        )
        .expect("by_ids dedup COUNT should succeed")
    });

    assert_eq!(count, 1, "by_ids dedup COUNT should keep one in-window row");
    assert_eq!(
        scanned, 3,
        "by_ids dedup COUNT should preserve parity via materialized fallback when COUNT pushdown is ineligible"
    );
}

#[test]
fn aggregate_by_ids_strict_missing_surfaces_corruption_error() {
    seed_simple_entities(&[8661]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let err = load
        .aggregate_count(
            Query::<SimpleEntity>::new(MissingRowPolicy::Error)
                .by_ids([Ulid::from_u128(8662)])
                .order_by("id")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("strict by_ids COUNT plan should build"),
        )
        .expect_err("strict by_ids aggregate should fail when row is missing");

    assert_eq!(
        err.class,
        crate::error::ErrorClass::Corruption,
        "strict by_ids aggregate missing row should classify as corruption"
    );
}

#[test]
fn aggregate_count_full_scan_window_scans_offset_plus_limit() {
    seed_simple_entities(&[8671, 8672, 8673, 8674, 8675, 8676, 8677]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_count(
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .offset(2)
                .limit(2)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("full-scan COUNT plan should build"),
        )
        .expect("full-scan COUNT should succeed")
    });

    assert_eq!(count, 2, "full-scan COUNT should honor the page window");
    assert_eq!(
        scanned, 4,
        "full-scan COUNT should scan exactly offset + limit keys"
    );
}

#[test]
fn aggregate_count_key_range_window_scans_offset_plus_limit() {
    seed_simple_entities(&[8681, 8682, 8683, 8684, 8685, 8686, 8687]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let mut logical_plan = crate::db::query::plan::AccessPlannedQuery::new(
        crate::db::access::AccessPath::KeyRange {
            start: Ulid::from_u128(8682),
            end: Ulid::from_u128(8686),
        },
        MissingRowPolicy::Ignore,
    );
    logical_plan.order = Some(crate::db::query::plan::OrderSpec {
        fields: vec![(
            "id".to_string(),
            crate::db::query::plan::OrderDirection::Asc,
        )],
    });
    logical_plan.page = Some(crate::db::query::plan::PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let key_range_plan = crate::db::executor::ExecutablePlan::<SimpleEntity>::new(logical_plan);

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_count(key_range_plan)
            .expect("key-range COUNT should succeed")
    });

    assert_eq!(count, 2, "key-range COUNT should honor the page window");
    assert_eq!(
        scanned, 3,
        "key-range COUNT should scan exactly offset + limit keys"
    );
}

#[test]
fn aggregate_exists_index_range_window_scans_offset_plus_one() {
    seed_unique_index_range_entities(&[
        (8691, 100),
        (8692, 101),
        (8693, 102),
        (8694, 103),
        (8695, 104),
        (8696, 105),
    ]);
    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);

    let mut logical_plan = crate::db::query::plan::AccessPlannedQuery::new(
        crate::db::access::AccessPath::index_range(
            UNIQUE_INDEX_RANGE_INDEX_MODELS[0],
            vec![],
            std::ops::Bound::Included(Value::Uint(101)),
            std::ops::Bound::Excluded(Value::Uint(106)),
        ),
        MissingRowPolicy::Ignore,
    );
    logical_plan.order = Some(crate::db::query::plan::OrderSpec {
        fields: vec![
            (
                "code".to_string(),
                crate::db::query::plan::OrderDirection::Asc,
            ),
            (
                "id".to_string(),
                crate::db::query::plan::OrderDirection::Asc,
            ),
        ],
    });
    logical_plan.page = Some(crate::db::query::plan::PageSpec {
        limit: None,
        offset: 2,
    });
    let index_range_plan =
        crate::db::executor::ExecutablePlan::<UniqueIndexRangeEntity>::new(logical_plan);

    let (exists, scanned) = capture_rows_scanned_for_entity(UniqueIndexRangeEntity::PATH, || {
        load.aggregate_exists(index_range_plan)
            .expect("index-range EXISTS should succeed")
    });

    assert!(
        exists,
        "index-range EXISTS window should find a matching row"
    );
    assert_eq!(
        scanned, 3,
        "index-range EXISTS window should scan exactly offset + 1 keys"
    );
}

#[test]
fn aggregate_parity_distinct_asc() {
    assert_distinct_parity_for_simple_rows(&DISTINCT_ASC_ROWS, false, "distinct ASC");
}

#[test]
fn aggregate_parity_distinct_desc() {
    assert_distinct_parity_for_simple_rows(&DISTINCT_DESC_ROWS, true, "distinct DESC");
}

#[test]
fn aggregate_field_parity_matrix_harness_covers_all_rank_terminals() {
    let labels = aggregate_field_terminal_parity_cases().map(|case| case.label);

    assert_eq!(
        labels,
        [
            "min_by(rank)",
            "max_by(rank)",
            "nth_by(rank, 1)",
            "sum_by(rank)",
            "avg_by(rank)",
            "median_by(rank)",
            "count_distinct_by(rank)",
            "min_max_by(rank)",
        ]
    );
}

#[test]
fn aggregate_field_terminal_parity_distinct_asc() {
    assert_distinct_field_terminal_parity(
        &FIELD_DISTINCT_ASC_ROWS,
        false,
        "field terminals distinct ASC",
    );
}

#[test]
fn aggregate_field_terminal_parity_distinct_desc() {
    assert_distinct_field_terminal_parity(
        &FIELD_DISTINCT_DESC_ROWS,
        true,
        "field terminals distinct DESC",
    );
}

#[test]
fn aggregate_parity_union_and_intersection_paths() {
    seed_simple_entities(&[8701, 8702, 8703, 8704, 8705, 8706]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let union_predicate = Predicate::Or(vec![
        id_in_predicate(&[8701, 8702, 8703, 8704]),
        id_in_predicate(&[8703, 8704, 8705, 8706]),
    ]);
    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .filter(union_predicate.clone())
                .order_by("id")
                .offset(1)
                .limit(4)
        },
        "union path",
    );

    let intersection_predicate = Predicate::And(vec![
        id_in_predicate(&[8701, 8702, 8703, 8704]),
        id_in_predicate(&[8703, 8704, 8705, 8706]),
    ]);
    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .filter(intersection_predicate.clone())
                .order_by_desc("id")
                .offset(0)
                .limit(2)
        },
        "intersection path",
    );
}

#[test]
fn aggregate_composite_count_direct_path_scan_does_not_exceed_fallback() {
    let (first, second) = composite_key_sets_with_base(8751);
    assert_composite_terminal_direct_path_scan_does_not_exceed_fallback(
        &phase_rows_with_base(8751),
        first,
        second,
        CompositeTerminal::Count,
        "COUNT",
    );
}

#[test]
fn aggregate_composite_exists_direct_path_scan_does_not_exceed_fallback() {
    let (first, second) = composite_key_sets_with_base(8761);
    assert_composite_terminal_direct_path_scan_does_not_exceed_fallback(
        &phase_rows_with_base(8761),
        first,
        second,
        CompositeTerminal::Exists,
        "EXISTS",
    );
}

#[test]
fn aggregate_parity_index_range_shape() {
    seed_unique_index_range_entities(&[
        (8901, 100),
        (8902, 101),
        (8903, 102),
        (8904, 103),
        (8905, 104),
        (8906, 105),
    ]);
    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    let range_predicate = u32_range_predicate("code", 101, 105);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
                .filter(range_predicate.clone())
                .order_by_desc("code")
                .offset(1)
                .limit(2)
        },
        "index-range shape",
    );
}

#[test]
fn aggregate_parity_strict_consistency() {
    seed_simple_entities(&[9001, 9002, 9003, 9004, 9005]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Error)
                .order_by_desc("id")
                .offset(1)
                .limit(3)
        },
        "strict consistency",
    );
}

#[test]
fn aggregate_parity_limit_zero_window() {
    seed_simple_entities(&[9101, 9102, 9103, 9104]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .offset(2)
                .limit(0)
        },
        "limit zero window",
    );
}
