//! Module: db::executor::tests::aggregate::path_parity_matrix
//! Responsibility: module-local ownership and contracts for db::executor::tests::aggregate::path_parity_matrix.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

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
    logical_plan.scalar_plan_mut().order = Some(crate::db::query::plan::OrderSpec {
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
        ExecutionKernel::is_stream_order_contract_safe::<PhaseEntity, _>(direct_plan.as_inner()),
        "direct composite {label} shape should be streaming-safe"
    );
    assert!(
        matches!(
            execution_root_node_type(&direct_plan),
            ExplainExecutionNodeType::Union | ExplainExecutionNodeType::Intersection
        ),
        "direct {label} shape should compile to a composite access path"
    );

    let fallback_plan = build_phase_composite_plan("label", first, second);
    assert!(
        !ExecutionKernel::is_stream_order_contract_safe::<PhaseEntity, _>(fallback_plan.as_inner()),
        "fallback composite {label} shape should be streaming-unsafe"
    );
    assert!(
        matches!(
            execution_root_node_type(&fallback_plan),
            ExplainExecutionNodeType::Union | ExplainExecutionNodeType::Intersection
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

    assert_bytes_parity_for_query(
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

    assert_bytes_parity_for_query(
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

    assert_bytes_by_parity_for_query(
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
        "rank",
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

    assert_bytes_parity_for_query(
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
fn aggregate_bytes_parity_ordered_page_window_asc() {
    seed_simple_entities(&[8_981, 8_982, 8_983, 8_984, 8_985, 8_986, 8_987, 8_988]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_bytes_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .offset(2)
                .limit(3)
        },
        "ordered ASC page window",
    );
}

#[test]
fn aggregate_bytes_key_range_window_parity_desc() {
    seed_simple_entities(&[8_989, 8_990, 8_991, 8_992, 8_993, 8_994, 8_995]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let mut logical_plan = crate::db::query::plan::AccessPlannedQuery::new(
        crate::db::access::AccessPath::KeyRange {
            start: Ulid::from_u128(8_990),
            end: Ulid::from_u128(8_994),
        },
        MissingRowPolicy::Ignore,
    );
    logical_plan.scalar_plan_mut().order = Some(crate::db::query::plan::OrderSpec {
        fields: vec![(
            "id".to_string(),
            crate::db::query::plan::OrderDirection::Desc,
        )],
    });
    logical_plan.scalar_plan_mut().page = Some(crate::db::query::plan::PageSpec {
        limit: Some(2),
        offset: 1,
    });

    let expected_response = load
        .execute(crate::db::executor::ExecutablePlan::<SimpleEntity>::new(
            logical_plan.clone(),
        ))
        .expect("baseline key-range bytes parity execute should succeed");
    let expected_bytes = persisted_payload_bytes_for_ids::<SimpleEntity>(expected_response.ids());
    let bytes = load
        .bytes(crate::db::executor::ExecutablePlan::<SimpleEntity>::new(
            logical_plan,
        ))
        .expect("key-range bytes terminal should succeed");

    assert_eq!(
        bytes, expected_bytes,
        "key-range DESC bytes window should match canonical execute parity",
    );
}

#[test]
fn aggregate_bytes_pk_fast_path_emits_hit_marker_only_for_eligible_shapes() {
    seed_simple_entities(&[9_011, 9_012, 9_013, 9_014]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let _ = LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesPrimaryKeyFastPath,
    );
    let _eligible_bytes = load
        .bytes(
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .offset(1)
                .limit(2)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("eligible bytes fast-path plan should build"),
        )
        .expect("eligible bytes fast-path execution should succeed");
    assert_eq!(
        LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesPrimaryKeyFastPath
        ),
        1,
        "PK full-scan bytes shape should emit one fast-path hit marker",
    );

    let _ = LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesPrimaryKeyFastPath,
    );
    let _fallback_bytes = load
        .bytes(
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_id(Ulid::from_u128(9_011))
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("ineligible bytes fallback plan should build"),
        )
        .expect("ineligible bytes fallback execution should succeed");
    assert_eq!(
        LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesPrimaryKeyFastPath
        ),
        0,
        "by-id bytes shape should bypass the PK fast-path branch",
    );
}

#[test]
fn aggregate_bytes_key_range_fast_path_emits_hit_marker_only_without_residual_predicates() {
    seed_simple_entities(&[9_021, 9_022, 9_023, 9_024, 9_025]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let eligible_plan = || {
        let mut logical_plan = AccessPlannedQuery::new(
            AccessPath::KeyRange {
                start: Ulid::from_u128(9_021),
                end: Ulid::from_u128(9_025),
            },
            MissingRowPolicy::Ignore,
        );
        logical_plan.scalar_plan_mut().order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        logical_plan.scalar_plan_mut().page = Some(PageSpec {
            limit: Some(2),
            offset: 1,
        });

        ExecutablePlan::<SimpleEntity>::new(logical_plan)
    };
    let ineligible_plan = || {
        let mut logical_plan = AccessPlannedQuery::new(
            AccessPath::KeyRange {
                start: Ulid::from_u128(9_021),
                end: Ulid::from_u128(9_025),
            },
            MissingRowPolicy::Ignore,
        );
        logical_plan.scalar_plan_mut().predicate = Some(id_in_predicate(&[9_022, 9_023, 9_024]));
        logical_plan.scalar_plan_mut().order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        logical_plan.scalar_plan_mut().page = Some(PageSpec {
            limit: Some(2),
            offset: 0,
        });

        ExecutablePlan::<SimpleEntity>::new(logical_plan)
    };

    let _ = LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesPrimaryKeyFastPath,
    );
    let eligible_bytes = load
        .bytes(eligible_plan())
        .expect("eligible key-range bytes fast-path execution should succeed");
    assert!(
        eligible_bytes > 0,
        "eligible key-range bytes fast-path should return a non-zero payload sum",
    );
    assert_eq!(
        LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesPrimaryKeyFastPath
        ),
        1,
        "key-range bytes shape without residual predicates should emit one fast-path hit marker",
    );

    let _ = LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesPrimaryKeyFastPath,
    );
    let fallback_bytes = load
        .bytes(ineligible_plan())
        .expect("residual key-range bytes fallback execution should succeed");
    assert!(
        fallback_bytes > 0,
        "residual key-range bytes fallback should still return payload bytes",
    );
    assert_eq!(
        LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesPrimaryKeyFastPath
        ),
        0,
        "residual key-range bytes shape should bypass the PK fast-path branch",
    );
}

#[test]
fn aggregate_bytes_unordered_secondary_stream_fast_path_emits_hit_marker_with_parity() {
    seed_pushdown_entities(&[
        (9_031, 7, 10),
        (9_032, 7, 20),
        (9_033, 7, 30),
        (9_034, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let build_plan = || {
        let mut logical_plan = AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: PUSHDOWN_PARITY_INDEX_MODELS[0],
                values: vec![Value::Uint(7)],
            },
            MissingRowPolicy::Ignore,
        );
        logical_plan.scalar_plan_mut().page = Some(PageSpec {
            limit: Some(2),
            offset: 1,
        });

        ExecutablePlan::<PushdownParityEntity>::new(logical_plan)
    };
    let expected_response = load
        .execute(build_plan())
        .expect("baseline unordered secondary bytes execute should succeed");
    let expected_bytes =
        persisted_payload_bytes_for_ids::<PushdownParityEntity>(expected_response.ids());

    let _ = LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesPrimaryKeyFastPath,
    );
    let _ = LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesStreamFastPath,
    );
    let bytes = load
        .bytes(build_plan())
        .expect("unordered secondary bytes fast path should succeed");
    assert_eq!(
        bytes, expected_bytes,
        "unordered secondary bytes stream fast path should preserve execute() parity",
    );
    assert_eq!(
        LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesPrimaryKeyFastPath
        ),
        0,
        "secondary bytes shape should bypass PK-only bytes fast-path markers",
    );
    assert_eq!(
        LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesStreamFastPath
        ),
        1,
        "unordered secondary bytes shape should emit one stream-fast-path hit marker",
    );
}

#[test]
fn aggregate_bytes_stream_fast_path_bypasses_ordered_secondary_shape() {
    seed_pushdown_entities(&[
        (9_041, 7, 10),
        (9_042, 7, 20),
        (9_043, 7, 30),
        (9_044, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let mut logical_plan = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: PUSHDOWN_PARITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    logical_plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    logical_plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    let _ = LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesStreamFastPath,
    );
    let bytes = load
        .bytes(ExecutablePlan::<PushdownParityEntity>::new(logical_plan))
        .expect("ordered secondary bytes execution should succeed");
    assert!(
        bytes > 0,
        "ordered secondary bytes fallback should still return persisted payload bytes",
    );
    assert_eq!(
        LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesStreamFastPath
        ),
        0,
        "ordered secondary bytes shape should bypass unordered stream fast-path branch",
    );
}

#[test]
fn aggregate_bytes_ordered_by_ids_stream_fast_path_emits_hit_marker_with_parity() {
    seed_simple_entities(&[9_045, 9_046, 9_047, 9_048, 9_049]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let build_plan = || {
        Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
            .by_ids([
                Ulid::from_u128(9_048),
                Ulid::from_u128(9_046),
                Ulid::from_u128(9_046),
                Ulid::from_u128(9_045),
            ])
            .order_by_desc("id")
            .offset(1)
            .limit(1)
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("ordered by_ids bytes plan should build")
    };
    let expected_response = load
        .execute(build_plan())
        .expect("baseline ordered by_ids bytes execute should succeed");
    let expected_bytes = persisted_payload_bytes_for_ids::<SimpleEntity>(expected_response.ids());

    let _ = LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesPrimaryKeyFastPath,
    );
    let _ = LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesStreamFastPath,
    );
    let bytes = load
        .bytes(build_plan())
        .expect("ordered by_ids bytes stream fast path should succeed");
    assert_eq!(
        bytes, expected_bytes,
        "ordered by_ids bytes stream fast path should preserve execute() parity",
    );
    assert_eq!(
        LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesPrimaryKeyFastPath
        ),
        0,
        "ordered by_ids bytes shape should bypass PK full-scan/key-range markers",
    );
    assert_eq!(
        LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesStreamFastPath
        ),
        1,
        "ordered by_ids bytes shape should emit one stream-fast-path hit marker",
    );
}

#[test]
fn aggregate_bytes_path_parity_index_prefix_and_full_scan_equivalent_rows() {
    seed_pushdown_entities(&[
        (8_971, 7, 10),
        (8_972, 7, 20),
        (8_973, 7, 30),
        (8_974, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let mut index_logical = crate::db::query::plan::AccessPlannedQuery::new(
        crate::db::access::AccessPath::IndexPrefix {
            index: PUSHDOWN_PARITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    index_logical.scalar_plan_mut().order = Some(crate::db::query::plan::OrderSpec {
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
    let index_plan =
        crate::db::executor::ExecutablePlan::<PushdownParityEntity>::new(index_logical);

    let mut full_scan_logical = crate::db::query::plan::AccessPlannedQuery::new(
        crate::db::access::AccessPath::FullScan,
        MissingRowPolicy::Ignore,
    );
    full_scan_logical.scalar_plan_mut().predicate = Some(u32_eq_predicate("group", 7));
    full_scan_logical.scalar_plan_mut().order = Some(crate::db::query::plan::OrderSpec {
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
    let full_scan_plan =
        crate::db::executor::ExecutablePlan::<PushdownParityEntity>::new(full_scan_logical);

    assert_eq!(
        execution_root_node_type(&index_plan),
        ExplainExecutionNodeType::IndexPrefixScan,
        "group equality filter should route through index-prefix access",
    );
    assert_eq!(
        execution_root_node_type(&full_scan_plan),
        ExplainExecutionNodeType::FullScan,
        "non-indexed label IN filter should route through full scan",
    );

    let index_bytes = load
        .bytes(index_plan)
        .expect("index-prefix bytes terminal should succeed");
    let full_scan_bytes = load
        .bytes(full_scan_plan)
        .expect("full-scan bytes terminal should succeed");

    assert_eq!(
        index_bytes, full_scan_bytes,
        "equivalent index-prefix/full-scan row sets should yield identical bytes totals"
    );

    let expected_bytes = persisted_payload_bytes_for_ids::<PushdownParityEntity>(
        load.execute(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by("rank")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("bytes expected-baseline plan should build"),
        )
        .expect("bytes expected-baseline execute should succeed")
        .ids(),
    );
    assert_eq!(
        index_bytes, expected_bytes,
        "forced index-prefix bytes total should match canonical query window",
    );
    assert_eq!(
        full_scan_bytes, expected_bytes,
        "forced full-scan bytes total should match canonical query window",
    );
}

#[test]
fn aggregate_bytes_by_path_parity_index_prefix_and_full_scan_equivalent_rows() {
    seed_pushdown_entities(&[
        (8_981, 7, 5),
        (8_982, 7, 10),
        (8_983, 7, 20),
        (8_984, 8, 40),
        (8_985, 7, 30),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let mut index_logical = crate::db::query::plan::AccessPlannedQuery::new(
        crate::db::access::AccessPath::IndexPrefix {
            index: PUSHDOWN_PARITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    index_logical.scalar_plan_mut().order = Some(crate::db::query::plan::OrderSpec {
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
    let index_plan =
        crate::db::executor::ExecutablePlan::<PushdownParityEntity>::new(index_logical);

    let mut full_scan_logical = crate::db::query::plan::AccessPlannedQuery::new(
        crate::db::access::AccessPath::FullScan,
        MissingRowPolicy::Ignore,
    );
    full_scan_logical.scalar_plan_mut().predicate = Some(u32_eq_predicate("group", 7));
    full_scan_logical.scalar_plan_mut().order = Some(crate::db::query::plan::OrderSpec {
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
    let full_scan_plan =
        crate::db::executor::ExecutablePlan::<PushdownParityEntity>::new(full_scan_logical);

    assert_eq!(
        execution_root_node_type(&index_plan),
        ExplainExecutionNodeType::IndexPrefixScan,
        "group equality filter should route through index-prefix access",
    );
    assert_eq!(
        execution_root_node_type(&full_scan_plan),
        ExplainExecutionNodeType::FullScan,
        "non-indexed label IN filter should route through full scan",
    );

    let index_bytes = load
        .bytes_by_slot(index_plan, slot(&load, "rank"))
        .expect("index-prefix bytes_by(rank) terminal should succeed");
    let full_scan_bytes = load
        .bytes_by_slot(full_scan_plan, slot(&load, "rank"))
        .expect("full-scan bytes_by(rank) terminal should succeed");

    assert_eq!(
        index_bytes, full_scan_bytes,
        "equivalent index-prefix/full-scan row sets should yield identical bytes_by(rank) totals"
    );

    let expected_response = load
        .execute(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by("rank")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("bytes_by expected-baseline plan should build"),
        )
        .expect("bytes_by expected-baseline execute should succeed");
    let expected_bytes = serialized_field_payload_bytes_for_rows(&expected_response, "rank");
    assert_eq!(
        index_bytes, expected_bytes,
        "forced index-prefix bytes_by(rank) total should match canonical query window",
    );
    assert_eq!(
        full_scan_bytes, expected_bytes,
        "forced full-scan bytes_by(rank) total should match canonical query window",
    );
}

#[test]
fn aggregate_bytes_by_covering_index_fast_path_emits_hit_marker_for_eligible_shape() {
    seed_pushdown_entities(&[
        (8_991, 7, 10),
        (8_992, 7, 20),
        (8_993, 7, 30),
        (8_994, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let build_plan = || {
        let mut logical_plan = AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: PUSHDOWN_PARITY_INDEX_MODELS[0],
                values: vec![Value::Uint(7)],
            },
            MissingRowPolicy::Ignore,
        );
        logical_plan.scalar_plan_mut().order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });

        ExecutablePlan::<PushdownParityEntity>::new(logical_plan)
    };
    let expected_response = load
        .execute(build_plan())
        .expect("baseline covering-index bytes_by(rank) execute should succeed");
    let expected_bytes = serialized_field_payload_bytes_for_rows(&expected_response, "rank");

    let _ = LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesByCoveringIndexFastPath,
    );
    let _ = LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesByCoveringConstantFastPath,
    );
    let bytes = load
        .bytes_by_slot(build_plan(), slot(&load, "rank"))
        .expect("covering-index bytes_by(rank) terminal should succeed");

    assert_eq!(
        bytes, expected_bytes,
        "covering-index bytes_by(rank) path should preserve execute() parity",
    );
    assert_eq!(
        LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesByCoveringIndexFastPath
        ),
        1,
        "eligible bytes_by(rank) index-prefix shape should emit one covering-index fast-path hit marker",
    );
    assert_eq!(
        LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesByCoveringConstantFastPath
        ),
        0,
        "covering-index bytes_by(rank) shape should not emit constant-covering hit markers",
    );
}

#[test]
fn aggregate_bytes_by_projection_mode_classifier_matches_bounded_route_shapes() {
    let covering_index_plan = {
        let mut logical_plan = AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: PUSHDOWN_PARITY_INDEX_MODELS[0],
                values: vec![Value::Uint(7)],
            },
            MissingRowPolicy::Ignore,
        );
        logical_plan.scalar_plan_mut().order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });
        ExecutablePlan::<PushdownParityEntity>::new(logical_plan)
    };
    let covering_index_mode = covering_index_plan.bytes_by_projection_mode("rank");
    assert_eq!(
        covering_index_mode,
        crate::db::executor::BytesByProjectionMode::CoveringIndex,
        "bytes-by classifier should mark eligible ordered index-prefix shapes as covering-index",
    );
    assert_eq!(
        ExecutablePlan::<PushdownParityEntity>::bytes_by_projection_mode_label(covering_index_mode),
        "field_covering_index",
        "bytes-by classifier labels should remain stable for covering-index mode",
    );

    let constant_covering_plan =
        ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: PUSHDOWN_PARITY_INDEX_MODELS[0],
                values: vec![Value::Uint(7), Value::Uint(20)],
            },
            MissingRowPolicy::Ignore,
        ));
    let constant_mode = constant_covering_plan.bytes_by_projection_mode("rank");
    assert_eq!(
        constant_mode,
        crate::db::executor::BytesByProjectionMode::CoveringConstant,
        "bytes-by classifier should mark prefix-bound fields as covering-constant",
    );
    assert_eq!(
        ExecutablePlan::<PushdownParityEntity>::bytes_by_projection_mode_label(constant_mode),
        "field_covering_constant",
        "bytes-by classifier labels should remain stable for covering-constant mode",
    );

    let strict_plan = ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: PUSHDOWN_PARITY_INDEX_MODELS[0],
            values: vec![Value::Uint(7), Value::Uint(20)],
        },
        MissingRowPolicy::Error,
    ));
    let strict_mode = strict_plan.bytes_by_projection_mode("rank");
    assert_eq!(
        strict_mode,
        crate::db::executor::BytesByProjectionMode::Materialized,
        "strict bytes-by classifier should fail closed to materialized mode",
    );
    assert_eq!(
        ExecutablePlan::<PushdownParityEntity>::bytes_by_projection_mode_label(strict_mode),
        "field_materialized",
        "bytes-by classifier labels should remain stable for strict materialized mode",
    );
}

#[test]
fn aggregate_bytes_by_constant_covering_fast_path_emits_hit_marker_for_prefix_bound_field() {
    seed_pushdown_entities(&[
        (8_996, 7, 20),
        (8_997, 7, 20),
        (8_998, 7, 30),
        (8_999, 8, 20),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let build_plan = || {
        let logical_plan = AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: PUSHDOWN_PARITY_INDEX_MODELS[0],
                values: vec![Value::Uint(7), Value::Uint(20)],
            },
            MissingRowPolicy::Ignore,
        );

        ExecutablePlan::<PushdownParityEntity>::new(logical_plan)
    };
    let expected_response = load
        .execute(build_plan())
        .expect("baseline constant-covering bytes_by(rank) execute should succeed");
    let expected_bytes = serialized_field_payload_bytes_for_rows(&expected_response, "rank");

    let _ = LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesByCoveringIndexFastPath,
    );
    let _ = LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesByCoveringConstantFastPath,
    );
    let bytes = load
        .bytes_by_slot(build_plan(), slot(&load, "rank"))
        .expect("constant-covering bytes_by(rank) terminal should succeed");

    assert_eq!(
        bytes, expected_bytes,
        "constant-covering bytes_by(rank) path should preserve execute() parity",
    );
    assert_eq!(
        LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesByCoveringIndexFastPath
        ),
        0,
        "constant-covering bytes_by(rank) shape should bypass index-covering hit markers",
    );
    assert_eq!(
        LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesByCoveringConstantFastPath
        ),
        1,
        "prefix-bound bytes_by(rank) shape should emit one constant-covering fast-path hit marker",
    );
}

#[test]
fn aggregate_bytes_by_constant_covering_fast_path_survives_residual_predicate_shape() {
    seed_pushdown_entities(&[
        (8_951, 7, 20),
        (8_952, 7, 20),
        (8_953, 7, 30),
        (8_954, 8, 20),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let build_plan = || {
        let mut logical_plan = AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: PUSHDOWN_PARITY_INDEX_MODELS[0],
                values: vec![Value::Uint(7), Value::Uint(20)],
            },
            MissingRowPolicy::Ignore,
        );
        logical_plan.scalar_plan_mut().predicate = Some(id_in_predicate(&[8_952]));

        ExecutablePlan::<PushdownParityEntity>::new(logical_plan)
    };
    let expected_response = load
        .execute(build_plan())
        .expect("baseline residual-predicate bytes_by(rank) execute should succeed");
    let expected_bytes = serialized_field_payload_bytes_for_rows(&expected_response, "rank");

    let _ = LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesByCoveringIndexFastPath,
    );
    let _ = LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesByCoveringConstantFastPath,
    );
    let bytes = load
        .bytes_by_slot(build_plan(), slot(&load, "rank"))
        .expect("residual-predicate bytes_by(rank) terminal should succeed");

    assert_eq!(
        bytes, expected_bytes,
        "constant-covering bytes_by(rank) should preserve parity under residual-predicate shapes",
    );
    assert_eq!(
        LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesByCoveringIndexFastPath
        ),
        0,
        "residual-predicate bytes_by(rank) shape should bypass index-covering fast-path marker",
    );
    assert_eq!(
        LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesByCoveringConstantFastPath
        ),
        1,
        "residual-predicate bytes_by(rank) shape should still emit constant-covering fast-path marker",
    );
}

#[test]
fn aggregate_bytes_by_constant_covering_fast_path_bypasses_strict_mode() {
    seed_pushdown_entities(&[
        (8_956, 7, 20),
        (8_957, 7, 20),
        (8_958, 7, 30),
        (8_959, 8, 20),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let build_plan = || {
        let logical_plan = AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: PUSHDOWN_PARITY_INDEX_MODELS[0],
                values: vec![Value::Uint(7), Value::Uint(20)],
            },
            MissingRowPolicy::Error,
        );

        ExecutablePlan::<PushdownParityEntity>::new(logical_plan)
    };
    let expected_response = load
        .execute(build_plan())
        .expect("baseline strict-mode bytes_by(rank) execute should succeed");
    let expected_bytes = serialized_field_payload_bytes_for_rows(&expected_response, "rank");

    let _ = LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesByCoveringIndexFastPath,
    );
    let _ = LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesByCoveringConstantFastPath,
    );
    let bytes = load
        .bytes_by_slot(build_plan(), slot(&load, "rank"))
        .expect("strict-mode bytes_by(rank) terminal should succeed");

    assert_eq!(
        bytes, expected_bytes,
        "strict-mode bytes_by(rank) should preserve execute() parity",
    );
    assert_eq!(
        LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesByCoveringConstantFastPath
        ),
        0,
        "strict-mode bytes_by(rank) must bypass constant-covering fast-path markers",
    );
    assert_eq!(
        LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesByCoveringIndexFastPath
        ),
        0,
        "strict-mode bytes_by(rank) must bypass index-covering fast-path markers",
    );
}

#[test]
fn aggregate_bytes_by_strict_mode_surfaces_missing_row_corruption() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PushdownParityEntity>::new(DB, false);
    for (id, group, rank) in [(8_961u128, 7u32, 20u32), (8_962, 7, 20), (8_963, 7, 30)] {
        save.insert(PushdownParityEntity {
            id: Ulid::from_u128(id),
            group,
            rank,
            label: format!("g{group}-r{rank}"),
        })
        .expect("strict bytes_by seed row save should succeed");
    }

    remove_pushdown_row_data(8_962);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let _ = LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesByCoveringIndexFastPath,
    );
    let _ = LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::BytesByCoveringConstantFastPath,
    );
    let err = load
        .bytes_by_slot(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Error)
                .filter(u32_eq_predicate_strict("group", 7))
                .order_by("rank")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("strict bytes_by plan should build"),
            slot(&load, "rank"),
        )
        .expect_err("strict bytes_by should fail on missing primary rows");

    assert_eq!(
        err.class,
        ErrorClass::Corruption,
        "strict bytes_by must preserve missing-row corruption classification",
    );
    assert!(
        err.message.contains("missing row"),
        "strict bytes_by must preserve missing-row error context",
    );
    assert_eq!(
        LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesByCoveringConstantFastPath
        ),
        0,
        "strict bytes_by missing-row shape must not emit constant-covering markers",
    );
    assert_eq!(
        LoadExecutor::<PushdownParityEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::BytesByCoveringIndexFastPath
        ),
        0,
        "strict bytes_by missing-row shape must not emit index-covering markers",
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
fn aggregate_by_id_count_ignore_missing_returns_zero() {
    seed_simple_entities(&[8626]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_count(
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_id(Ulid::from_u128(8627))
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("ignore by_id COUNT plan should build"),
        )
        .expect("ignore by_id COUNT should succeed")
    });

    assert_eq!(
        count, 0,
        "missing by_id COUNT should return zero under ignore mode"
    );
    assert_eq!(
        scanned, 1,
        "missing by_id COUNT should evaluate exactly one candidate key",
    );
}

#[test]
fn aggregate_by_id_count_strict_missing_surfaces_corruption_error() {
    seed_simple_entities(&[8628]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let err = load
        .aggregate_count(
            Query::<SimpleEntity>::new(MissingRowPolicy::Error)
                .by_id(Ulid::from_u128(8629))
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("strict by_id COUNT plan should build"),
        )
        .expect_err("strict by_id COUNT should fail when row is missing");

    assert_eq!(
        err.class,
        crate::error::ErrorClass::Corruption,
        "strict by_id COUNT missing row should classify as corruption",
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
    let _ = LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::PrimaryKeyCountFastPath,
    );

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
        scanned, 2,
        "ordered by_ids dedup COUNT should scan only offset + limit rows on the key-stream fast path",
    );
    assert_eq!(
        LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::PrimaryKeyCountFastPath
        ),
        1,
        "ordered by_ids COUNT should emit one primary-key stream fast-path hit",
    );
}

#[test]
fn aggregate_by_ids_count_pk_desc_window_uses_primary_key_stream_fast_path() {
    seed_simple_entities(&[8_656, 8_657, 8_658, 8_659, 8_660]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let _ = LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::PrimaryKeyCountFastPath,
    );

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_count(
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_ids([
                    Ulid::from_u128(8_659),
                    Ulid::from_u128(8_657),
                    Ulid::from_u128(8_657),
                    Ulid::from_u128(8_656),
                ])
                .order_by_desc("id")
                .offset(1)
                .limit(1)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("ordered by_ids DESC COUNT plan should build"),
        )
        .expect("ordered by_ids DESC COUNT should succeed")
    });

    assert_eq!(
        count, 1,
        "ordered by_ids DESC COUNT should keep one in-window row"
    );
    assert_eq!(
        scanned, 2,
        "ordered by_ids DESC COUNT should scan only offset + limit rows on the key-stream fast path",
    );
    assert_eq!(
        LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::PrimaryKeyCountFastPath
        ),
        1,
        "ordered by_ids DESC COUNT should emit one primary-key stream fast-path hit",
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
fn aggregate_count_pk_cardinality_fast_path_emits_hit_marker_only_for_eligible_shapes() {
    seed_simple_entities(&[8_671, 8_672, 8_673, 8_674]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let _ = LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::PrimaryKeyCardinalityCountFastPath,
    );
    let _eligible_count = load
        .aggregate_count(
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .offset(1)
                .limit(2)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("eligible COUNT PK-cardinality plan should build"),
        )
        .expect("eligible COUNT PK-cardinality execution should succeed");
    assert_eq!(
        LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::PrimaryKeyCardinalityCountFastPath
        ),
        1,
        "PK full-scan COUNT shape should emit one PK-cardinality fast-path hit marker",
    );

    let _ = LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::PrimaryKeyCardinalityCountFastPath,
    );
    let _ineligible_count = load
        .aggregate_count(
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_id(Ulid::from_u128(8_671))
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("ineligible COUNT fallback plan should build"),
        )
        .expect("ineligible COUNT fallback execution should succeed");
    assert_eq!(
        LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::PrimaryKeyCardinalityCountFastPath
        ),
        0,
        "by-id COUNT shape should bypass the PK-cardinality fast-path branch",
    );
}

#[test]
fn aggregate_count_primary_key_stream_fast_path_emits_hit_marker_for_by_ids_unordered_shape() {
    seed_simple_entities(&[8_701, 8_702, 8_703, 8_704]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let _ = LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::PrimaryKeyCountFastPath,
    );
    let _ = LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
        ExecutionOptimizationCounter::PrimaryKeyCardinalityCountFastPath,
    );
    let count = load
        .aggregate_count(
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_ids([
                    Ulid::from_u128(8_704),
                    Ulid::from_u128(8_702),
                    Ulid::from_u128(8_702),
                    Ulid::from_u128(8_701),
                ])
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("eligible unordered by-ids COUNT plan should build"),
        )
        .expect("eligible unordered by-ids COUNT execution should succeed");

    assert_eq!(
        count, 3,
        "unordered by-ids COUNT should preserve canonical dedup semantics",
    );
    assert_eq!(
        LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::PrimaryKeyCountFastPath
        ),
        1,
        "unordered by-ids COUNT shape should emit one primary-key stream fast-path hit marker",
    );
    assert_eq!(
        LoadExecutor::<SimpleEntity>::take_execution_optimization_hits_for_tests(
            ExecutionOptimizationCounter::PrimaryKeyCardinalityCountFastPath
        ),
        0,
        "by-ids COUNT shape should not emit PK-cardinality fast-path markers",
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
    logical_plan.scalar_plan_mut().order = Some(crate::db::query::plan::OrderSpec {
        fields: vec![(
            "id".to_string(),
            crate::db::query::plan::OrderDirection::Asc,
        )],
    });
    logical_plan.scalar_plan_mut().page = Some(crate::db::query::plan::PageSpec {
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
fn aggregate_exists_full_scan_window_scans_offset_plus_one() {
    seed_simple_entities(&[8681, 8682, 8683, 8684, 8685, 8686, 8687]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (exists, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_exists(
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .offset(2)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("full-scan EXISTS plan should build"),
        )
        .expect("full-scan EXISTS should succeed")
    });

    assert!(exists, "full-scan EXISTS window should find a matching row");
    assert_eq!(
        scanned, 3,
        "full-scan EXISTS window should scan exactly offset + 1 keys"
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
    logical_plan.scalar_plan_mut().order = Some(crate::db::query::plan::OrderSpec {
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
    logical_plan.scalar_plan_mut().page = Some(crate::db::query::plan::PageSpec {
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
