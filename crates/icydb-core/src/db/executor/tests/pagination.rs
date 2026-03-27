//! Module: db::executor::tests::pagination
//! Responsibility: executor-owned pagination contracts for the revived live test harness.
//! Does not own: old matrix wrappers or query-intent paging policy tests.
//! Boundary: covers small runtime pagination behaviors that are easiest to validate end-to-end.

use super::*;
use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        cursor::{ContinuationToken, CursorBoundary, CursorBoundarySlot},
        executor::ExecutablePlan,
        executor::pipeline::contracts::{CursorPage, PageCursor},
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::plan::{
            AccessPlannedQuery, LoadSpec, LogicalPlan, OrderDirection, OrderSpec, PageSpec,
            QueryMode, ScalarPlan,
        },
        response::EntityResponse,
    },
    metrics::sink::{MetricsEvent, MetricsSink, with_metrics_sink},
    types::Ulid,
    value::Value,
};
use proptest::prelude::*;
use std::{cell::RefCell, collections::BTreeSet, ops::Bound};

///
/// PaginationCaptureSink
///
/// Small metrics sink used to keep row-scan assertions local to the live
/// pagination tests without re-enabling the old matrix wrapper family.
///

#[derive(Default)]
struct PaginationCaptureSink {
    events: RefCell<Vec<MetricsEvent>>,
}

impl PaginationCaptureSink {
    fn into_events(self) -> Vec<MetricsEvent> {
        self.events.into_inner()
    }
}

impl MetricsSink for PaginationCaptureSink {
    fn record(&self, event: MetricsEvent) {
        self.events.borrow_mut().push(event);
    }
}

fn setup_pagination_test() {
    reset_store();
}

fn rows_scanned_for_entity(events: &[MetricsEvent], entity_path: &'static str) -> usize {
    events.iter().fold(0usize, |acc, event| {
        let scanned = match event {
            MetricsEvent::RowsScanned {
                entity_path: path,
                rows_scanned,
            } if *path == entity_path => usize::try_from(*rows_scanned).unwrap_or(usize::MAX),
            _ => 0,
        };

        acc.saturating_add(scanned)
    })
}

fn capture_rows_scanned_for_entity<R>(
    entity_path: &'static str,
    run: impl FnOnce() -> R,
) -> (R, usize) {
    let sink = PaginationCaptureSink::default();
    let output = with_metrics_sink(&sink, run);
    let rows_scanned = rows_scanned_for_entity(&sink.into_events(), entity_path);

    (output, rows_scanned)
}

fn seed_simple_rows(ids: &[u128]) {
    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in ids {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(*id),
        })
        .expect("simple pagination seed save should succeed");
    }
}

fn build_scalar_limit_plan(
    access: AccessPlan<Ulid>,
    limit: u32,
    offset: u32,
) -> ExecutablePlan<SimpleEntity> {
    ExecutablePlan::new(AccessPlannedQuery {
        logical: LogicalPlan::Scalar(ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(limit),
                offset,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access: access.into_value_plan(),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
    })
}

fn build_simple_union_page_plan(
    left: Vec<Ulid>,
    right: Vec<Ulid>,
    descending: bool,
    limit: u32,
    offset: u32,
    predicate: Option<Predicate>,
) -> ExecutablePlan<SimpleEntity> {
    ExecutablePlan::new(AccessPlannedQuery {
        logical: LogicalPlan::Scalar(ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate,
            order: Some(OrderSpec {
                fields: vec![(
                    "id".to_string(),
                    if descending {
                        OrderDirection::Desc
                    } else {
                        OrderDirection::Asc
                    },
                )],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(limit),
                offset,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::ByKeys(left)),
            AccessPlan::path(AccessPath::ByKeys(right)),
        ])
        .into_value_plan(),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
    })
}

fn execute_page_ids_and_keys_scanned(
    load: &LoadExecutor<SimpleEntity>,
    plan: ExecutablePlan<SimpleEntity>,
) -> (Vec<Ulid>, usize) {
    let (page, trace) = load
        .execute_paged_with_cursor_traced(plan, None)
        .expect("paged trace execution should succeed");
    let keys_scanned = trace
        .map(|trace| trace.keys_scanned())
        .expect("traced execution should emit keys_scanned");
    let keys_scanned =
        usize::try_from(keys_scanned).expect("keys_scanned should fit within usize in test scope");
    let page_ids = page.items.iter().map(|row| row.entity_ref().id).collect();

    (page_ids, keys_scanned)
}

fn simple_ids_from_items(items: &crate::db::response::EntityResponse<SimpleEntity>) -> Vec<Ulid> {
    items.iter().map(|row| row.entity_ref().id).collect()
}

fn collect_simple_pages_from_executable_plan(
    load: &LoadExecutor<SimpleEntity>,
    build_plan: impl Fn() -> ExecutablePlan<SimpleEntity>,
    max_pages: usize,
) -> (Vec<Ulid>, Vec<CursorBoundary>) {
    let mut ids = Vec::new();
    let mut boundaries = Vec::new();
    let mut encoded_cursor = None::<Vec<u8>>;

    for _ in 0..max_pages {
        let boundary_plan = build_plan();
        let page = load
            .execute_paged_with_cursor(
                build_plan(),
                boundary_plan
                    .prepare_cursor(encoded_cursor.as_deref())
                    .expect("simple boundary should plan"),
            )
            .expect("simple page should execute");
        ids.extend(simple_ids_from_items(&page.items));

        let Some(cursor) = page.next_cursor else {
            break;
        };
        let scalar = cursor
            .as_scalar()
            .expect("simple pagination should stay on scalar cursors");
        boundaries.push(scalar.boundary().clone());
        encoded_cursor = Some(
            cursor
                .encode()
                .expect("simple continuation cursor should serialize"),
        );
    }

    (ids, boundaries)
}

fn seed_pushdown_rows(rows: &[(u128, u32, u32, &str)]) {
    let save = SaveExecutor::<PushdownParityEntity>::new(DB, false);
    for (id, group, rank, label) in rows {
        save.insert(PushdownParityEntity {
            id: Ulid::from_u128(*id),
            group: *group,
            rank: *rank,
            label: (*label).to_string(),
        })
        .expect("pushdown pagination seed save should succeed");
    }
}

fn pushdown_group_predicate(group: u32) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        "group",
        CompareOp::Eq,
        Value::Uint(u64::from(group)),
        CoercionId::Strict,
    ))
}

fn strict_compare_predicate(field: &str, op: CompareOp, value: Value) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        value,
        CoercionId::Strict,
    ))
}

fn group_rank_range_predicate(group: u32, lower_inclusive: u32, upper_exclusive: u32) -> Predicate {
    Predicate::And(vec![
        pushdown_group_predicate(group),
        strict_compare_predicate(
            "rank",
            CompareOp::Gte,
            Value::Uint(u64::from(lower_inclusive)),
        ),
        strict_compare_predicate(
            "rank",
            CompareOp::Lt,
            Value::Uint(u64::from(upper_exclusive)),
        ),
    ])
}

fn group_rank_between_equivalent_predicate(
    group: u32,
    lower_inclusive: u32,
    upper_inclusive: u32,
) -> Predicate {
    Predicate::And(vec![
        pushdown_group_predicate(group),
        strict_compare_predicate(
            "rank",
            CompareOp::Gte,
            Value::Uint(u64::from(lower_inclusive)),
        ),
        strict_compare_predicate(
            "rank",
            CompareOp::Lte,
            Value::Uint(u64::from(upper_inclusive)),
        ),
    ])
}

fn pushdown_group_ids(rows: &[(u128, u32, u32, &str)], group: u32) -> Vec<Ulid> {
    rows.iter()
        .filter(|(_, row_group, _, _)| *row_group == group)
        .map(|(id, _, _, _)| Ulid::from_u128(*id))
        .collect()
}

fn pushdown_ids_in_group_rank_range(
    rows: &[(u128, u32, u32, &str)],
    group: u32,
    lower_inclusive: u32,
    upper_exclusive: u32,
) -> Vec<Ulid> {
    rows.iter()
        .filter(|(_, row_group, rank, _)| {
            *row_group == group && *rank >= lower_inclusive && *rank < upper_exclusive
        })
        .map(|(id, _, _, _)| Ulid::from_u128(*id))
        .collect()
}

fn pushdown_ids_in_group_rank_between_equivalent_range(
    rows: &[(u128, u32, u32, &str)],
    group: u32,
    lower_inclusive: u32,
    upper_inclusive: u32,
) -> Vec<Ulid> {
    rows.iter()
        .filter(|(_, row_group, rank, _)| {
            *row_group == group && *rank >= lower_inclusive && *rank <= upper_inclusive
        })
        .map(|(id, _, _, _)| Ulid::from_u128(*id))
        .collect()
}

fn ordered_pushdown_group_ids(
    rows: &[(u128, u32, u32, &str)],
    group: u32,
    descending: bool,
) -> Vec<Ulid> {
    let mut ordered = rows
        .iter()
        .filter(|(_, row_group, _, _)| *row_group == group)
        .map(|(id, _, rank, _)| (*rank, Ulid::from_u128(*id)))
        .collect::<Vec<_>>();
    ordered.sort_by(|(left_rank, left_id), (right_rank, right_id)| {
        if descending {
            right_rank
                .cmp(left_rank)
                .then_with(|| left_id.cmp(right_id))
        } else {
            left_rank
                .cmp(right_rank)
                .then_with(|| left_id.cmp(right_id))
        }
    });

    ordered.into_iter().map(|(_, id)| id).collect()
}

fn pushdown_ids_from_response(response: &EntityResponse<PushdownParityEntity>) -> Vec<Ulid> {
    response.iter().map(|row| row.entity_ref().id).collect()
}

fn pushdown_trace_rows(prefix: u128) -> [(u128, u32, u32, &'static str); 5] {
    [
        (prefix + 1, 7, 10, "g7-r10"),
        (prefix + 2, 7, 20, "g7-r20-a"),
        (prefix + 3, 7, 20, "g7-r20-b"),
        (prefix + 4, 7, 30, "g7-r30"),
        (prefix + 5, 8, 15, "g8-r15"),
    ]
}

fn seed_indexed_metrics_rows(rows: &[(u128, u32, &str)]) {
    let save = SaveExecutor::<IndexedMetricsEntity>::new(DB, false);
    for (id, tag, label) in rows {
        save.insert(IndexedMetricsEntity {
            id: Ulid::from_u128(*id),
            tag: *tag,
            label: (*label).to_string(),
        })
        .expect("indexed metrics pagination seed save should succeed");
    }
}

fn seed_unique_index_range_rows(rows: &[(u128, u32, &str)]) {
    let save = SaveExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    for (id, code, label) in rows {
        save.insert(UniqueIndexRangeEntity {
            id: Ulid::from_u128(*id),
            code: *code,
            label: (*label).to_string(),
        })
        .expect("unique index-range pagination seed save should succeed");
    }
}

fn tag_range_predicate(lower_inclusive: u32, upper_exclusive: u32) -> Predicate {
    Predicate::And(vec![
        strict_compare_predicate(
            "tag",
            CompareOp::Gte,
            Value::Uint(u64::from(lower_inclusive)),
        ),
        strict_compare_predicate(
            "tag",
            CompareOp::Lt,
            Value::Uint(u64::from(upper_exclusive)),
        ),
    ])
}

fn tag_between_equivalent_predicate(lower_inclusive: u32, upper_inclusive: u32) -> Predicate {
    Predicate::And(vec![
        strict_compare_predicate(
            "tag",
            CompareOp::Gte,
            Value::Uint(u64::from(lower_inclusive)),
        ),
        strict_compare_predicate(
            "tag",
            CompareOp::Lte,
            Value::Uint(u64::from(upper_inclusive)),
        ),
    ])
}

fn indexed_metric_ids_from_response(response: &EntityResponse<IndexedMetricsEntity>) -> Vec<Ulid> {
    response.iter().map(|row| row.entity_ref().id).collect()
}

fn collect_indexed_metric_pages_from_executable_plan(
    load: &LoadExecutor<IndexedMetricsEntity>,
    build_plan: impl Fn() -> ExecutablePlan<IndexedMetricsEntity>,
    max_pages: usize,
) -> (Vec<Ulid>, Vec<CursorBoundary>) {
    let mut ids = Vec::new();
    let mut boundaries = Vec::new();
    let mut encoded_cursor = None::<Vec<u8>>;

    for _ in 0..max_pages {
        let boundary_plan = build_plan();
        let page = load
            .execute_paged_with_cursor(
                build_plan(),
                boundary_plan
                    .prepare_cursor(encoded_cursor.as_deref())
                    .expect("indexed metrics boundary should plan"),
            )
            .expect("indexed metrics page should execute");
        ids.extend(indexed_metric_ids_from_response(&page.items));

        let Some(cursor) = page.next_cursor else {
            break;
        };
        let scalar = cursor
            .as_scalar()
            .expect("indexed metrics pagination should stay on scalar cursors");
        boundaries.push(scalar.boundary().clone());
        encoded_cursor = Some(
            cursor
                .encode()
                .expect("indexed metrics continuation cursor should serialize"),
        );
    }

    (ids, boundaries)
}

fn collect_indexed_metric_pages_from_executable_plan_with_tokens(
    load: &LoadExecutor<IndexedMetricsEntity>,
    build_plan: impl Fn() -> ExecutablePlan<IndexedMetricsEntity>,
    max_pages: usize,
) -> (Vec<Ulid>, Vec<CursorBoundary>, Vec<Vec<u8>>) {
    let mut ids = Vec::new();
    let mut boundaries = Vec::new();
    let mut tokens = Vec::new();
    let mut encoded_cursor = None::<Vec<u8>>;

    for _ in 0..max_pages {
        let boundary_plan = build_plan();
        let page = load
            .execute_paged_with_cursor(
                build_plan(),
                boundary_plan
                    .prepare_cursor(encoded_cursor.as_deref())
                    .expect("indexed metrics boundary should plan"),
            )
            .expect("indexed metrics page should execute");
        ids.extend(indexed_metric_ids_from_response(&page.items));

        let Some(cursor) = page.next_cursor else {
            break;
        };
        let scalar = cursor
            .as_scalar()
            .expect("indexed metrics pagination should stay on scalar cursors");
        let token = cursor
            .encode()
            .expect("indexed metrics continuation cursor should serialize");
        boundaries.push(scalar.boundary().clone());
        tokens.push(token.clone());
        encoded_cursor = Some(token);
    }

    (ids, boundaries, tokens)
}

fn assert_indexed_metric_resume_suffixes_from_tokens(
    load: &LoadExecutor<IndexedMetricsEntity>,
    build_plan: &impl Fn() -> ExecutablePlan<IndexedMetricsEntity>,
    tokens: &[Vec<u8>],
    expected_ids: &[Ulid],
    context: &str,
) {
    for token in tokens {
        let page = load
            .execute_paged_with_cursor(
                build_plan(),
                build_plan()
                    .prepare_cursor(Some(token.as_slice()))
                    .expect("indexed metrics token resume should plan"),
            )
            .expect("indexed metrics token resume should execute");
        let resumed_ids = indexed_metric_ids_from_response(&page.items);
        let first_resumed_id = *resumed_ids
            .first()
            .expect("indexed metrics resumed page should contain at least one row");
        let expected_start = expected_ids
            .iter()
            .position(|id| *id == first_resumed_id)
            .expect("indexed metrics resumed id should exist in the expected baseline");
        assert_eq!(
            resumed_ids.as_slice(),
            &expected_ids[expected_start..expected_start.saturating_add(resumed_ids.len())],
            "{context}: resumed indexed-metrics page should preserve suffix order",
        );
    }
}

fn indexed_metrics_ids_in_tag_range(
    rows: &[(u128, u32, &str)],
    lower_inclusive: u32,
    upper_exclusive: u32,
) -> Vec<Ulid> {
    let mut ordered = rows
        .iter()
        .filter(|(_, tag, _)| *tag >= lower_inclusive && *tag < upper_exclusive)
        .map(|(id, tag, _)| (*tag, Ulid::from_u128(*id)))
        .collect::<Vec<_>>();
    ordered.sort_by(|(left_tag, left_id), (right_tag, right_id)| {
        left_tag.cmp(right_tag).then_with(|| left_id.cmp(right_id))
    });

    ordered.into_iter().map(|(_, id)| id).collect()
}

fn indexed_metrics_ids_in_between_equivalent_range(
    rows: &[(u128, u32, &str)],
    lower_inclusive: u32,
    upper_inclusive: u32,
) -> Vec<Ulid> {
    let mut ordered = rows
        .iter()
        .filter(|(_, tag, _)| *tag >= lower_inclusive && *tag <= upper_inclusive)
        .map(|(id, tag, _)| (*tag, Ulid::from_u128(*id)))
        .collect::<Vec<_>>();
    ordered.sort_by(|(left_tag, left_id), (right_tag, right_id)| {
        left_tag.cmp(right_tag).then_with(|| left_id.cmp(right_id))
    });

    ordered.into_iter().map(|(_, id)| id).collect()
}

fn ordered_index_candidate_ids_for_direction(
    rows: &[(u128, u32, &str)],
    lower_inclusive: u32,
    upper_exclusive: u32,
    direction: OrderDirection,
) -> Vec<Ulid> {
    let mut ordered = rows
        .iter()
        .filter(|(_, tag, _)| *tag >= lower_inclusive && *tag < upper_exclusive)
        .map(|(id, tag, _)| (*tag, Ulid::from_u128(*id)))
        .collect::<Vec<_>>();
    ordered.sort_by(
        |(left_tag, left_id), (right_tag, right_id)| match direction {
            OrderDirection::Asc => left_tag.cmp(right_tag).then_with(|| left_id.cmp(right_id)),
            OrderDirection::Desc => right_tag.cmp(left_tag).then_with(|| right_id.cmp(left_id)),
        },
    );

    ordered.into_iter().map(|(_, id)| id).collect()
}

fn build_distinct_desc_index_range_plan(
    limit: u32,
    offset: u32,
) -> ExecutablePlan<IndexedMetricsEntity> {
    ExecutablePlan::<IndexedMetricsEntity>::new(AccessPlannedQuery {
        logical: LogicalPlan::Scalar(ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![
                    ("tag".to_string(), OrderDirection::Desc),
                    ("id".to_string(), OrderDirection::Desc),
                ],
            }),
            distinct: true,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(limit),
                offset,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::index_range(
            INDEXED_METRICS_INDEX_MODELS[0],
            Vec::new(),
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        )),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
    })
}

fn build_distinct_secondary_offset_fast_plan(
    direction: OrderDirection,
    predicate: Predicate,
) -> ExecutablePlan<PushdownParityEntity> {
    let base = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .distinct()
        .limit(2)
        .offset(1);
    let ordered = match direction {
        OrderDirection::Asc => base.order_by("rank").order_by("id"),
        OrderDirection::Desc => base.order_by_desc("rank").order_by_desc("id"),
    };

    ordered
        .plan()
        .map(ExecutablePlan::from)
        .expect("distinct secondary offset fast-path plan should build")
}

fn build_distinct_secondary_offset_fallback_plan(
    direction: OrderDirection,
    ids: &[Ulid],
) -> ExecutablePlan<PushdownParityEntity> {
    let base = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .by_ids(ids.iter().copied())
        .distinct()
        .limit(2)
        .offset(1);
    let ordered = match direction {
        OrderDirection::Asc => base.order_by("rank").order_by("id"),
        OrderDirection::Desc => base.order_by_desc("rank").order_by_desc("id"),
    };

    ordered
        .plan()
        .map(ExecutablePlan::from)
        .expect("distinct secondary offset fallback plan should build")
}

fn build_distinct_index_range_offset_fast_plan(
    direction: OrderDirection,
) -> ExecutablePlan<IndexedMetricsEntity> {
    ExecutablePlan::<IndexedMetricsEntity>::new(AccessPlannedQuery {
        logical: LogicalPlan::Scalar(ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![
                    ("tag".to_string(), direction),
                    ("id".to_string(), direction),
                ],
            }),
            distinct: true,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(2),
                offset: 1,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::index_range(
            INDEXED_METRICS_INDEX_MODELS[0],
            Vec::new(),
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        )),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
    })
}

fn build_distinct_index_range_offset_fallback_plan(
    direction: OrderDirection,
    ids: &[Ulid],
) -> ExecutablePlan<IndexedMetricsEntity> {
    let base = Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
        .by_ids(ids.iter().copied())
        .distinct()
        .limit(2)
        .offset(1);
    let ordered = match direction {
        OrderDirection::Asc => base.order_by("tag").order_by("id"),
        OrderDirection::Desc => base.order_by_desc("tag").order_by_desc("id"),
    };

    ordered
        .plan()
        .map(ExecutablePlan::from)
        .expect("distinct index-range offset fallback plan should build")
}

fn assert_distinct_secondary_offset_parity_case(
    load: &LoadExecutor<PushdownParityEntity>,
    predicate: Predicate,
    group_ids: &[Ulid],
    direction: OrderDirection,
    case_name: &str,
) {
    let build_fast_plan =
        || build_distinct_secondary_offset_fast_plan(direction, predicate.clone());
    let build_fallback_plan =
        || build_distinct_secondary_offset_fallback_plan(direction, group_ids);

    let (_seed_fast, trace_fast) = load
        .execute_paged_with_cursor_traced(build_fast_plan(), None)
        .expect("distinct secondary offset fast-path seed should execute");
    let trace_fast = trace_fast.expect("debug trace should be present");
    assert_eq!(
        trace_fast.optimization(),
        None,
        "distinct secondary offset residual-filter path should remain materialized for case={case_name}",
    );

    let (_seed_fallback, trace_fallback) = load
        .execute_paged_with_cursor_traced(build_fallback_plan(), None)
        .expect("distinct secondary offset fallback seed should execute");
    let trace_fallback = trace_fallback.expect("debug trace should be present");
    assert_eq!(
        trace_fallback.optimization(),
        None,
        "distinct secondary offset fallback should remain non-optimized for case={case_name}",
    );

    let (fast_ids, fast_boundaries, fast_tokens) =
        collect_pushdown_pages_from_executable_plan_with_tokens(load, build_fast_plan, 20);
    let (fallback_ids, fallback_boundaries) =
        collect_pushdown_pages_from_executable_plan(load, build_fallback_plan, 20);
    assert_eq!(
        fast_ids, fallback_ids,
        "distinct secondary offset fast/fallback ids should match for case={case_name}",
    );
    assert_eq!(
        fast_boundaries, fallback_boundaries,
        "distinct secondary offset fast/fallback boundaries should match for case={case_name}",
    );
    assert_pushdown_resume_suffixes_from_tokens(
        load,
        &build_fast_plan,
        &fast_tokens,
        &fast_ids,
        case_name,
    );
}

fn assert_distinct_index_range_offset_parity_case(
    load: &LoadExecutor<IndexedMetricsEntity>,
    rows: &[(u128, u32, &str)],
    direction: OrderDirection,
    case_name: &str,
) {
    let build_fast_plan = || build_distinct_index_range_offset_fast_plan(direction);
    let candidate_ids = ordered_index_candidate_ids_for_direction(rows, 10, 30, direction);
    let build_fallback_plan =
        || build_distinct_index_range_offset_fallback_plan(direction, &candidate_ids);

    let (_seed_fast, trace_fast) = load
        .execute_paged_with_cursor_traced(build_fast_plan(), None)
        .expect("distinct index-range offset fast-path seed should execute");
    let trace_fast = trace_fast.expect("debug trace should be present");
    assert_eq!(
        trace_fast.optimization(),
        Some(crate::db::diagnostics::ExecutionOptimization::IndexRangeLimitPushdown),
        "distinct index-range offset fast path should use limit pushdown for case={case_name}",
    );

    let (_seed_fallback, trace_fallback) = load
        .execute_paged_with_cursor_traced(build_fallback_plan(), None)
        .expect("distinct index-range offset fallback seed should execute");
    let trace_fallback = trace_fallback.expect("debug trace should be present");
    assert_eq!(
        trace_fallback.optimization(),
        None,
        "distinct index-range offset fallback should remain non-optimized for case={case_name}",
    );

    let (fast_ids, fast_boundaries, fast_tokens) =
        collect_indexed_metric_pages_from_executable_plan_with_tokens(load, build_fast_plan, 20);
    let (fallback_ids, fallback_boundaries) =
        collect_indexed_metric_pages_from_executable_plan(load, build_fallback_plan, 20);
    assert_eq!(
        fast_ids, fallback_ids,
        "distinct index-range offset fast/fallback ids should match for case={case_name}",
    );
    assert_eq!(
        fast_boundaries, fallback_boundaries,
        "distinct index-range offset fast/fallback boundaries should match for case={case_name}",
    );
    assert_indexed_metric_resume_suffixes_from_tokens(
        load,
        &build_fast_plan,
        &fast_tokens,
        &fast_ids,
        case_name,
    );
}

fn indexed_metric_tag_id_boundary(tag: u32, id: u128) -> CursorBoundary {
    CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Uint(u64::from(tag))),
            CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(id))),
        ],
    }
}

fn unique_code_range_predicate(lower_inclusive: u32, upper_exclusive: u32) -> Predicate {
    Predicate::And(vec![
        strict_compare_predicate(
            "code",
            CompareOp::Gte,
            Value::Uint(u64::from(lower_inclusive)),
        ),
        strict_compare_predicate(
            "code",
            CompareOp::Lt,
            Value::Uint(u64::from(upper_exclusive)),
        ),
    ])
}

fn unique_index_range_ids_from_response(
    response: &EntityResponse<UniqueIndexRangeEntity>,
) -> Vec<Ulid> {
    response.iter().map(|row| row.entity_ref().id).collect()
}

fn execute_unique_index_range_code_page_asc(
    load: &LoadExecutor<UniqueIndexRangeEntity>,
    predicate: Predicate,
    limit: u32,
    encoded_cursor: Option<&[u8]>,
    context: &'static str,
) -> CursorPage<UniqueIndexRangeEntity> {
    let plan = Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("code")
        .limit(limit)
        .plan()
        .map_or_else(
            |_| panic!("{context} plan should build"),
            ExecutablePlan::from,
        );
    let boundary = plan
        .prepare_cursor(encoded_cursor)
        .unwrap_or_else(|_| panic!("{context} boundary should plan"));

    load.execute_paged_with_cursor(plan, boundary)
        .unwrap_or_else(|_| panic!("{context} should execute"))
}

type RangeBounds = &'static [(CompareOp, u32)];

///
/// RangeMatrixCase
///
/// Small owner-local table row used by the remaining range parity loops after
/// draining the stale matrix wrapper file.
///

struct RangeMatrixCase {
    name: &'static str,
    bounds: RangeBounds,
    descending: bool,
}

fn ordered_indexed_metrics_ids_for_bounds(
    rows: &[(u128, u32, &str)],
    bounds: RangeBounds,
    descending: bool,
) -> Vec<Ulid> {
    let mut ordered = rows
        .iter()
        .filter(|(_, tag, _)| {
            bounds.iter().all(|(op, value)| match op {
                CompareOp::Eq => *tag == *value,
                CompareOp::Gt => *tag > *value,
                CompareOp::Gte => *tag >= *value,
                CompareOp::Lt => *tag < *value,
                CompareOp::Lte => *tag <= *value,
                _ => false,
            })
        })
        .map(|(id, tag, _)| (*tag, Ulid::from_u128(*id)))
        .collect::<Vec<_>>();
    ordered.sort_by(|(left_tag, left_id), (right_tag, right_id)| {
        if descending {
            right_tag.cmp(left_tag).then_with(|| left_id.cmp(right_id))
        } else {
            left_tag.cmp(right_tag).then_with(|| left_id.cmp(right_id))
        }
    });

    ordered.into_iter().map(|(_, id)| id).collect()
}

fn ordered_pushdown_ids_for_group_rank_bounds(
    rows: &[(u128, u32, u32, &str)],
    group: u32,
    bounds: RangeBounds,
    descending: bool,
) -> Vec<Ulid> {
    let mut ordered = rows
        .iter()
        .filter(|(_, row_group, rank, _)| {
            *row_group == group
                && bounds.iter().all(|(op, value)| match op {
                    CompareOp::Eq => *rank == *value,
                    CompareOp::Gt => *rank > *value,
                    CompareOp::Gte => *rank >= *value,
                    CompareOp::Lt => *rank < *value,
                    CompareOp::Lte => *rank <= *value,
                    _ => false,
                })
        })
        .map(|(id, _, rank, _)| (*rank, Ulid::from_u128(*id)))
        .collect::<Vec<_>>();
    ordered.sort_by(|(left_rank, left_id), (right_rank, right_id)| {
        if descending {
            right_rank
                .cmp(left_rank)
                .then_with(|| left_id.cmp(right_id))
        } else {
            left_rank
                .cmp(right_rank)
                .then_with(|| left_id.cmp(right_id))
        }
    });

    ordered.into_iter().map(|(_, id)| id).collect()
}

fn predicate_from_field_bounds(field: &str, bounds: RangeBounds) -> Predicate {
    Predicate::And(
        bounds
            .iter()
            .map(|(op, value)| strict_compare_predicate(field, *op, Value::Uint(u64::from(*value))))
            .collect(),
    )
}

fn predicate_from_group_rank_bounds(group: u32, bounds: RangeBounds) -> Predicate {
    let mut predicates = Vec::with_capacity(bounds.len().saturating_add(1));
    predicates.push(pushdown_group_predicate(group));
    predicates.extend(
        bounds.iter().map(|(op, value)| {
            strict_compare_predicate("rank", *op, Value::Uint(u64::from(*value)))
        }),
    );

    Predicate::And(predicates)
}

fn encode_token(cursor: &PageCursor, context: &'static str) -> Vec<u8> {
    cursor.encode().unwrap_or_else(|_| panic!("{context}"))
}

fn assert_anchor_monotonic(
    anchors: &mut Vec<Vec<u8>>,
    cursor_bytes: &[u8],
    decode_context: &'static str,
    missing_anchor_context: &'static str,
    order_context: &'static str,
) {
    let decoded =
        ContinuationToken::decode(cursor_bytes).unwrap_or_else(|_| panic!("{decode_context}"));
    let anchor = decoded.index_range_anchor().map_or_else(
        || panic!("{missing_anchor_context}"),
        |anchor| anchor.last_raw_key().to_vec(),
    );
    if let Some(previous) = anchors.last() {
        assert!(previous < &anchor, "{order_context}");
    }
    anchors.push(anchor);
}

fn assert_unique_range_resume_suffixes_from_tokens(
    load: &LoadExecutor<UniqueIndexRangeEntity>,
    predicate: Predicate,
    limit: u32,
    tokens: &[Vec<u8>],
    expected_ids: &[Ulid],
) {
    for token in tokens {
        let page = execute_unique_index_range_code_page_asc(
            load,
            predicate.clone(),
            limit,
            Some(token.as_slice()),
            "unique range token resume",
        );
        let resumed_ids = unique_index_range_ids_from_response(&page.items);
        let first_resumed_id = *resumed_ids
            .first()
            .expect("resumed unique range page should contain at least one row");
        let expected_start = expected_ids
            .iter()
            .position(|id| *id == first_resumed_id)
            .expect("resumed id should exist in the unbounded baseline");
        assert_eq!(
            resumed_ids.as_slice(),
            &expected_ids[expected_start..expected_start.saturating_add(resumed_ids.len())],
            "resumed unique range page should preserve suffix order from the unbounded baseline",
        );
    }
}

fn execute_indexed_metrics_tag_page_desc(
    load: &LoadExecutor<IndexedMetricsEntity>,
    predicate: Predicate,
    limit: u32,
    encoded_cursor: Option<&[u8]>,
    context: &'static str,
) -> CursorPage<IndexedMetricsEntity> {
    let plan = Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by_desc("tag")
        .limit(limit)
        .plan()
        .map_or_else(
            |_| panic!("{context} plan should build"),
            ExecutablePlan::from,
        );
    let boundary = plan
        .prepare_cursor(encoded_cursor)
        .unwrap_or_else(|_| panic!("{context} boundary should plan"));

    load.execute_paged_with_cursor(plan, boundary)
        .unwrap_or_else(|_| panic!("{context} should execute"))
}

fn execute_indexed_metrics_tag_page_desc_from_boundary(
    load: &LoadExecutor<IndexedMetricsEntity>,
    predicate: Predicate,
    limit: u32,
    boundary: Option<CursorBoundary>,
    context: &'static str,
) -> CursorPage<IndexedMetricsEntity> {
    let plan = Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by_desc("tag")
        .limit(limit)
        .plan()
        .map_or_else(
            |_| panic!("{context} plan should build"),
            ExecutablePlan::from,
        );

    load.execute_paged_with_cursor(plan, boundary)
        .unwrap_or_else(|_| panic!("{context} should execute"))
}

fn execute_indexed_metrics_tag_page_asc_from_boundary(
    load: &LoadExecutor<IndexedMetricsEntity>,
    predicate: Predicate,
    limit: u32,
    boundary: Option<CursorBoundary>,
    context: &'static str,
) -> CursorPage<IndexedMetricsEntity> {
    let plan = Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("tag")
        .limit(limit)
        .plan()
        .map_or_else(
            |_| panic!("{context} plan should build"),
            ExecutablePlan::from,
        );

    load.execute_paged_with_cursor(plan, boundary)
        .unwrap_or_else(|_| panic!("{context} should execute"))
}

fn pushdown_rank_id_boundary(rank: u32, id: u128) -> CursorBoundary {
    CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Uint(u64::from(rank))),
            CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(id))),
        ],
    }
}

fn execute_pushdown_rank_page_asc(
    load: &LoadExecutor<PushdownParityEntity>,
    predicate: Predicate,
    boundary: Option<CursorBoundary>,
    context: &'static str,
) -> CursorPage<PushdownParityEntity> {
    load.execute_paged_with_cursor(
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(predicate)
            .order_by("rank")
            .limit(10)
            .plan()
            .map_or_else(
                |_| panic!("{context} plan should build"),
                ExecutablePlan::from,
            ),
        boundary,
    )
    .unwrap_or_else(|_| panic!("{context} should execute"))
}

fn execute_pushdown_rank_page_desc(
    load: &LoadExecutor<PushdownParityEntity>,
    predicate: Predicate,
    boundary: Option<CursorBoundary>,
    context: &'static str,
) -> CursorPage<PushdownParityEntity> {
    load.execute_paged_with_cursor(
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(predicate)
            .order_by_desc("rank")
            .limit(10)
            .plan()
            .map_or_else(
                |_| panic!("{context} plan should build"),
                ExecutablePlan::from,
            ),
        boundary,
    )
    .unwrap_or_else(|_| panic!("{context} should execute"))
}

fn collect_pushdown_paged_ids(
    load: &LoadExecutor<PushdownParityEntity>,
    build_plan: impl Fn() -> ExecutablePlan<PushdownParityEntity>,
    max_pages: usize,
) -> Vec<Ulid> {
    let mut ids = Vec::new();
    let mut encoded_cursor = None::<Vec<u8>>;

    for _ in 0..max_pages {
        let boundary_plan = build_plan();
        let page = load
            .execute_paged_with_cursor(
                build_plan(),
                boundary_plan
                    .prepare_cursor(encoded_cursor.as_deref())
                    .expect("pushdown pagination cursor should plan"),
            )
            .expect("pushdown pagination page should execute");
        ids.extend(pushdown_ids_from_response(&page.items));

        let Some(cursor) = page.next_cursor else {
            break;
        };
        encoded_cursor = Some(
            cursor
                .encode()
                .expect("pushdown pagination cursor should serialize"),
        );
    }

    ids
}

fn collect_pushdown_pages_from_executable_plan(
    load: &LoadExecutor<PushdownParityEntity>,
    build_plan: impl Fn() -> ExecutablePlan<PushdownParityEntity>,
    max_pages: usize,
) -> (Vec<Ulid>, Vec<CursorBoundary>) {
    let mut ids = Vec::new();
    let mut boundaries = Vec::new();
    let mut encoded_cursor = None::<Vec<u8>>;

    for _ in 0..max_pages {
        let boundary_plan = build_plan();
        let page = load
            .execute_paged_with_cursor(
                build_plan(),
                boundary_plan
                    .prepare_cursor(encoded_cursor.as_deref())
                    .expect("pushdown boundary should plan"),
            )
            .expect("pushdown page should execute");
        ids.extend(pushdown_ids_from_response(&page.items));

        let Some(cursor) = page.next_cursor else {
            break;
        };
        let scalar = cursor
            .as_scalar()
            .expect("pushdown pagination should stay on scalar cursors");
        boundaries.push(scalar.boundary().clone());
        encoded_cursor = Some(
            cursor
                .encode()
                .expect("pushdown continuation cursor should serialize"),
        );
    }

    (ids, boundaries)
}

fn collect_pushdown_pages_from_executable_plan_with_tokens(
    load: &LoadExecutor<PushdownParityEntity>,
    build_plan: impl Fn() -> ExecutablePlan<PushdownParityEntity>,
    max_pages: usize,
) -> (Vec<Ulid>, Vec<CursorBoundary>, Vec<Vec<u8>>) {
    let mut ids = Vec::new();
    let mut boundaries = Vec::new();
    let mut tokens = Vec::new();
    let mut encoded_cursor = None::<Vec<u8>>;

    for _ in 0..max_pages {
        let boundary_plan = build_plan();
        let page = load
            .execute_paged_with_cursor(
                build_plan(),
                boundary_plan
                    .prepare_cursor(encoded_cursor.as_deref())
                    .expect("pushdown boundary should plan"),
            )
            .expect("pushdown page should execute");
        ids.extend(pushdown_ids_from_response(&page.items));

        let Some(cursor) = page.next_cursor else {
            break;
        };
        let scalar = cursor
            .as_scalar()
            .expect("pushdown pagination should stay on scalar cursors");
        let token = cursor
            .encode()
            .expect("pushdown continuation cursor should serialize");
        boundaries.push(scalar.boundary().clone());
        tokens.push(token.clone());
        encoded_cursor = Some(token);
    }

    (ids, boundaries, tokens)
}

fn assert_pushdown_resume_suffixes_from_boundaries(
    load: &LoadExecutor<PushdownParityEntity>,
    build_plan: &impl Fn() -> ExecutablePlan<PushdownParityEntity>,
    boundaries: &[CursorBoundary],
    expected_ids: &[Ulid],
    context: &str,
) {
    for boundary in boundaries {
        let page = load
            .execute_paged_with_cursor(build_plan(), Some(boundary.clone()))
            .expect("pushdown boundary resume should execute");
        let resumed_ids = pushdown_ids_from_response(&page.items);
        let first_resumed_id = *resumed_ids
            .first()
            .expect("pushdown resumed page should contain at least one row");
        let expected_start = expected_ids
            .iter()
            .position(|id| *id == first_resumed_id)
            .expect("pushdown resumed id should exist in the expected baseline");
        assert_eq!(
            resumed_ids.as_slice(),
            &expected_ids[expected_start..expected_start.saturating_add(resumed_ids.len())],
            "{context}: resumed pushdown page should preserve suffix order",
        );
    }
}

fn assert_pushdown_resume_suffixes_from_tokens(
    load: &LoadExecutor<PushdownParityEntity>,
    build_plan: &impl Fn() -> ExecutablePlan<PushdownParityEntity>,
    tokens: &[Vec<u8>],
    expected_ids: &[Ulid],
    context: &str,
) {
    for token in tokens {
        let page = load
            .execute_paged_with_cursor(
                build_plan(),
                build_plan()
                    .prepare_cursor(Some(token.as_slice()))
                    .expect("pushdown token resume should plan"),
            )
            .expect("pushdown token resume should execute");
        let resumed_ids = pushdown_ids_from_response(&page.items);
        let first_resumed_id = *resumed_ids
            .first()
            .expect("pushdown resumed page should contain at least one row");
        let expected_start = expected_ids
            .iter()
            .position(|id| *id == first_resumed_id)
            .expect("pushdown resumed id should exist in the expected baseline");
        assert_eq!(
            resumed_ids.as_slice(),
            &expected_ids[expected_start..expected_start.saturating_add(resumed_ids.len())],
            "{context}: resumed pushdown page should preserve suffix order",
        );
    }
}

fn ordered_pushdown_ids_with_rank_and_id_direction(
    rows: &[(u128, u32, u32, &str)],
    group: u32,
    rank_desc: bool,
    id_desc: bool,
) -> Vec<Ulid> {
    let mut ordered = rows
        .iter()
        .filter(|(_, row_group, _, _)| *row_group == group)
        .map(|(id, _, rank, _)| (*rank, Ulid::from_u128(*id)))
        .collect::<Vec<_>>();
    ordered.sort_by(|(left_rank, left_id), (right_rank, right_id)| {
        let rank_cmp = if rank_desc {
            right_rank.cmp(left_rank)
        } else {
            left_rank.cmp(right_rank)
        };

        rank_cmp.then_with(|| {
            if id_desc {
                right_id.cmp(left_id)
            } else {
                left_id.cmp(right_id)
            }
        })
    });

    ordered.into_iter().map(|(_, id)| id).collect()
}

fn build_simple_ordered_page_plan(
    descending: bool,
    limit: u32,
    offset: u32,
) -> ExecutablePlan<SimpleEntity> {
    let query = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .limit(limit)
        .offset(offset);
    let query = if descending {
        query.order_by_desc("id")
    } else {
        query.order_by("id")
    };

    query
        .plan()
        .map(ExecutablePlan::from)
        .expect("simple ordered pagination plan should build")
}

fn build_simple_by_ids_ordered_page_plan(
    ids: impl IntoIterator<Item = Ulid>,
    descending: bool,
    limit: u32,
    offset: u32,
) -> ExecutablePlan<SimpleEntity> {
    let query = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .by_ids(ids)
        .limit(limit)
        .offset(offset);
    let query = if descending {
        query.order_by_desc("id")
    } else {
        query.order_by("id")
    };

    query
        .plan()
        .map(ExecutablePlan::from)
        .expect("simple by-ids ordered pagination plan should build")
}

#[test]
fn load_limit_without_cursor_applies_scan_budget_across_primary_access_shapes() {
    setup_pagination_test();
    seed_simple_rows(&[41_001, 41_002, 41_003, 41_004, 41_005, 41_006]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, true);

    // Phase 1: by-key access should preserve the requested row and bounded scan work.
    let single_lookup_id = Ulid::from_u128(41_003);
    let single_lookup_plan =
        build_scalar_limit_plan(AccessPlan::path(AccessPath::ByKey(single_lookup_id)), 1, 0);
    let (single_lookup_ids, single_lookup_scanned) =
        execute_page_ids_and_keys_scanned(&load, single_lookup_plan);
    assert_eq!(
        single_lookup_ids,
        vec![single_lookup_id],
        "ByKey should return the selected id",
    );
    assert!(
        single_lookup_scanned <= 2,
        "ByKey limit window should keep scan budget bounded (keys_scanned={single_lookup_scanned})",
    );

    // Phase 2: multi-key access should stop after the offset+limit(+1) window.
    let multi_lookup_plan = build_scalar_limit_plan(
        AccessPlan::path(AccessPath::ByKeys(vec![
            Ulid::from_u128(41_006),
            Ulid::from_u128(41_002),
            Ulid::from_u128(41_004),
            Ulid::from_u128(41_001),
            Ulid::from_u128(41_005),
        ])),
        2,
        1,
    );
    let (multi_lookup_ids, multi_lookup_scanned) =
        execute_page_ids_and_keys_scanned(&load, multi_lookup_plan);
    assert_eq!(
        multi_lookup_ids,
        vec![Ulid::from_u128(41_002), Ulid::from_u128(41_004)],
        "ByKeys pagination should preserve canonical ordered offset/limit rows",
    );
    assert!(
        multi_lookup_scanned <= 4,
        "ByKeys limit window should cap scanned keys at offset+limit+1 (keys_scanned={multi_lookup_scanned})",
    );

    // Phase 3: range access should apply the same no-cursor budget.
    let range_scan_plan = build_scalar_limit_plan(
        AccessPlan::path(AccessPath::KeyRange {
            start: Ulid::from_u128(41_002),
            end: Ulid::from_u128(41_005),
        }),
        2,
        1,
    );
    let (range_scan_ids, range_scan_scanned) =
        execute_page_ids_and_keys_scanned(&load, range_scan_plan);
    assert_eq!(
        range_scan_ids,
        vec![Ulid::from_u128(41_003), Ulid::from_u128(41_004)],
        "KeyRange pagination should preserve canonical ordered offset/limit rows",
    );
    assert!(
        range_scan_scanned <= 4,
        "KeyRange limit window should cap scanned keys at offset+limit+1 (keys_scanned={range_scan_scanned})",
    );

    // Phase 4: full scan should still honor the same requested page window.
    let full_scan_window_plan =
        build_scalar_limit_plan(AccessPlan::path(AccessPath::FullScan), 2, 1);
    let (full_scan_window_ids, full_scan_window_scanned) =
        execute_page_ids_and_keys_scanned(&load, full_scan_window_plan);
    assert_eq!(
        full_scan_window_ids,
        vec![Ulid::from_u128(41_002), Ulid::from_u128(41_003)],
        "FullScan pagination should preserve canonical ordered offset/limit rows",
    );
    assert!(
        full_scan_window_scanned >= full_scan_window_ids.len(),
        "FullScan tracing should report at least the emitted row count (keys_scanned={full_scan_window_scanned})",
    );
}

#[test]
fn load_execute_order_by_limit_one_seeks_single_row_without_cursor() {
    setup_pagination_test();
    seed_simple_rows(&[41_101, 41_102, 41_103, 41_104]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let plan = build_scalar_limit_plan(AccessPlan::path(AccessPath::FullScan), 1, 0);
    let (response, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.execute(plan)
            .expect("unpaged ORDER BY LIMIT 1 execution should succeed")
    });
    let response_ids: Vec<Ulid> = response.iter().map(|row| row.entity_ref().id).collect();

    assert_eq!(
        response_ids,
        vec![Ulid::from_u128(41_101)],
        "unpaged ORDER BY LIMIT 1 should return the first ordered row",
    );
    assert_eq!(
        scanned, 1,
        "unpaged ORDER BY LIMIT 1 should scan exactly one row under seek hint",
    );
}

#[test]
fn load_execute_order_by_limit_window_uses_keep_count_seek_budget_without_cursor() {
    setup_pagination_test();
    seed_simple_rows(&[41_201, 41_202, 41_203, 41_204, 41_205, 41_206]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let plan = build_scalar_limit_plan(AccessPlan::path(AccessPath::FullScan), 3, 1);
    let (response, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.execute(plan)
            .expect("unpaged ORDER BY LIMIT window execution should succeed")
    });
    let response_ids: Vec<Ulid> = response.iter().map(|row| row.entity_ref().id).collect();

    assert_eq!(
        response_ids,
        vec![
            Ulid::from_u128(41_202),
            Ulid::from_u128(41_203),
            Ulid::from_u128(41_204),
        ],
        "unpaged ORDER BY LIMIT window should preserve offset/limit ordering semantics",
    );
    assert_eq!(
        scanned, 4,
        "unpaged ORDER BY LIMIT window should scan keep-count rows (offset+limit) without continuation lookahead",
    );
}

#[test]
fn load_applies_order_and_pagination() {
    setup_pagination_test();
    seed_simple_rows(&[3, 1, 2]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = build_simple_ordered_page_plan(false, 1, 1);
    let response = load.execute(plan).expect("load should succeed");

    assert_eq!(response.len(), 1, "pagination should return one row");
    assert_eq!(
        response[0].entity_ref().id,
        Ulid::from_u128(2),
        "pagination should run after canonical ordering by id",
    );
}

#[test]
fn load_offset_pagination_preserves_next_cursor_boundary() {
    setup_pagination_test();
    seed_simple_rows(&[5, 1, 4, 2, 3]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let page_plan = build_simple_ordered_page_plan(false, 2, 1);
    let page_boundary = page_plan
        .prepare_cursor(None)
        .expect("offset page boundary should plan");
    let page = load
        .execute_paged_with_cursor(page_plan, page_boundary)
        .expect("offset page should execute");

    let page_ids = simple_ids_from_items(&page.items);
    assert_eq!(
        page_ids,
        vec![Ulid::from_u128(2), Ulid::from_u128(3)],
        "offset pagination should return canonical ordered window",
    );

    let token = page
        .next_cursor
        .as_ref()
        .expect("offset page should emit continuation cursor");
    let expected_boundary = crate::db::cursor::CursorBoundary {
        slots: vec![crate::db::cursor::CursorBoundarySlot::Present(Value::Ulid(
            page.items[1].entity_ref().id,
        ))],
    };
    assert_eq!(
        token
            .as_scalar()
            .expect("page cursor should stay scalar")
            .boundary(),
        &expected_boundary,
        "next cursor must encode the last returned row for offset pages",
    );
}

#[test]
fn load_cursor_with_offset_applies_offset_once_across_pages() {
    setup_pagination_test();
    seed_simple_rows(&[6, 1, 5, 2, 4, 3]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    // Phase 1: first page consumes offset before applying limit.
    let page1_plan = build_simple_ordered_page_plan(false, 2, 1);
    let page1_boundary = page1_plan
        .prepare_cursor(None)
        .expect("offset page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("offset page1 should execute");
    assert_eq!(
        simple_ids_from_items(&page1.items),
        vec![Ulid::from_u128(2), Ulid::from_u128(3)],
        "first page should apply offset once",
    );

    // Phase 2: continuation resumes from cursor boundary without re-applying offset.
    let cursor = page1
        .next_cursor
        .expect("first page should emit continuation cursor");
    let page2_plan = build_simple_ordered_page_plan(false, 2, 1);
    let page2_boundary = page2_plan
        .prepare_cursor(Some(
            cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("offset page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("offset page2 should execute");
    assert_eq!(
        simple_ids_from_items(&page2.items),
        vec![Ulid::from_u128(4), Ulid::from_u128(5)],
        "continuation page should not re-apply offset",
    );
}

#[test]
fn load_cursor_pagination_pk_order_round_trips_across_pages() {
    setup_pagination_test();
    seed_simple_rows(&[4, 1, 3, 2]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let page1_plan = build_simple_ordered_page_plan(false, 2, 0);
    let page1_boundary = page1_plan
        .prepare_cursor(None)
        .expect("pk-order page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("pk-order page1 should execute");
    assert_eq!(
        simple_ids_from_items(&page1.items),
        vec![Ulid::from_u128(1), Ulid::from_u128(2)],
    );

    let cursor = page1
        .next_cursor
        .as_ref()
        .expect("pk-order page1 should emit continuation cursor");
    let page2_plan = build_simple_ordered_page_plan(false, 2, 0);
    let page2_boundary = page2_plan
        .prepare_cursor(Some(
            cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("pk-order page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("pk-order page2 should execute");
    assert_eq!(
        simple_ids_from_items(&page2.items),
        vec![Ulid::from_u128(3), Ulid::from_u128(4)],
    );
    assert!(
        page2.next_cursor.is_none(),
        "final pk-order page should not emit continuation cursor",
    );
}

#[test]
fn load_cursor_pagination_pk_fast_path_matches_non_fast_post_access_semantics() {
    setup_pagination_test();

    let keys = [5_u128, 1_u128, 4_u128, 2_u128, 3_u128];
    seed_simple_rows(&keys);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    // Phase 1: compare fast and non-fast first pages.
    let fast_page1_plan = build_simple_ordered_page_plan(false, 2, 1);
    let fast_page1_boundary = fast_page1_plan
        .prepare_cursor(None)
        .expect("fast page1 boundary should plan");
    let fast_page1 = load
        .execute_paged_with_cursor(fast_page1_plan, fast_page1_boundary)
        .expect("fast page1 should execute");

    let non_fast_page1_plan =
        build_simple_by_ids_ordered_page_plan(keys.into_iter().map(Ulid::from_u128), false, 2, 1);
    let non_fast_page1_boundary = non_fast_page1_plan
        .prepare_cursor(None)
        .expect("non-fast page1 boundary should plan");
    let non_fast_page1 = load
        .execute_paged_with_cursor(non_fast_page1_plan, non_fast_page1_boundary)
        .expect("non-fast page1 should execute");

    assert_eq!(
        simple_ids_from_items(&fast_page1.items),
        simple_ids_from_items(&non_fast_page1.items),
        "page1 rows should match between fast and non-fast access paths",
    );
    assert_eq!(
        fast_page1.next_cursor.is_some(),
        non_fast_page1.next_cursor.is_some(),
        "page1 cursor presence should match between paths",
    );

    let fast_cursor_page1 = fast_page1
        .next_cursor
        .as_ref()
        .expect("fast page1 should emit continuation cursor");
    let non_fast_cursor_page1 = non_fast_page1
        .next_cursor
        .as_ref()
        .expect("non-fast page1 should emit continuation cursor");
    assert_eq!(
        fast_cursor_page1
            .as_scalar()
            .expect("fast page1 cursor should stay scalar")
            .boundary(),
        non_fast_cursor_page1
            .as_scalar()
            .expect("non-fast page1 cursor should stay scalar")
            .boundary(),
        "cursor boundaries should match even when signatures differ by access path",
    );

    // Phase 2: continuation should preserve parity on the second page too.
    let fast_page2_plan = build_simple_ordered_page_plan(false, 2, 1);
    let fast_page2_boundary = fast_page2_plan
        .prepare_cursor(Some(
            fast_cursor_page1
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("fast page2 boundary should plan");
    let fast_page2 = load
        .execute_paged_with_cursor(fast_page2_plan, fast_page2_boundary)
        .expect("fast page2 should execute");

    let non_fast_page2_plan =
        build_simple_by_ids_ordered_page_plan(keys.into_iter().map(Ulid::from_u128), false, 2, 1);
    let non_fast_page2_boundary = non_fast_page2_plan
        .prepare_cursor(Some(
            non_fast_cursor_page1
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("non-fast page2 boundary should plan");
    let non_fast_page2 = load
        .execute_paged_with_cursor(non_fast_page2_plan, non_fast_page2_boundary)
        .expect("non-fast page2 should execute");

    assert_eq!(
        simple_ids_from_items(&fast_page2.items),
        simple_ids_from_items(&non_fast_page2.items),
        "page2 rows should match between fast and non-fast access paths",
    );
    assert_eq!(
        fast_page2.next_cursor.is_some(),
        non_fast_page2.next_cursor.is_some(),
        "page2 cursor presence should match between paths",
    );
}

#[test]
fn load_cursor_pagination_pk_fast_path_desc_matches_non_fast_post_access_semantics() {
    setup_pagination_test();

    let keys = [5_u128, 1_u128, 4_u128, 2_u128, 3_u128];
    seed_simple_rows(&keys);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    // Phase 1: compare descending fast and non-fast first pages.
    let fast_page1_plan = build_simple_ordered_page_plan(true, 2, 1);
    let fast_page1_boundary = fast_page1_plan
        .prepare_cursor(None)
        .expect("fast descending page1 boundary should plan");
    let fast_page1 = load
        .execute_paged_with_cursor(fast_page1_plan, fast_page1_boundary)
        .expect("fast descending page1 should execute");

    let non_fast_page1_plan =
        build_simple_by_ids_ordered_page_plan(keys.into_iter().map(Ulid::from_u128), true, 2, 1);
    let non_fast_page1_boundary = non_fast_page1_plan
        .prepare_cursor(None)
        .expect("non-fast descending page1 boundary should plan");
    let non_fast_page1 = load
        .execute_paged_with_cursor(non_fast_page1_plan, non_fast_page1_boundary)
        .expect("non-fast descending page1 should execute");

    assert_eq!(
        simple_ids_from_items(&fast_page1.items),
        simple_ids_from_items(&non_fast_page1.items),
        "descending page1 rows should match between fast and non-fast access paths",
    );
    assert_eq!(
        fast_page1.next_cursor.is_some(),
        non_fast_page1.next_cursor.is_some(),
        "descending page1 cursor presence should match between paths",
    );

    let fast_cursor_page1 = fast_page1
        .next_cursor
        .as_ref()
        .expect("fast descending page1 should emit continuation cursor");
    let non_fast_cursor_page1 = non_fast_page1
        .next_cursor
        .as_ref()
        .expect("non-fast descending page1 should emit continuation cursor");
    assert_eq!(
        fast_cursor_page1
            .as_scalar()
            .expect("fast descending page1 cursor should stay scalar")
            .boundary(),
        non_fast_cursor_page1
            .as_scalar()
            .expect("non-fast descending page1 cursor should stay scalar")
            .boundary(),
        "descending cursor boundaries should match even when signatures differ by access path",
    );

    // Phase 2: continuation should preserve parity on the second page too.
    let fast_page2_plan = build_simple_ordered_page_plan(true, 2, 1);
    let fast_page2_boundary = fast_page2_plan
        .prepare_cursor(Some(
            fast_cursor_page1
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("fast descending page2 boundary should plan");
    let fast_page2 = load
        .execute_paged_with_cursor(fast_page2_plan, fast_page2_boundary)
        .expect("fast descending page2 should execute");

    let non_fast_page2_plan =
        build_simple_by_ids_ordered_page_plan(keys.into_iter().map(Ulid::from_u128), true, 2, 1);
    let non_fast_page2_boundary = non_fast_page2_plan
        .prepare_cursor(Some(
            non_fast_cursor_page1
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("non-fast descending page2 boundary should plan");
    let non_fast_page2 = load
        .execute_paged_with_cursor(non_fast_page2_plan, non_fast_page2_boundary)
        .expect("non-fast descending page2 should execute");

    assert_eq!(
        simple_ids_from_items(&fast_page2.items),
        simple_ids_from_items(&non_fast_page2.items),
        "descending page2 rows should match between fast and non-fast access paths",
    );
    assert_eq!(
        fast_page2.next_cursor.is_some(),
        non_fast_page2.next_cursor.is_some(),
        "descending page2 cursor presence should match between paths",
    );
}

#[test]
fn load_index_pushdown_eligible_paged_results_match_index_scan_window() {
    setup_pagination_test();

    let rows = [
        (11_001, 7, 10, "g7-r10"),
        (11_002, 7, 20, "g7-r20"),
        (11_003, 7, 30, "g7-r30"),
        (11_004, 7, 40, "g7-r40"),
        (11_005, 8, 5, "g8-r5"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let page1_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .plan()
        .map(ExecutablePlan::from)
        .expect("page1 parity plan should build");
    let page1_boundary = page1_plan
        .prepare_cursor(None)
        .expect("page1 parity boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("page1 parity should execute");
    let page1_ids: Vec<Ulid> = page1.items.iter().map(|row| row.entity_ref().id).collect();

    let expected_all = ordered_pushdown_group_ids(&rows, 7, false);
    let expected_page1: Vec<Ulid> = expected_all.iter().copied().take(2).collect();
    assert_eq!(
        page1_ids, expected_page1,
        "page1 output must match the canonical index-order window",
    );

    let page2_cursor = page1
        .next_cursor
        .as_ref()
        .expect("page1 parity should emit continuation cursor");
    let page2_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .map(ExecutablePlan::from)
        .expect("page2 parity plan should build");
    let page2_boundary = page2_plan
        .prepare_cursor(Some(
            page2_cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("page2 parity boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("page2 parity should execute");
    let page2_ids: Vec<Ulid> = page2.items.iter().map(|row| row.entity_ref().id).collect();

    let expected_page2: Vec<Ulid> = expected_all.iter().copied().skip(2).take(2).collect();
    assert_eq!(
        page2_ids, expected_page2,
        "page2 continuation must match the canonical index-order window",
    );
}

#[test]
fn load_index_pushdown_and_fallback_emit_equivalent_cursor_boundaries() {
    setup_pagination_test();

    let rows = [
        (12_001, 7, 10, "g7-r10"),
        (12_002, 7, 20, "g7-r20"),
        (12_003, 7, 30, "g7-r30"),
        (12_004, 7, 40, "g7-r40"),
        (12_005, 8, 5, "g8-r5"),
    ];
    seed_pushdown_rows(&rows);
    let group7_ids = pushdown_group_ids(&rows, 7);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let pushdown_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .map(ExecutablePlan::from)
        .expect("pushdown plan should build");
    let pushdown_page = load
        .execute_paged_with_cursor(pushdown_plan, None)
        .expect("pushdown page should execute");

    let fallback_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .by_ids(group7_ids.iter().copied())
        .order_by("rank")
        .limit(2)
        .plan()
        .map(ExecutablePlan::from)
        .expect("fallback plan should build");
    let fallback_page = load
        .execute_paged_with_cursor(fallback_plan, None)
        .expect("fallback page should execute");

    let pushdown_ids: Vec<Ulid> = pushdown_page
        .items
        .iter()
        .map(|row| row.entity_ref().id)
        .collect();
    let fallback_ids: Vec<Ulid> = fallback_page
        .items
        .iter()
        .map(|row| row.entity_ref().id)
        .collect();
    assert_eq!(
        pushdown_ids, fallback_ids,
        "pushdown and fallback page windows should match",
    );

    let pushdown_cursor = pushdown_page
        .next_cursor
        .as_ref()
        .expect("pushdown page should emit continuation cursor");
    let fallback_cursor = fallback_page
        .next_cursor
        .as_ref()
        .expect("fallback page should emit continuation cursor");
    assert_eq!(
        pushdown_cursor
            .as_scalar()
            .expect("pushdown page cursor should stay scalar")
            .boundary(),
        fallback_cursor
            .as_scalar()
            .expect("fallback page cursor should stay scalar")
            .boundary(),
        "pushdown and fallback cursors should encode the same continuation boundary",
    );
}

#[test]
fn load_index_pushdown_and_fallback_resume_equivalently_from_shared_boundary() {
    setup_pagination_test();

    let rows = [
        (13_001, 7, 10, "g7-r10"),
        (13_002, 7, 20, "g7-r20"),
        (13_003, 7, 30, "g7-r30"),
        (13_004, 7, 40, "g7-r40"),
        (13_005, 7, 50, "g7-r50"),
        (13_006, 8, 5, "g8-r5"),
    ];
    seed_pushdown_rows(&rows);
    let group7_ids = pushdown_group_ids(&rows, 7);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let seed_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .plan()
        .map(ExecutablePlan::from)
        .expect("seed plan should build");
    let seed_page = load
        .execute_paged_with_cursor(seed_plan, None)
        .expect("seed page should execute");
    let shared_boundary = seed_page
        .next_cursor
        .as_ref()
        .expect("seed page should emit continuation cursor")
        .as_scalar()
        .expect("seed page cursor should stay scalar")
        .boundary()
        .clone();

    let pushdown_page2_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .map(ExecutablePlan::from)
        .expect("pushdown page2 plan should build");
    let pushdown_page2 = load
        .execute_paged_with_cursor(pushdown_page2_plan, Some(shared_boundary.clone()))
        .expect("pushdown page2 should execute");

    let fallback_page2_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .by_ids(group7_ids.iter().copied())
        .order_by("rank")
        .limit(2)
        .plan()
        .map(ExecutablePlan::from)
        .expect("fallback page2 plan should build");
    let fallback_page2 = load
        .execute_paged_with_cursor(fallback_page2_plan, Some(shared_boundary))
        .expect("fallback page2 should execute");

    let pushdown_page2_ids: Vec<Ulid> = pushdown_page2
        .items
        .iter()
        .map(|row| row.entity_ref().id)
        .collect();
    let fallback_page2_ids: Vec<Ulid> = fallback_page2
        .items
        .iter()
        .map(|row| row.entity_ref().id)
        .collect();
    assert_eq!(
        pushdown_page2_ids, fallback_page2_ids,
        "pushdown and fallback should return the same rows for a shared continuation boundary",
    );

    let pushdown_next = pushdown_page2
        .next_cursor
        .as_ref()
        .expect("pushdown page2 should emit continuation cursor");
    let fallback_next = fallback_page2
        .next_cursor
        .as_ref()
        .expect("fallback page2 should emit continuation cursor");
    assert_eq!(
        pushdown_next
            .as_scalar()
            .expect("pushdown page2 cursor should stay scalar")
            .boundary(),
        fallback_next
            .as_scalar()
            .expect("fallback page2 cursor should stay scalar")
            .boundary(),
        "pushdown and fallback page2 cursors should encode identical boundaries",
    );
}

#[test]
fn load_index_pushdown_eligible_order_matches_index_scan_order() {
    setup_pagination_test();

    let rows = [
        (10_001, 7, 10, "g7-r10-a"),
        (10_002, 7, 10, "g7-r10-b"),
        (10_003, 7, 20, "g7-r20"),
        (10_004, 7, 30, "g7-r30"),
        (10_005, 8, 5, "g8-r5"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let explain = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("rank")
        .explain()
        .expect("parity explain should build");
    assert!(
        matches!(
            explain.order_pushdown(),
            crate::db::query::explain::ExplainOrderPushdown::MissingModelContext
        ),
        "query-layer explain should not evaluate secondary pushdown eligibility",
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("rank")
        .plan()
        .map(ExecutablePlan::from)
        .expect("parity load plan should build");
    let response = load.execute(plan).expect("parity load should execute");

    assert_eq!(
        pushdown_ids_from_response(&response),
        ordered_pushdown_group_ids(&rows, 7, false),
        "fallback post-access ordering must match canonical index traversal order for eligible plans",
    );
}

#[test]
fn load_index_prefix_spec_closed_bounds_preserve_prefix_window_end_to_end() {
    setup_pagination_test();

    let rows = [
        (10_301, 7, 10, "g7-r10"),
        (10_302, 7, 20, "g7-r20"),
        (10_303, 7, u32::MAX, "g7-rmax"),
        (10_304, 8, 5, "g8-r5"),
        (10_305, 8, u32::MAX, "g8-rmax"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let pushdown_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("rank")
        .plan()
        .map(ExecutablePlan::from)
        .expect("pushdown plan should build");
    let prefix_specs = pushdown_plan
        .index_prefix_specs()
        .expect("prefix specs should materialize");
    assert_eq!(
        prefix_specs.len(),
        1,
        "single index-prefix path should lower to exactly one prefix spec",
    );
    assert!(
        matches!(prefix_specs[0].lower(), std::ops::Bound::Included(_)),
        "index-prefix lower bound should stay closed",
    );
    assert!(
        matches!(prefix_specs[0].upper(), std::ops::Bound::Included(_)),
        "index-prefix upper bound should stay closed",
    );
    let pushdown_response = load
        .execute(pushdown_plan)
        .expect("pushdown execution should succeed");

    let group7_ids = pushdown_group_ids(&rows, 7);
    let fallback_response = load
        .execute(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .by_ids(group7_ids.iter().copied())
                .order_by("rank")
                .plan()
                .map(ExecutablePlan::from)
                .expect("fallback plan should build"),
        )
        .expect("fallback execution should succeed");

    assert_eq!(
        pushdown_ids_from_response(&pushdown_response),
        pushdown_ids_from_response(&fallback_response),
        "closed prefix bounds should preserve the exact group window",
    );
    assert!(
        pushdown_response
            .iter()
            .all(|row| row.entity_ref().group == 7),
        "closed prefix bounds must not leak adjacent prefix rows",
    );
}

#[test]
fn load_index_pushdown_desc_with_explicit_pk_desc_is_eligible_and_ordered() {
    setup_pagination_test();

    let rows = [
        (10_501, 7, 10, "g7-r10-a"),
        (10_502, 7, 10, "g7-r10-b"),
        (10_503, 7, 20, "g7-r20"),
        (10_504, 7, 30, "g7-r30"),
        (10_505, 8, 5, "g8-r5"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let explain = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by_desc("rank")
        .order_by_desc("id")
        .explain()
        .expect("descending parity explain should build");
    assert!(
        matches!(
            explain.order_pushdown(),
            crate::db::query::explain::ExplainOrderPushdown::MissingModelContext
        ),
        "query-layer explain should not evaluate secondary pushdown eligibility",
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by_desc("rank")
        .order_by_desc("id")
        .plan()
        .map(ExecutablePlan::from)
        .expect("descending parity load plan should build");
    let response = load
        .execute(plan)
        .expect("descending parity load should execute");

    assert_eq!(
        pushdown_ids_from_response(&response),
        {
            let mut ordered = rows
                .iter()
                .filter(|(_, group, _, _)| *group == 7)
                .map(|(id, _, rank, _)| (*rank, Ulid::from_u128(*id)))
                .collect::<Vec<_>>();
            ordered.sort_by(|(left_rank, left_id), (right_rank, right_id)| {
                right_rank
                    .cmp(left_rank)
                    .then_with(|| right_id.cmp(left_id))
            });
            ordered.into_iter().map(|(_, id)| id).collect::<Vec<_>>()
        },
        "descending pushdown order should match reversed canonical index traversal",
    );
}

#[test]
fn load_index_range_cursor_anchor_matches_last_emitted_row_after_post_access_pipeline() {
    setup_pagination_test();

    let rows = [
        (12_501, 7, 10, "g7-r10-a"),
        (12_502, 7, 10, "g7-r10-b"),
        (12_503, 7, 20, "g7-r20"),
        (12_504, 7, 30, "g7-r30"),
        (12_505, 8, 5, "g8-r5"),
    ];
    seed_pushdown_rows(&rows);

    // Phase 1: execute one bounded index-range page under canonical post-access order/page phases.
    let predicate = group_rank_range_predicate(7, 10, 40);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let page_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .plan()
        .map(ExecutablePlan::from)
        .expect("index-range page plan should build");
    let page = load
        .execute_paged_with_cursor(page_plan, None)
        .expect("index-range page should execute");
    assert_eq!(page.items.len(), 2, "page should emit exactly two rows");

    // Phase 2: confirm the continuation boundary tracks the last emitted row.
    let emitted_cursor = page
        .next_cursor
        .as_ref()
        .expect("page should emit a continuation cursor");
    let scalar_cursor = emitted_cursor
        .as_scalar()
        .expect("index-range pagination must emit a scalar continuation cursor");
    let last_entity = page
        .items
        .last()
        .expect("non-empty page must include a trailing emitted row")
        .entity_ref();

    let fallback_ids = pushdown_group_ids(&rows, 7);
    let fallback_page = load
        .execute_paged_with_cursor(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .by_ids(fallback_ids.iter().copied())
                .order_by("rank")
                .limit(2)
                .plan()
                .map(ExecutablePlan::from)
                .expect("fallback page plan should build"),
            None,
        )
        .expect("fallback page should execute");
    let fallback_cursor = fallback_page
        .next_cursor
        .as_ref()
        .expect("fallback page should emit a continuation cursor")
        .as_scalar()
        .expect("fallback page cursor should stay scalar");
    assert_eq!(
        scalar_cursor.boundary(),
        fallback_cursor.boundary(),
        "continuation boundary must track the last emitted post-access row",
    );

    // Phase 3: confirm the raw index anchor matches the last emitted row index key.
    let comparison_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .map(ExecutablePlan::from)
        .expect("comparison plan should build");
    let (index_model, _, _, _) = comparison_plan
        .logical_plan()
        .access
        .as_index_range_path()
        .expect("comparison plan should remain on index-range access");
    let expected_anchor = crate::db::cursor::cursor_anchor_from_raw_index_key(
        &crate::db::index::IndexKey::new(last_entity, index_model)
            .expect("index key derivation should succeed")
            .expect("last emitted row should be indexable")
            .to_raw(),
    );
    let emitted_anchor = scalar_cursor
        .index_range_anchor()
        .expect("index-range cursor should carry a raw-key anchor");
    assert_eq!(
        emitted_anchor.last_raw_key(),
        expected_anchor.last_raw_key(),
        "continuation raw-key anchor must match the last emitted row index key",
    );
}

#[test]
fn load_cursor_rejects_signature_mismatch_between_pushdown_and_fallback_shapes() {
    setup_pagination_test();

    let rows = [
        (13_501, 7, 10, "g7-r10"),
        (13_502, 7, 20, "g7-r20"),
        (13_503, 7, 30, "g7-r30"),
        (13_504, 7, 40, "g7-r40"),
        (13_505, 7, 50, "g7-r50"),
        (13_506, 8, 5, "g8-r5"),
    ];
    seed_pushdown_rows(&rows);
    let group7_ids = pushdown_group_ids(&rows, 7);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    // Phase 1: capture one pushdown cursor and prove fallback boundary parity.
    let pushdown_seed_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .map(ExecutablePlan::from)
        .expect("pushdown seed plan should build");
    let pushdown_seed_page = load
        .execute_paged_with_cursor(pushdown_seed_plan, None)
        .expect("pushdown seed page should execute");
    let pushdown_cursor = pushdown_seed_page
        .next_cursor
        .as_ref()
        .expect("pushdown seed page should emit continuation cursor");

    let fallback_seed_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .by_ids(group7_ids.iter().copied())
        .order_by("rank")
        .limit(2)
        .plan()
        .map(ExecutablePlan::from)
        .expect("fallback seed plan should build");
    let fallback_seed_page = load
        .execute_paged_with_cursor(fallback_seed_plan, None)
        .expect("fallback seed page should execute");
    let fallback_cursor = fallback_seed_page
        .next_cursor
        .as_ref()
        .expect("fallback seed page should emit continuation cursor");
    assert_eq!(
        pushdown_cursor
            .as_scalar()
            .expect("pushdown seed cursor should stay scalar")
            .boundary(),
        fallback_cursor
            .as_scalar()
            .expect("fallback seed cursor should stay scalar")
            .boundary(),
        "pushdown and fallback cursor boundaries should match for the same ordered window",
    );

    // Phase 2: enforce signature contract across pushdown and fallback shapes.
    let fallback_resume_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .by_ids(group7_ids.iter().copied())
        .order_by("rank")
        .limit(2)
        .plan()
        .map(ExecutablePlan::from)
        .expect("fallback resume plan should build");
    let err = fallback_resume_plan
        .prepare_cursor(Some(
            pushdown_cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect_err("cursor from a different access-shape signature should be rejected");
    assert!(
        matches!(
            err,
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::cursor::CursorPlanError::ContinuationCursorSignatureMismatch { .. }
                )
        ),
        "pushdown/fallback cross-shape cursor replay should fail with signature mismatch",
    );
}

#[test]
fn load_composite_pk_budget_trace_limits_access_rows_for_safe_shape() {
    setup_pagination_test();
    seed_simple_rows(&[37_201, 37_202, 37_203, 37_204, 37_205, 37_206]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    let plan = build_simple_union_page_plan(
        vec![
            Ulid::from_u128(37_201),
            Ulid::from_u128(37_202),
            Ulid::from_u128(37_203),
            Ulid::from_u128(37_204),
        ],
        vec![
            Ulid::from_u128(37_203),
            Ulid::from_u128(37_204),
            Ulid::from_u128(37_205),
            Ulid::from_u128(37_206),
        ],
        false,
        2,
        1,
        None,
    );
    let (page, trace) = load
        .execute_paged_with_cursor_traced(plan, None)
        .expect("composite budget trace execution should succeed");

    assert_eq!(
        trace.map(|trace| trace.keys_scanned()),
        Some(4),
        "safe composite PK-order shape should apply offset+limit+1 scan budget",
    );
    assert_eq!(
        simple_ids_from_items(&page.items),
        vec![Ulid::from_u128(37_202), Ulid::from_u128(37_203)],
        "safe composite budget path must preserve canonical offset/limit page rows",
    );
}

#[test]
fn load_composite_pk_budget_disabled_when_cursor_boundary_present() {
    setup_pagination_test();
    seed_simple_rows(&[37_301, 37_302, 37_303, 37_304, 37_305, 37_306]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    let plan = build_simple_union_page_plan(
        vec![
            Ulid::from_u128(37_301),
            Ulid::from_u128(37_302),
            Ulid::from_u128(37_303),
            Ulid::from_u128(37_304),
        ],
        vec![
            Ulid::from_u128(37_303),
            Ulid::from_u128(37_304),
            Ulid::from_u128(37_305),
            Ulid::from_u128(37_306),
        ],
        false,
        2,
        0,
        None,
    );
    let cursor = crate::db::cursor::CursorBoundary {
        slots: vec![crate::db::cursor::CursorBoundarySlot::Present(Value::Ulid(
            Ulid::from_u128(37_303),
        ))],
    };
    let (_page, trace) = load
        .execute_paged_with_cursor_traced(plan, Some(cursor))
        .expect("composite cursor trace execution should succeed");

    assert_eq!(
        trace.map(|trace| trace.keys_scanned()),
        Some(6),
        "cursor narrowing is post-access for this shape, so scan budgeting must stay disabled",
    );
}

#[test]
fn load_composite_pk_budget_trace_limits_access_rows_for_safe_desc_shape() {
    setup_pagination_test();
    seed_simple_rows(&[37_601, 37_602, 37_603, 37_604, 37_605, 37_606]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    let plan = build_simple_union_page_plan(
        vec![
            Ulid::from_u128(37_601),
            Ulid::from_u128(37_602),
            Ulid::from_u128(37_603),
            Ulid::from_u128(37_604),
        ],
        vec![
            Ulid::from_u128(37_603),
            Ulid::from_u128(37_604),
            Ulid::from_u128(37_605),
            Ulid::from_u128(37_606),
        ],
        true,
        2,
        1,
        None,
    );
    let (page, trace) = load
        .execute_paged_with_cursor_traced(plan, None)
        .expect("composite desc budget trace execution should succeed");

    assert_eq!(
        trace.map(|trace| trace.keys_scanned()),
        Some(4),
        "safe DESC composite PK-order shape should apply offset+limit+1 scan budget",
    );
    assert_eq!(
        simple_ids_from_items(&page.items),
        vec![Ulid::from_u128(37_605), Ulid::from_u128(37_604)],
        "safe DESC composite budget path must preserve canonical offset/limit page rows",
    );
}

#[test]
fn load_composite_budgeted_and_fallback_paths_emit_equivalent_continuation_boundary() {
    setup_pagination_test();
    seed_simple_rows(&[37_801, 37_802, 37_803, 37_804, 37_805, 37_806]);

    // Phase 1: compare the budget-eligible and residual-filter fallback paths.
    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    let budgeted_plan = build_simple_union_page_plan(
        vec![
            Ulid::from_u128(37_801),
            Ulid::from_u128(37_802),
            Ulid::from_u128(37_803),
            Ulid::from_u128(37_804),
        ],
        vec![
            Ulid::from_u128(37_803),
            Ulid::from_u128(37_804),
            Ulid::from_u128(37_805),
            Ulid::from_u128(37_806),
        ],
        false,
        2,
        1,
        None,
    );
    let fallback_plan = build_simple_union_page_plan(
        vec![
            Ulid::from_u128(37_801),
            Ulid::from_u128(37_802),
            Ulid::from_u128(37_803),
            Ulid::from_u128(37_804),
        ],
        vec![
            Ulid::from_u128(37_803),
            Ulid::from_u128(37_804),
            Ulid::from_u128(37_805),
            Ulid::from_u128(37_806),
        ],
        false,
        2,
        1,
        Some(Predicate::And(vec![
            strict_compare_predicate("id", CompareOp::Gte, Value::Ulid(Ulid::from_u128(37_801))),
            strict_compare_predicate("id", CompareOp::Lte, Value::Ulid(Ulid::from_u128(37_806))),
        ])),
    );
    let (budgeted_page, budgeted_trace) = load
        .execute_paged_with_cursor_traced(budgeted_plan, None)
        .expect("budgeted trace execution should succeed");
    let (fallback_page, fallback_trace) = load
        .execute_paged_with_cursor_traced(fallback_plan, None)
        .expect("fallback trace execution should succeed");

    assert_eq!(
        budgeted_trace.map(|trace| trace.keys_scanned()),
        Some(4),
        "budgeted path should cap keys scanned at offset+limit+1",
    );
    assert_eq!(
        fallback_trace.map(|trace| trace.keys_scanned()),
        Some(6),
        "residual-filter fallback path should preserve full access scan volume",
    );
    assert_eq!(
        simple_ids_from_items(&budgeted_page.items),
        simple_ids_from_items(&fallback_page.items),
        "budgeted and fallback paths must emit identical page rows",
    );

    // Phase 2: both shapes must encode the same continuation boundary for the same page.
    let budgeted_cursor = budgeted_page
        .next_cursor
        .as_ref()
        .expect("budgeted path should emit continuation cursor")
        .as_scalar()
        .expect("budgeted path should keep a scalar cursor");
    let fallback_cursor = fallback_page
        .next_cursor
        .as_ref()
        .expect("fallback path should emit continuation cursor")
        .as_scalar()
        .expect("fallback path should keep a scalar cursor");
    assert_eq!(
        budgeted_cursor.boundary(),
        fallback_cursor.boundary(),
        "budgeted and fallback paths should encode the same continuation boundary",
    );
}

#[test]
fn load_composite_budget_disabled_when_post_access_sort_is_required() {
    setup_pagination_test();

    let rows = [
        (37_401, 7, 30, "r30"),
        (37_402, 7, 10, "r10-a"),
        (37_403, 7, 20, "r20"),
        (37_404, 7, 10, "r10-b"),
        (37_405, 7, 40, "r40"),
        (37_406, 7, 50, "r50"),
    ];
    seed_pushdown_rows(&rows);

    let plan = ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery {
        logical: LogicalPlan::Scalar(ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![
                    ("rank".to_string(), OrderDirection::Asc),
                    ("id".to_string(), OrderDirection::Asc),
                ],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(2),
                offset: 1,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::ByKeys(vec![
                Ulid::from_u128(37_401),
                Ulid::from_u128(37_402),
                Ulid::from_u128(37_403),
                Ulid::from_u128(37_404),
            ])),
            AccessPlan::path(AccessPath::ByKeys(vec![
                Ulid::from_u128(37_403),
                Ulid::from_u128(37_404),
                Ulid::from_u128(37_405),
                Ulid::from_u128(37_406),
            ])),
        ])
        .into_value_plan(),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
    });
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);
    let (_page, trace) = load
        .execute_paged_with_cursor_traced(plan, None)
        .expect("composite post-sort trace execution should succeed");

    assert_eq!(
        trace.map(|trace| trace.keys_scanned()),
        Some(6),
        "post-access sort requirement must disable scan budgeting for composite paths",
    );
}

#[test]
fn load_composite_budget_disabled_for_offset_with_residual_filter() {
    setup_pagination_test();
    seed_simple_rows(&[37_501, 37_502, 37_503, 37_504, 37_505, 37_506]);

    let plan = build_simple_union_page_plan(
        vec![
            Ulid::from_u128(37_501),
            Ulid::from_u128(37_502),
            Ulid::from_u128(37_503),
            Ulid::from_u128(37_504),
        ],
        vec![
            Ulid::from_u128(37_503),
            Ulid::from_u128(37_504),
            Ulid::from_u128(37_505),
            Ulid::from_u128(37_506),
        ],
        false,
        2,
        1,
        Some(strict_compare_predicate(
            "id",
            CompareOp::Gte,
            Value::Ulid(Ulid::from_u128(37_502)),
        )),
    );
    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    let (page, trace) = load
        .execute_paged_with_cursor_traced(plan, None)
        .expect("offset+filter budget-disable trace execution should succeed");

    assert_eq!(
        trace.map(|trace| trace.keys_scanned()),
        Some(6),
        "residual filter must disable scan budgeting and preserve full access scan volume",
    );
    assert_eq!(
        simple_ids_from_items(&page.items),
        vec![Ulid::from_u128(37_503), Ulid::from_u128(37_504)],
        "offset+filter window should remain canonical under fallback path",
    );
    assert!(
        page.next_cursor.is_some(),
        "offset+filter first page should still emit continuation when more rows remain",
    );
}

#[test]
fn load_nested_composite_pk_budget_trace_limits_access_rows_for_safe_shape() {
    setup_pagination_test();
    seed_simple_rows(&[
        37_701, 37_702, 37_703, 37_704, 37_705, 37_706, 37_707, 37_708,
    ]);

    let plan = ExecutablePlan::<SimpleEntity>::new(AccessPlannedQuery {
        logical: LogicalPlan::Scalar(ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(2),
                offset: 1,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::Union(vec![
            AccessPlan::Intersection(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![
                    Ulid::from_u128(37_701),
                    Ulid::from_u128(37_702),
                    Ulid::from_u128(37_703),
                    Ulid::from_u128(37_704),
                    Ulid::from_u128(37_705),
                ])),
                AccessPlan::path(AccessPath::ByKeys(vec![
                    Ulid::from_u128(37_703),
                    Ulid::from_u128(37_704),
                    Ulid::from_u128(37_705),
                    Ulid::from_u128(37_706),
                    Ulid::from_u128(37_707),
                ])),
            ]),
            AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![
                    Ulid::from_u128(37_705),
                    Ulid::from_u128(37_706),
                    Ulid::from_u128(37_707),
                ])),
                AccessPlan::path(AccessPath::ByKeys(vec![
                    Ulid::from_u128(37_707),
                    Ulid::from_u128(37_708),
                ])),
            ]),
        ])
        .into_value_plan(),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
    });
    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    let (page, trace) = load
        .execute_paged_with_cursor_traced(plan, None)
        .expect("nested composite budget trace execution should succeed");

    assert_eq!(
        trace.map(|trace| trace.keys_scanned()),
        Some(4),
        "safe nested composite PK-order shape should apply offset+limit+1 scan budget",
    );
    assert_eq!(
        simple_ids_from_items(&page.items),
        vec![Ulid::from_u128(37_704), Ulid::from_u128(37_705)],
        "safe nested composite budget path must preserve canonical page window",
    );
}

#[test]
fn load_composite_union_mixed_direction_fallback_preserves_order_and_pagination() {
    setup_pagination_test();

    let rows = [
        (39_001, 7, 10, "g7-r10"),
        (39_002, 7, 20, "g7-r20-a"),
        (39_003, 7, 20, "g7-r20-b"),
        (39_004, 7, 30, "g7-r30"),
        (39_005, 8, 15, "g8-r15"),
    ];
    seed_pushdown_rows(&rows);

    let build_plan = || {
        ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery {
            logical: LogicalPlan::Scalar(ScalarPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: Some(OrderSpec {
                    fields: vec![
                        ("rank".to_string(), OrderDirection::Desc),
                        ("id".to_string(), OrderDirection::Asc),
                    ],
                }),
                distinct: false,
                delete_limit: None,
                page: Some(PageSpec {
                    limit: Some(2),
                    offset: 0,
                }),
                consistency: MissingRowPolicy::Ignore,
            }),
            access: AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![
                    Ulid::from_u128(39_001),
                    Ulid::from_u128(39_002),
                    Ulid::from_u128(39_004),
                ])),
                AccessPlan::path(AccessPath::ByKeys(vec![
                    Ulid::from_u128(39_002),
                    Ulid::from_u128(39_003),
                    Ulid::from_u128(39_005),
                ])),
            ])
            .into_value_plan(),
            projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        })
    };

    // Phase 1: collect the full paged stream under the live continuation API.
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let mut ids = Vec::new();
    let mut boundary_count = 0usize;
    let mut encoded_cursor = None::<Vec<u8>>;
    for _ in 0..10 {
        let plan = build_plan();
        let boundary = plan
            .prepare_cursor(encoded_cursor.as_deref())
            .expect("mixed-direction fallback boundary should plan");
        let page = load
            .execute_paged_with_cursor(plan, boundary)
            .expect("mixed-direction fallback page should execute");
        ids.extend(pushdown_ids_from_response(&page.items));

        let Some(cursor) = page.next_cursor else {
            break;
        };
        boundary_count = boundary_count.saturating_add(1);
        encoded_cursor = Some(
            cursor
                .encode()
                .expect("mixed-direction continuation cursor should serialize"),
        );
    }

    // Phase 2: assert stable mixed-direction pagination across page boundaries.
    assert_eq!(
        ids,
        vec![
            Ulid::from_u128(39_004),
            Ulid::from_u128(39_002),
            Ulid::from_u128(39_003),
            Ulid::from_u128(39_005),
            Ulid::from_u128(39_001),
        ],
        "mixed-direction union fallback should preserve rank DESC with PK ASC tie-break across pages",
    );
    assert_eq!(
        boundary_count, 2,
        "limit=2 over five rows should emit exactly two continuation boundaries",
    );
}

#[test]
fn load_composite_between_equivalent_pushdown_matches_by_ids_fallback() {
    setup_pagination_test();

    let rows = [
        (19_201, 7, 5, "g7-r5"),
        (19_202, 7, 10, "g7-r10-a"),
        (19_203, 7, 10, "g7-r10-b"),
        (19_204, 7, 20, "g7-r20"),
        (19_205, 7, 30, "g7-r30"),
        (19_206, 7, 40, "g7-r40"),
        (19_207, 8, 15, "g8-r15"),
    ];
    seed_pushdown_rows(&rows);

    // Phase 1: prove the live planner stays on index-range access for the bounded range.
    let predicate = group_rank_between_equivalent_predicate(7, 10, 30);
    let pushdown_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("rank")
        .plan()
        .map(ExecutablePlan::from)
        .expect("composite between-equivalent plan should build");
    let (index_model, _, _, _) = pushdown_plan
        .logical_plan()
        .access
        .as_index_range_path()
        .expect("between-equivalent range should stay on index-range access");
    assert_eq!(
        index_model.name(),
        PUSHDOWN_PARITY_INDEX_MODELS[0].name(),
        "between-equivalent range should use the composite pushdown index",
    );

    // Phase 2: compare pushdown results with the legal by-ids fallback shape.
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let pushdown = load
        .execute(pushdown_plan)
        .expect("between-equivalent pushdown execution should succeed");
    let fallback = load
        .execute(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .by_ids(pushdown_ids_in_group_rank_between_equivalent_range(
                    &rows, 7, 10, 30,
                ))
                .order_by("rank")
                .plan()
                .map(ExecutablePlan::from)
                .expect("between-equivalent fallback plan should build"),
        )
        .expect("between-equivalent fallback execution should succeed");

    assert_eq!(
        pushdown_ids_from_response(&pushdown),
        pushdown_ids_from_response(&fallback),
        "between-equivalent pushdown should match the canonical by-ids fallback rows",
    );
}

#[test]
fn load_composite_range_pushdown_handles_min_and_max_rank_edges() {
    const MAX_RANK: u32 = u32::MAX;

    setup_pagination_test();

    let rows = [
        (19_401, 7, 0, "g7-r0"),
        (19_402, 7, 1, "g7-r1"),
        (19_403, 7, 10, "g7-r10"),
        (19_404, 7, MAX_RANK - 1, "g7-rmax-1"),
        (19_405, 7, MAX_RANK, "g7-rmax"),
        (19_406, 8, MAX_RANK, "g8-rmax"),
    ];
    seed_pushdown_rows(&rows);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    // Phase 1: exclusive upper bound must exclude the max-rank row while staying on index range.
    let exclusive_predicate = group_rank_range_predicate(7, 0, MAX_RANK);
    let exclusive_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(exclusive_predicate)
        .order_by("rank")
        .plan()
        .map(ExecutablePlan::from)
        .expect("composite exclusive edge plan should build");
    exclusive_plan
        .logical_plan()
        .access
        .as_index_range_path()
        .expect("exclusive edge range should stay on index-range access");
    let exclusive_pushdown = load
        .execute(exclusive_plan)
        .expect("composite exclusive edge pushdown should execute");
    let exclusive_fallback = load
        .execute(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .by_ids(pushdown_ids_in_group_rank_range(&rows, 7, 0, MAX_RANK))
                .order_by("rank")
                .plan()
                .map(ExecutablePlan::from)
                .expect("composite exclusive edge fallback plan should build"),
        )
        .expect("composite exclusive edge fallback should execute");
    assert_eq!(
        pushdown_ids_from_response(&exclusive_pushdown),
        pushdown_ids_from_response(&exclusive_fallback),
        "exclusive upper bound should match fallback and exclude the max-rank row",
    );

    // Phase 2: inclusive upper bound must include the max-rank row.
    let inclusive_predicate = group_rank_between_equivalent_predicate(7, 0, MAX_RANK);
    let inclusive_pushdown = load
        .execute(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(inclusive_predicate)
                .order_by("rank")
                .plan()
                .map(ExecutablePlan::from)
                .expect("composite inclusive edge plan should build"),
        )
        .expect("composite inclusive edge pushdown should execute");
    assert!(
        pushdown_ids_from_response(&inclusive_pushdown).contains(&Ulid::from_u128(19_405)),
        "inclusive upper-bound range must include rows at the max field value",
    );
}

#[test]
fn load_composite_range_cursor_pagination_matches_fallback_without_duplicates() {
    setup_pagination_test();

    let rows = [
        (20_001, 7, 5, "g7-r5"),
        (20_002, 7, 10, "g7-r10-a"),
        (20_003, 7, 20, "g7-r20-a"),
        (20_004, 7, 20, "g7-r20-b"),
        (20_005, 7, 30, "g7-r30"),
        (20_006, 7, 35, "g7-r35"),
        (20_007, 7, 40, "g7-r40"),
        (20_008, 8, 15, "g8-r15"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = group_rank_range_predicate(7, 10, 40);
    let pushdown_seed_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .plan()
        .map(ExecutablePlan::from)
        .expect("composite range pagination plan should build");
    pushdown_seed_plan
        .logical_plan()
        .access
        .as_index_range_path()
        .expect("composite range pagination should stay on index-range access");

    // Phase 1: collect all pushdown pages.
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let pushdown_ids = collect_pushdown_paged_ids(
        &load,
        || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .order_by("rank")
                .limit(2)
                .plan()
                .map(ExecutablePlan::from)
                .expect("composite range pushdown page plan should build")
        },
        8,
    );

    // Phase 2: collect the legal by-ids fallback pages and compare the full suffix.
    let fallback_seed_ids = pushdown_ids_in_group_rank_range(&rows, 7, 10, 40);
    let fallback_ids = collect_pushdown_paged_ids(
        &load,
        || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .by_ids(fallback_seed_ids.iter().copied())
                .order_by("rank")
                .limit(2)
                .plan()
                .map(ExecutablePlan::from)
                .expect("composite range fallback page plan should build")
        },
        8,
    );

    assert_eq!(
        pushdown_ids, fallback_ids,
        "composite range cursor pagination should match fallback across all pages",
    );
    let unique_pushdown_ids: std::collections::BTreeSet<Ulid> =
        pushdown_ids.iter().copied().collect();
    assert_eq!(
        unique_pushdown_ids.len(),
        pushdown_ids.len(),
        "composite range cursor pagination must not emit duplicate rows",
    );
}

#[test]
fn load_composite_range_cursor_pagination_matches_unbounded_and_anchor_is_strictly_monotonic() {
    setup_pagination_test();

    let rows = [
        (20_101, 7, 10, "g7-r10-a"),
        (20_102, 7, 10, "g7-r10-b"),
        (20_103, 7, 20, "g7-r20-a"),
        (20_104, 7, 20, "g7-r20-b"),
        (20_105, 7, 25, "g7-r25"),
        (20_106, 7, 30, "g7-r30"),
        (20_107, 7, 35, "g7-r35"),
        (20_108, 8, 10, "g8-r10"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = group_rank_range_predicate(7, 10, 40);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    // Phase 1: compare paged results with the unbounded result set byte-for-byte.
    let unbounded = load
        .execute(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .order_by("rank")
                .plan()
                .map(ExecutablePlan::from)
                .expect("composite monotonicity unbounded plan should build"),
        )
        .expect("composite monotonicity unbounded execution should succeed");
    let unbounded_ids = pushdown_ids_from_response(&unbounded);
    let unbounded_row_bytes: Vec<Vec<u8>> = unbounded
        .iter()
        .map(|row| {
            crate::serialize::serialize(row.entity_ref())
                .expect("composite monotonicity row serialization should succeed")
        })
        .collect();

    let mut paged_ids = Vec::new();
    let mut paged_row_bytes = Vec::new();
    let mut encoded_cursor = None::<Vec<u8>>;
    let mut previous_anchor = None::<Vec<u8>>;
    for _ in 0..8 {
        let boundary_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(predicate.clone())
            .order_by("rank")
            .limit(3)
            .plan()
            .map(ExecutablePlan::from)
            .expect("composite monotonicity boundary plan should build");
        let page = load
            .execute_paged_with_cursor(
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .filter(predicate.clone())
                    .order_by("rank")
                    .limit(3)
                    .plan()
                    .map(ExecutablePlan::from)
                    .expect("composite monotonicity page plan should build"),
                boundary_plan
                    .prepare_cursor(encoded_cursor.as_deref())
                    .expect("composite monotonicity cursor should plan"),
            )
            .expect("composite monotonicity page should execute");

        paged_ids.extend(pushdown_ids_from_response(&page.items));
        paged_row_bytes.extend(page.items.iter().map(|row| {
            crate::serialize::serialize(row.entity_ref())
                .expect("composite monotonicity paged row serialization should succeed")
        }));

        let Some(cursor) = page.next_cursor else {
            break;
        };
        let scalar = cursor
            .as_scalar()
            .expect("composite monotonicity cursor should stay scalar");
        let anchor = scalar
            .index_range_anchor()
            .expect("composite monotonicity cursor should include a raw-key anchor")
            .last_raw_key()
            .to_vec();
        if let Some(previous_anchor) = previous_anchor.as_ref() {
            assert!(
                previous_anchor.as_slice() < anchor.as_slice(),
                "composite range continuation anchors must progress strictly monotonically",
            );
        }
        previous_anchor = Some(anchor);
        encoded_cursor = Some(
            cursor
                .encode()
                .expect("composite monotonicity cursor should serialize"),
        );
    }

    assert_eq!(
        paged_ids, unbounded_ids,
        "concatenated paginated ids must match unbounded ids in the same order",
    );
    assert_eq!(
        paged_row_bytes, unbounded_row_bytes,
        "concatenated paginated rows must be byte-for-byte identical to the unbounded result set",
    );
}

#[test]
fn load_composite_desc_range_mixed_edges_resume_inside_duplicate_group() {
    setup_pagination_test();

    let rows = [
        (21_601, 7, 10, "g7-r10-a"),
        (21_602, 7, 10, "g7-r10-b"),
        (21_603, 7, 20, "g7-r20"),
        (21_604, 7, 30, "g7-r30-a"),
        (21_605, 7, 30, "g7-r30-b"),
        (21_606, 7, 40, "g7-r40"),
        (21_607, 8, 30, "g8-r30"),
    ];
    seed_pushdown_rows(&rows);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    // Phase 1: collect the full descending window.
    let base_page = execute_pushdown_rank_page_desc(
        &load,
        Predicate::And(vec![
            strict_compare_predicate("group", CompareOp::Eq, Value::Uint(7)),
            strict_compare_predicate("rank", CompareOp::Gt, Value::Uint(10)),
            strict_compare_predicate("rank", CompareOp::Lte, Value::Uint(30)),
        ]),
        None,
        "composite mixed-edge desc base page",
    );
    let all_ids = pushdown_ids_from_response(&base_page.items);
    assert_eq!(
        all_ids,
        vec![
            Ulid::from_u128(21_604),
            Ulid::from_u128(21_605),
            Ulid::from_u128(21_603),
        ],
        "composite descending mixed-edge range should preserve duplicate-group order with canonical PK tie-break",
    );

    // Phase 2: build explicit canonical resume boundaries from rank + PK.
    let first_resume = execute_pushdown_rank_page_desc(
        &load,
        Predicate::And(vec![
            strict_compare_predicate("group", CompareOp::Eq, Value::Uint(7)),
            strict_compare_predicate("rank", CompareOp::Gt, Value::Uint(10)),
            strict_compare_predicate("rank", CompareOp::Lte, Value::Uint(30)),
        ]),
        Some(pushdown_rank_id_boundary(30, 21_604)),
        "composite mixed-edge desc first resume",
    );
    assert_eq!(
        pushdown_ids_from_response(&first_resume.items),
        all_ids[1..].to_vec(),
        "boundary inside the upper duplicate group should resume from the sibling row",
    );

    let terminal_resume = execute_pushdown_rank_page_desc(
        &load,
        Predicate::And(vec![
            strict_compare_predicate("group", CompareOp::Eq, Value::Uint(7)),
            strict_compare_predicate("rank", CompareOp::Gt, Value::Uint(10)),
            strict_compare_predicate("rank", CompareOp::Lte, Value::Uint(30)),
        ]),
        Some(pushdown_rank_id_boundary(20, 21_603)),
        "composite mixed-edge desc terminal resume",
    );
    assert!(
        terminal_resume.items.is_empty(),
        "composite descending mixed-edge range should be exhausted after the lower-edge terminal boundary",
    );
    assert!(
        terminal_resume.next_cursor.is_none(),
        "composite empty descending mixed-edge continuation page must not emit a cursor",
    );
}

#[test]
fn load_composite_between_cursor_boundaries_respect_duplicate_lower_and_upper_edges() {
    setup_pagination_test();

    let rows = [
        (25_001, 7, 10, "g7-r10-a"),
        (25_002, 7, 10, "g7-r10-b"),
        (25_003, 7, 20, "g7-r20"),
        (25_004, 7, 30, "g7-r30-a"),
        (25_005, 7, 30, "g7-r30-b"),
        (25_006, 7, 40, "g7-r40"),
        (25_007, 8, 10, "g8-r10"),
    ];
    seed_pushdown_rows(&rows);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    // Phase 1: capture the full bounded window.
    let base_page = execute_pushdown_rank_page_asc(
        &load,
        group_rank_between_equivalent_predicate(7, 10, 30),
        None,
        "composite duplicate-edge base page",
    );
    let all_ids = pushdown_ids_from_response(&base_page.items);
    assert_eq!(
        all_ids.len(),
        5,
        "composite between range should include duplicate lower and upper edge rows",
    );

    // Phase 2: explicit resume boundaries must skip strictly after the chosen row.
    let first_resume = execute_pushdown_rank_page_asc(
        &load,
        group_rank_between_equivalent_predicate(7, 10, 30),
        Some(pushdown_rank_id_boundary(10, 25_001)),
        "composite duplicate-edge first resume",
    );
    assert_eq!(
        pushdown_ids_from_response(&first_resume.items),
        all_ids[1..].to_vec(),
        "boundary at the first lower-edge row should skip only that row",
    );

    let middle_resume = execute_pushdown_rank_page_asc(
        &load,
        group_rank_between_equivalent_predicate(7, 10, 30),
        Some(pushdown_rank_id_boundary(20, 25_003)),
        "composite duplicate-edge middle resume",
    );
    assert_eq!(
        pushdown_ids_from_response(&middle_resume.items),
        all_ids[3..].to_vec(),
        "mid-window boundary should resume at the next strict row",
    );

    let terminal_resume = execute_pushdown_rank_page_asc(
        &load,
        group_rank_between_equivalent_predicate(7, 10, 30),
        Some(pushdown_rank_id_boundary(30, 25_005)),
        "composite duplicate-edge terminal resume",
    );
    assert!(
        terminal_resume.items.is_empty(),
        "boundary at upper-edge terminal row should return an empty continuation page",
    );
    assert!(
        terminal_resume.next_cursor.is_none(),
        "composite empty continuation page should not emit a cursor",
    );
}

#[test]
fn load_single_field_desc_range_resume_from_upper_anchor_returns_remaining_rows() {
    setup_pagination_test();

    let rows = [
        (21_101, 10, "t10"),
        (21_102, 20, "t20"),
        (21_103, 30, "t30"),
        (21_104, 40, "t40"),
        (21_105, 50, "t50"),
    ];
    seed_indexed_metrics_rows(&rows);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);

    // Phase 1: capture the first descending page and its continuation cursor.
    let page1 = execute_indexed_metrics_tag_page_desc(
        &load,
        tag_between_equivalent_predicate(10, 50),
        1,
        None,
        "single-field desc upper-anchor page1",
    );
    assert_eq!(
        indexed_metric_ids_from_response(&page1.items),
        vec![Ulid::from_u128(21_105)],
        "descending first page should start at the upper envelope row",
    );

    let encoded_cursor = page1
        .next_cursor
        .as_ref()
        .expect("single-field desc upper-anchor page1 should emit continuation cursor")
        .encode()
        .expect("continuation cursor should serialize");

    // Phase 2: resuming from that anchor should return the remaining suffix in order.
    let resume = execute_indexed_metrics_tag_page_desc(
        &load,
        tag_between_equivalent_predicate(10, 50),
        10,
        Some(encoded_cursor.as_slice()),
        "single-field desc upper-anchor resume",
    );
    assert_eq!(
        indexed_metric_ids_from_response(&resume.items),
        vec![
            Ulid::from_u128(21_104),
            Ulid::from_u128(21_103),
            Ulid::from_u128(21_102),
            Ulid::from_u128(21_101),
        ],
        "descending resume from the upper anchor must continue with the remaining lower rows",
    );
}

#[test]
fn load_single_field_range_cursor_boundaries_respect_lower_and_upper_edges() {
    setup_pagination_test();

    let rows = [
        (21_001, 10, "t10-a"),
        (21_002, 10, "t10-b"),
        (21_003, 20, "t20"),
        (21_004, 25, "t25"),
        (21_005, 30, "t30"),
        (21_006, 5, "t5"),
    ];
    seed_indexed_metrics_rows(&rows);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);
    let half_open_predicate = Predicate::And(vec![
        strict_compare_predicate("tag", CompareOp::Gte, Value::Uint(10)),
        strict_compare_predicate("tag", CompareOp::Lt, Value::Uint(30)),
    ]);

    // Phase 1: capture the full half-open ascending window.
    let base_page = execute_indexed_metrics_tag_page_asc_from_boundary(
        &load,
        half_open_predicate.clone(),
        10,
        None,
        "single-field range boundary base page",
    );
    let all_ids = indexed_metric_ids_from_response(&base_page.items);
    assert_eq!(
        all_ids,
        vec![
            Ulid::from_u128(21_001),
            Ulid::from_u128(21_002),
            Ulid::from_u128(21_003),
            Ulid::from_u128(21_004),
        ],
        "single-field half-open range should include only rows in [10, 30)",
    );

    // Phase 2: a lower-edge duplicate boundary must skip only that row.
    let first_resume = execute_indexed_metrics_tag_page_asc_from_boundary(
        &load,
        half_open_predicate,
        10,
        Some(indexed_metric_tag_id_boundary(10, 21_001)),
        "single-field range boundary first resume",
    );
    assert_eq!(
        indexed_metric_ids_from_response(&first_resume.items),
        all_ids[1..].to_vec(),
        "ascending boundary at the first lower-edge row should skip only that row",
    );

    let terminal_resume = execute_indexed_metrics_tag_page_asc_from_boundary(
        &load,
        Predicate::And(vec![
            strict_compare_predicate("tag", CompareOp::Gte, Value::Uint(10)),
            strict_compare_predicate("tag", CompareOp::Lt, Value::Uint(30)),
        ]),
        10,
        Some(indexed_metric_tag_id_boundary(25, 21_004)),
        "single-field range boundary terminal resume",
    );
    assert!(
        terminal_resume.items.is_empty(),
        "cursor boundary at the upper edge row should return an empty continuation page",
    );
    assert!(
        terminal_resume.next_cursor.is_none(),
        "single-field empty continuation page should not emit a cursor",
    );
}

#[test]
fn load_single_field_between_equivalent_pushdown_matches_expected_order() {
    setup_pagination_test();

    let rows = [
        (19_101, 30, "t30"),
        (19_102, 10, "t10-a"),
        (19_103, 10, "t10-b"),
        (19_104, 20, "t20"),
        (19_105, 40, "t40"),
        (19_106, 5, "t5"),
    ];
    seed_indexed_metrics_rows(&rows);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);

    let response = load
        .execute(
            Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
                .filter(tag_between_equivalent_predicate(10, 30))
                .order_by("tag")
                .plan()
                .map(ExecutablePlan::from)
                .expect("single-field between-equivalent plan should build"),
        )
        .expect("single-field between-equivalent execution should succeed");
    assert_eq!(
        indexed_metric_ids_from_response(&response),
        indexed_metrics_ids_in_between_equivalent_range(&rows, 10, 30),
        "single-field between-equivalent range should match the ordered fallback row set",
    );
}

#[test]
fn load_single_field_range_pushdown_handles_min_and_max_tag_edges() {
    const MAX_TAG: u32 = u32::MAX;

    setup_pagination_test();

    let rows = [
        (19_301, 0, "t0"),
        (19_302, 1, "t1"),
        (19_303, 50, "t50"),
        (19_304, MAX_TAG - 1, "tmax-1"),
        (19_305, MAX_TAG, "tmax"),
    ];
    seed_indexed_metrics_rows(&rows);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);

    // Phase 1: the exclusive upper bound must stop before the max-value group.
    let exclusive = load
        .execute(
            Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
                .filter(tag_range_predicate(0, MAX_TAG))
                .order_by("tag")
                .plan()
                .map(ExecutablePlan::from)
                .expect("single-field extreme-edge exclusive plan should build"),
        )
        .expect("single-field extreme-edge exclusive execution should succeed");
    assert_eq!(
        indexed_metric_ids_from_response(&exclusive),
        indexed_metrics_ids_in_tag_range(&rows, 0, MAX_TAG),
        "exclusive upper-bound range must exclude rows at the max field value",
    );

    // Phase 2: the inclusive equivalent must admit the max-value group.
    let inclusive = load
        .execute(
            Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
                .filter(tag_between_equivalent_predicate(0, MAX_TAG))
                .order_by("tag")
                .plan()
                .map(ExecutablePlan::from)
                .expect("single-field extreme-edge inclusive plan should build"),
        )
        .expect("single-field extreme-edge inclusive execution should succeed");
    let inclusive_ids = indexed_metric_ids_from_response(&inclusive);
    assert_eq!(
        inclusive_ids,
        indexed_metrics_ids_in_between_equivalent_range(&rows, 0, MAX_TAG),
        "inclusive upper-bound range must include rows at the max field value",
    );
    assert!(
        inclusive_ids.contains(&Ulid::from_u128(19_305)),
        "inclusive upper-bound range must retain the exact max-value row",
    );
}

#[test]
fn load_unique_index_range_cursor_pagination_matches_unbounded_case_f() {
    setup_pagination_test();

    let rows = [
        (23_001, 5, "c5"),
        (23_002, 10, "c10"),
        (23_003, 20, "c20"),
        (23_004, 30, "c30"),
        (23_005, 40, "c40"),
        (23_006, 55, "c55"),
        (23_007, 70, "c70"),
    ];
    seed_unique_index_range_rows(&rows);

    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    let predicate = unique_code_range_predicate(10, 60);

    // Phase 1: capture the unbounded baseline for ids and serialized rows.
    let unbounded = load
        .execute(
            Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .order_by("code")
                .plan()
                .map(ExecutablePlan::from)
                .expect("unique unbounded plan should build"),
        )
        .expect("unique unbounded execution should succeed");
    let unbounded_ids = unique_index_range_ids_from_response(&unbounded);
    let unbounded_row_bytes: Vec<Vec<u8>> = unbounded
        .iter()
        .map(|row| {
            crate::serialize::serialize(row.entity_ref())
                .expect("unique unbounded row serialization should succeed")
        })
        .collect();

    // Phase 2: walk the paged lane and verify anchor monotonicity and byte-for-byte parity.
    let mut paged_ids = Vec::new();
    let mut paged_row_bytes = Vec::new();
    let mut encoded_cursor = None::<Vec<u8>>;
    let mut previous_anchor = None::<Vec<u8>>;
    for _ in 0..8 {
        let page = execute_unique_index_range_code_page_asc(
            &load,
            predicate.clone(),
            2,
            encoded_cursor.as_deref(),
            "unique index-range paged execution",
        );
        paged_ids.extend(unique_index_range_ids_from_response(&page.items));
        paged_row_bytes.extend(page.items.iter().map(|row| {
            crate::serialize::serialize(row.entity_ref())
                .expect("unique paged row serialization should succeed")
        }));

        let Some(cursor) = page.next_cursor else {
            break;
        };
        let scalar = cursor
            .as_scalar()
            .expect("unique continuation cursor should stay scalar");
        let anchor = scalar
            .index_range_anchor()
            .expect("unique index-range cursor should include a raw-key anchor")
            .last_raw_key()
            .to_vec();
        if let Some(previous_anchor) = previous_anchor.as_ref() {
            assert!(
                previous_anchor < &anchor,
                "unique index-range continuation anchors must advance strictly",
            );
        }
        previous_anchor = Some(anchor);
        encoded_cursor = Some(
            cursor
                .encode()
                .expect("unique continuation cursor should serialize"),
        );
    }

    let unique_paged_ids: std::collections::BTreeSet<Ulid> = paged_ids.iter().copied().collect();
    assert_eq!(
        unique_paged_ids.len(),
        paged_ids.len(),
        "unique index-range pagination must not emit duplicate rows",
    );
    assert_eq!(
        paged_ids, unbounded_ids,
        "unique index-range paginated ids must match unbounded ids in order",
    );
    assert_eq!(
        paged_row_bytes, unbounded_row_bytes,
        "unique index-range paginated rows must match unbounded rows byte-for-byte",
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn load_index_range_cursor_property_matrix_preserves_union_monotonicity_and_resume_suffix(
        codes in proptest::collection::btree_set(1u32..2000u32, 5..18),
        start_seed in any::<u8>(),
        span_seed in any::<u8>(),
        limit in 1u32..6u32,
    ) {
        setup_pagination_test();

        let sorted_codes = codes.into_iter().collect::<Vec<_>>();
        let start = usize::from(start_seed) % sorted_codes.len();
        let max_span = sorted_codes.len() - start;
        let span = (usize::from(span_seed) % max_span).saturating_add(1);
        let end = start.saturating_add(span);
        let lower = sorted_codes[start];
        let upper = if end < sorted_codes.len() {
            sorted_codes[end]
        } else {
            sorted_codes
                .last()
                .copied()
                .expect("generated code set should be non-empty")
                .saturating_add(1)
        };
        prop_assume!(upper > lower);

        let save = SaveExecutor::<UniqueIndexRangeEntity>::new(DB, false);
        for (row_index, code) in sorted_codes.iter().copied().enumerate() {
            save.insert(UniqueIndexRangeEntity {
                id: Ulid::from_u128(31_000_000 + row_index as u128),
                code,
                label: format!("code-{code}"),
            })
            .expect("property seed row save should succeed");
        }

        let predicate = unique_code_range_predicate(lower, upper);
        let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
        let unbounded = load
            .execute(
                Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
                    .filter(predicate.clone())
                    .order_by("code")
                    .plan()
                    .map(ExecutablePlan::from)
                    .expect("property matrix unbounded plan should build"),
            )
            .expect("property matrix unbounded execution should succeed");
        let expected_ids = unique_index_range_ids_from_response(&unbounded);

        let mut cursor = None::<Vec<u8>>;
        let mut paged_ids = Vec::new();
        let mut tokens = Vec::new();
        let mut anchors = Vec::new();
        let mut pages = 0usize;

        loop {
            pages = pages.saturating_add(1);
            prop_assert!(
                pages <= 64,
                "property matrix pagination should terminate in bounded pages",
            );

            let page = execute_unique_index_range_code_page_asc(
                &load,
                predicate.clone(),
                limit,
                cursor.as_deref(),
                "property matrix paged execution",
            );
            paged_ids.extend(unique_index_range_ids_from_response(&page.items));

            let Some(next_cursor) = page.next_cursor else {
                break;
            };

            let next_cursor_bytes = encode_token(
                &next_cursor,
                "property matrix continuation cursor should serialize",
            );
            assert_anchor_monotonic(
                &mut anchors,
                next_cursor_bytes.as_slice(),
                "property matrix continuation cursor should decode",
                "property matrix index-range cursor should include a raw-key anchor",
                "property matrix continuation anchors must advance strictly",
            );
            tokens.push(next_cursor_bytes.clone());
            cursor = Some(next_cursor_bytes);
        }

        prop_assert_eq!(
            paged_ids.as_slice(),
            expected_ids.as_slice(),
            "property matrix paginated union must equal unbounded full scan",
        );
        let unique_ids: BTreeSet<Ulid> = paged_ids.iter().copied().collect();
        prop_assert_eq!(
            unique_ids.len(),
            paged_ids.len(),
            "property matrix pagination must not emit duplicates",
        );

        assert_unique_range_resume_suffixes_from_tokens(
            &load,
            predicate,
            limit,
            tokens.as_slice(),
            expected_ids.as_slice(),
        );
    }
}

#[test]
fn load_single_field_range_pushdown_parity_matrix_is_table_driven() {
    const GT_10: RangeBounds = &[(CompareOp::Gt, 10)];
    const GTE_10: RangeBounds = &[(CompareOp::Gte, 10)];
    const LT_30: RangeBounds = &[(CompareOp::Lt, 30)];
    const LTE_30: RangeBounds = &[(CompareOp::Lte, 30)];
    const GTE_10_LT_30: RangeBounds = &[(CompareOp::Gte, 10), (CompareOp::Lt, 30)];
    const GT_10_LTE_30: RangeBounds = &[(CompareOp::Gt, 10), (CompareOp::Lte, 30)];
    const BETWEEN_10_30: RangeBounds = &[(CompareOp::Gte, 10), (CompareOp::Lte, 30)];
    const GT_40_NO_MATCH: RangeBounds = &[(CompareOp::Gt, 40)];
    const LTE_40_ALL: RangeBounds = &[(CompareOp::Lte, 40)];

    let cases = [
        RangeMatrixCase {
            name: "gt_only",
            bounds: GT_10,
            descending: false,
        },
        RangeMatrixCase {
            name: "gte_only",
            bounds: GTE_10,
            descending: false,
        },
        RangeMatrixCase {
            name: "lt_only_desc",
            bounds: LT_30,
            descending: true,
        },
        RangeMatrixCase {
            name: "lte_only",
            bounds: LTE_30,
            descending: false,
        },
        RangeMatrixCase {
            name: "gte_lt_window",
            bounds: GTE_10_LT_30,
            descending: false,
        },
        RangeMatrixCase {
            name: "gt_lte_window_desc",
            bounds: GT_10_LTE_30,
            descending: true,
        },
        RangeMatrixCase {
            name: "between_equivalent",
            bounds: BETWEEN_10_30,
            descending: false,
        },
        RangeMatrixCase {
            name: "no_match",
            bounds: GT_40_NO_MATCH,
            descending: false,
        },
        RangeMatrixCase {
            name: "all_rows",
            bounds: LTE_40_ALL,
            descending: false,
        },
    ];

    setup_pagination_test();
    let rows = [
        (23_001, 0, "t0"),
        (23_002, 10, "t10-a"),
        (23_003, 10, "t10-b"),
        (23_004, 20, "t20"),
        (23_005, 30, "t30"),
        (23_006, 40, "t40"),
    ];
    seed_indexed_metrics_rows(&rows);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);
    for case in cases {
        let query = Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
            .filter(predicate_from_field_bounds("tag", case.bounds));
        let executed = if case.descending {
            load.execute(query.order_by_desc("tag").plan().map_or_else(
                |_| panic!("single-field {} desc plan should build", case.name),
                ExecutablePlan::from,
            ))
        } else {
            load.execute(query.order_by("tag").plan().map_or_else(
                |_| panic!("single-field {} asc plan should build", case.name),
                ExecutablePlan::from,
            ))
        }
        .unwrap_or_else(|_| panic!("single-field {} execution should succeed", case.name));

        assert_eq!(
            indexed_metric_ids_from_response(&executed),
            ordered_indexed_metrics_ids_for_bounds(&rows, case.bounds, case.descending),
            "single-field {} range case should match ordered fallback ids",
            case.name,
        );
    }
}

#[test]
fn load_composite_range_pushdown_parity_matrix_is_table_driven() {
    const GT_10: RangeBounds = &[(CompareOp::Gt, 10)];
    const GTE_10: RangeBounds = &[(CompareOp::Gte, 10)];
    const LT_30: RangeBounds = &[(CompareOp::Lt, 30)];
    const LTE_30: RangeBounds = &[(CompareOp::Lte, 30)];
    const GTE_10_LT_40: RangeBounds = &[(CompareOp::Gte, 10), (CompareOp::Lt, 40)];
    const GT_10_LTE_40: RangeBounds = &[(CompareOp::Gt, 10), (CompareOp::Lte, 40)];
    const BETWEEN_10_30: RangeBounds = &[(CompareOp::Gte, 10), (CompareOp::Lte, 30)];
    const GT_50_NO_MATCH: RangeBounds = &[(CompareOp::Gt, 50)];
    const LTE_50_ALL: RangeBounds = &[(CompareOp::Lte, 50)];

    let cases = [
        RangeMatrixCase {
            name: "gt_only",
            bounds: GT_10,
            descending: false,
        },
        RangeMatrixCase {
            name: "gte_only",
            bounds: GTE_10,
            descending: false,
        },
        RangeMatrixCase {
            name: "lt_only_desc",
            bounds: LT_30,
            descending: true,
        },
        RangeMatrixCase {
            name: "lte_only",
            bounds: LTE_30,
            descending: false,
        },
        RangeMatrixCase {
            name: "gte_lt_window",
            bounds: GTE_10_LT_40,
            descending: false,
        },
        RangeMatrixCase {
            name: "gt_lte_window_desc",
            bounds: GT_10_LTE_40,
            descending: true,
        },
        RangeMatrixCase {
            name: "between_equivalent",
            bounds: BETWEEN_10_30,
            descending: false,
        },
        RangeMatrixCase {
            name: "no_match",
            bounds: GT_50_NO_MATCH,
            descending: false,
        },
        RangeMatrixCase {
            name: "all_rows",
            bounds: LTE_50_ALL,
            descending: false,
        },
    ];

    setup_pagination_test();
    let rows = [
        (24_001, 7, 0, "g7-r0"),
        (24_002, 7, 10, "g7-r10-a"),
        (24_003, 7, 10, "g7-r10-b"),
        (24_004, 7, 20, "g7-r20"),
        (24_005, 7, 30, "g7-r30"),
        (24_006, 7, 40, "g7-r40"),
        (24_007, 8, 15, "g8-r15"),
        (24_008, 7, 50, "g7-r50"),
    ];
    seed_pushdown_rows(&rows);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    for case in cases {
        let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(predicate_from_group_rank_bounds(7, case.bounds));
        let executed = if case.descending {
            load.execute(query.order_by_desc("rank").plan().map_or_else(
                |_| panic!("composite {} desc plan should build", case.name),
                ExecutablePlan::from,
            ))
        } else {
            load.execute(query.order_by("rank").plan().map_or_else(
                |_| panic!("composite {} asc plan should build", case.name),
                ExecutablePlan::from,
            ))
        }
        .unwrap_or_else(|_| panic!("composite {} execution should succeed", case.name));

        assert_eq!(
            pushdown_ids_from_response(&executed),
            ordered_pushdown_ids_for_group_rank_bounds(&rows, 7, case.bounds, case.descending),
            "composite {} range case should match ordered fallback ids",
            case.name,
        );
    }
}

#[test]
fn load_single_field_desc_range_resume_from_lower_boundary_returns_empty() {
    setup_pagination_test();

    let rows = [
        (21_201, 10, "t10"),
        (21_202, 20, "t20"),
        (21_203, 30, "t30"),
    ];
    seed_indexed_metrics_rows(&rows);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);
    let resume = execute_indexed_metrics_tag_page_desc_from_boundary(
        &load,
        tag_between_equivalent_predicate(10, 30),
        10,
        Some(indexed_metric_tag_id_boundary(10, 21_201)),
        "single-field desc lower-boundary resume",
    );
    assert!(
        resume.items.is_empty(),
        "descending resume from the lower boundary row must return an empty page",
    );
    assert!(
        resume.next_cursor.is_none(),
        "empty descending continuation page must not emit a cursor",
    );
}

#[test]
fn load_single_field_desc_range_single_element_resume_returns_empty() {
    setup_pagination_test();

    let rows = [
        (21_301, 20, "t20"),
        (21_302, 30, "t30"),
        (21_303, 40, "t40"),
    ];
    seed_indexed_metrics_rows(&rows);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);
    let page1 = execute_indexed_metrics_tag_page_desc(
        &load,
        tag_between_equivalent_predicate(30, 30),
        1,
        None,
        "single-element desc page1",
    );
    assert_eq!(
        indexed_metric_ids_from_response(&page1.items),
        vec![Ulid::from_u128(21_302)],
        "single-element descending range should return the only row",
    );
    assert!(
        page1.next_cursor.is_none(),
        "single-element descending first page should not emit a cursor",
    );

    let resume = execute_indexed_metrics_tag_page_desc_from_boundary(
        &load,
        tag_between_equivalent_predicate(30, 30),
        1,
        Some(indexed_metric_tag_id_boundary(30, 21_302)),
        "single-element desc explicit resume",
    );
    assert!(
        resume.items.is_empty(),
        "resuming a single-element descending range must return an empty page",
    );
    assert!(
        resume.next_cursor.is_none(),
        "single-element empty resume must not emit a cursor",
    );
}

#[test]
fn load_single_field_desc_range_multi_page_has_no_duplicate_or_omission() {
    setup_pagination_test();

    let rows = [
        (21_401, 10, "A"),
        (21_402, 20, "B"),
        (21_403, 30, "C"),
        (21_404, 40, "D"),
        (21_405, 50, "E"),
    ];
    seed_indexed_metrics_rows(&rows);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);

    // Phase 1: walk the descending range one continuation at a time.
    let page1 = execute_indexed_metrics_tag_page_desc(
        &load,
        tag_between_equivalent_predicate(10, 50),
        2,
        None,
        "multi-page desc page1",
    );
    assert_eq!(
        indexed_metric_ids_from_response(&page1.items),
        vec![Ulid::from_u128(21_405), Ulid::from_u128(21_404)],
        "descending page1 should return E, D",
    );
    let page1_cursor = page1
        .next_cursor
        .as_ref()
        .expect("multi-page desc page1 should emit continuation cursor")
        .encode()
        .expect("continuation cursor should serialize");

    let page2 = execute_indexed_metrics_tag_page_desc(
        &load,
        tag_between_equivalent_predicate(10, 50),
        2,
        Some(page1_cursor.as_slice()),
        "multi-page desc page2",
    );
    assert_eq!(
        indexed_metric_ids_from_response(&page2.items),
        vec![Ulid::from_u128(21_403), Ulid::from_u128(21_402)],
        "descending page2 should return C, B",
    );
    let page2_cursor = page2
        .next_cursor
        .as_ref()
        .expect("multi-page desc page2 should emit continuation cursor")
        .encode()
        .expect("continuation cursor should serialize");

    let page3 = execute_indexed_metrics_tag_page_desc(
        &load,
        tag_between_equivalent_predicate(10, 50),
        2,
        Some(page2_cursor.as_slice()),
        "multi-page desc page3",
    );
    assert_eq!(
        indexed_metric_ids_from_response(&page3.items),
        vec![Ulid::from_u128(21_401)],
        "descending page3 should return A",
    );
    assert!(
        page3.next_cursor.is_none(),
        "final descending page should not emit a continuation cursor",
    );

    // Phase 2: concatenated pages must match the full descending suffix exactly once.
    let mut all_ids = indexed_metric_ids_from_response(&page1.items);
    all_ids.extend(indexed_metric_ids_from_response(&page2.items));
    all_ids.extend(indexed_metric_ids_from_response(&page3.items));
    assert_eq!(
        all_ids,
        vec![
            Ulid::from_u128(21_405),
            Ulid::from_u128(21_404),
            Ulid::from_u128(21_403),
            Ulid::from_u128(21_402),
            Ulid::from_u128(21_401),
        ],
        "descending pagination must not omit rows and must preserve strict order",
    );
    let unique_ids: std::collections::BTreeSet<Ulid> = all_ids.iter().copied().collect();
    assert_eq!(
        unique_ids.len(),
        all_ids.len(),
        "descending pagination must not duplicate rows",
    );
}

#[test]
fn load_single_field_desc_range_mixed_edges_resume_inside_duplicate_group() {
    setup_pagination_test();

    let rows = [
        (21_501, 10, "t10-a"),
        (21_502, 10, "t10-b"),
        (21_503, 20, "t20-a"),
        (21_504, 20, "t20-b"),
        (21_505, 30, "t30-a"),
        (21_506, 30, "t30-b"),
        (21_507, 40, "t40"),
    ];
    seed_indexed_metrics_rows(&rows);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);
    // Phase 1: capture the full descending mixed-edge window.
    let base_page = execute_indexed_metrics_tag_page_desc(
        &load,
        Predicate::And(vec![
            strict_compare_predicate("tag", CompareOp::Gt, Value::Uint(10)),
            strict_compare_predicate("tag", CompareOp::Lte, Value::Uint(30)),
        ]),
        10,
        None,
        "single-field mixed-edge desc base page",
    );
    let all_ids = indexed_metric_ids_from_response(&base_page.items);
    assert_eq!(
        all_ids,
        vec![
            Ulid::from_u128(21_505),
            Ulid::from_u128(21_506),
            Ulid::from_u128(21_503),
            Ulid::from_u128(21_504),
        ],
        "descending mixed-edge range should preserve duplicate-group order with canonical PK tie-break",
    );

    // Phase 2: a boundary inside the upper duplicate group must resume strictly after that row.
    let first_resume = execute_indexed_metrics_tag_page_desc_from_boundary(
        &load,
        Predicate::And(vec![
            strict_compare_predicate("tag", CompareOp::Gt, Value::Uint(10)),
            strict_compare_predicate("tag", CompareOp::Lte, Value::Uint(30)),
        ]),
        10,
        Some(indexed_metric_tag_id_boundary(30, 21_505)),
        "single-field mixed-edge desc first resume",
    );
    assert_eq!(
        indexed_metric_ids_from_response(&first_resume.items),
        all_ids[1..].to_vec(),
        "descending mixed-edge resume should continue at the sibling row and then lower groups",
    );

    let terminal_resume = execute_indexed_metrics_tag_page_desc_from_boundary(
        &load,
        Predicate::And(vec![
            strict_compare_predicate("tag", CompareOp::Gt, Value::Uint(10)),
            strict_compare_predicate("tag", CompareOp::Lte, Value::Uint(30)),
        ]),
        10,
        Some(indexed_metric_tag_id_boundary(20, 21_504)),
        "single-field mixed-edge desc terminal resume",
    );
    assert!(
        terminal_resume.items.is_empty(),
        "descending mixed-edge range should be exhausted after the lower-edge terminal boundary",
    );
    assert!(
        terminal_resume.next_cursor.is_none(),
        "empty descending mixed-edge continuation page must not emit a cursor",
    );
}

#[test]
fn load_trace_marks_secondary_order_pushdown_outcomes() {
    #[derive(Clone, Copy)]
    struct Case {
        name: &'static str,
        prefix: u128,
        order: [(&'static str, OrderDirection); 2],
        include_filter: bool,
    }

    let cases = [
        Case {
            name: "accepted_ascending",
            prefix: 16_000,
            order: [("rank", OrderDirection::Asc), ("id", OrderDirection::Asc)],
            include_filter: true,
        },
        Case {
            name: "accepted_with_filter",
            prefix: 17_000,
            order: [("rank", OrderDirection::Asc), ("id", OrderDirection::Asc)],
            include_filter: true,
        },
        Case {
            name: "rejected_descending",
            prefix: 18_000,
            order: [("rank", OrderDirection::Desc), ("id", OrderDirection::Asc)],
            include_filter: true,
        },
    ];

    setup_pagination_test();

    for case in cases {
        reset_store();
        seed_pushdown_rows(&pushdown_trace_rows(case.prefix));

        let mut query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore).limit(1);
        if case.include_filter {
            query = query.filter(pushdown_group_predicate(7));
        }
        for (field, direction) in case.order {
            query = match direction {
                OrderDirection::Asc => query.order_by(field),
                OrderDirection::Desc => query.order_by_desc(field),
            };
        }

        let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);
        let (_page, trace) = load
            .execute_paged_with_cursor_traced(
                query
                    .plan()
                    .map(ExecutablePlan::from)
                    .expect("trace outcome plan should build for case"),
                None,
            )
            .expect("trace outcome execution should succeed for case");
        let trace = trace.expect("debug trace should be present");
        assert!(
            trace.optimization().is_none(),
            "trace should emit expected secondary-order pushdown outcome for case '{}'",
            case.name,
        );
    }
}

#[test]
fn load_trace_marks_composite_index_range_pushdown_rejection_outcome() {
    setup_pagination_test();
    seed_pushdown_rows(&pushdown_trace_rows(22_000));

    let plan = ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery {
        logical: LogicalPlan::Scalar(ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(1),
                offset: 0,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::index_range(
                PUSHDOWN_PARITY_INDEX_MODELS[0],
                vec![Value::Uint(7)],
                std::ops::Bound::Included(Value::Uint(10)),
                std::ops::Bound::Excluded(Value::Uint(20)),
            )),
            AccessPlan::path(AccessPath::FullScan),
        ]),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
    });

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);
    let (_page, trace) = load
        .execute_paged_with_cursor_traced(plan, None)
        .expect("composite-index-range trace test execution should succeed");
    let trace = trace.expect("debug trace should be present");
    assert!(
        trace.optimization().is_none(),
        "composite access with index-range child should not emit secondary-order pushdown traces",
    );
}

#[test]
fn load_distinct_flag_preserves_union_pagination_rows_and_boundaries() {
    setup_pagination_test();

    let rows = [
        (39_201, 7, 10, "g7-r10"),
        (39_202, 7, 20, "g7-r20-a"),
        (39_203, 7, 20, "g7-r20-b"),
        (39_204, 7, 30, "g7-r30"),
        (39_205, 8, 15, "g8-r15"),
    ];
    seed_pushdown_rows(&rows);

    let build_plan = |distinct: bool, limit: u32| {
        ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery {
            logical: LogicalPlan::Scalar(ScalarPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: Some(OrderSpec {
                    fields: vec![
                        ("rank".to_string(), OrderDirection::Desc),
                        ("id".to_string(), OrderDirection::Asc),
                    ],
                }),
                distinct,
                delete_limit: None,
                page: Some(PageSpec {
                    limit: Some(limit),
                    offset: 0,
                }),
                consistency: MissingRowPolicy::Ignore,
            }),
            access: AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![
                    Ulid::from_u128(39_201),
                    Ulid::from_u128(39_202),
                    Ulid::from_u128(39_204),
                ])),
                AccessPlan::path(AccessPath::ByKeys(vec![
                    Ulid::from_u128(39_202),
                    Ulid::from_u128(39_203),
                    Ulid::from_u128(39_205),
                ])),
            ])
            .into_value_plan(),
            projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        })
    };

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    for limit in [1_u32, 2, 3] {
        let (plain_ids, plain_boundaries) =
            collect_pushdown_pages_from_executable_plan(&load, || build_plan(false, limit), 12);
        let (distinct_ids, distinct_boundaries) =
            collect_pushdown_pages_from_executable_plan(&load, || build_plan(true, limit), 12);

        assert_eq!(
            plain_ids, distinct_ids,
            "distinct on/off should preserve canonical row order for limit={limit}",
        );
        assert_eq!(
            plain_boundaries, distinct_boundaries,
            "distinct on/off should preserve continuation boundaries for limit={limit}",
        );
    }
}

#[test]
fn load_row_distinct_keeps_rows_with_same_projected_values_when_datakey_differs() {
    setup_pagination_test();

    let rows = [
        (39_211, 7, 10, "g7-r10-a"),
        (39_212, 7, 10, "g7-r10-b"),
        (39_213, 7, 20, "g7-r20"),
        (39_214, 8, 99, "g8-r99"),
    ];
    seed_pushdown_rows(&rows);

    let expected_ids = vec![
        Ulid::from_u128(39_211),
        Ulid::from_u128(39_212),
        Ulid::from_u128(39_213),
    ];

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let response = load
        .execute(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(pushdown_group_predicate(7))
                .distinct()
                .order_by("id")
                .plan()
                .map(ExecutablePlan::from)
                .expect("row DISTINCT projection-invariant plan should build"),
        )
        .expect("row DISTINCT projection-invariant execution should succeed");

    assert_eq!(
        pushdown_ids_from_response(&response),
        expected_ids,
        "row DISTINCT must keep rows with different DataKeys even when projected values are equal",
    );

    let projected_ranks = response
        .iter()
        .map(|row| row.entity_ref().rank)
        .collect::<Vec<_>>();
    assert_eq!(
        projected_ranks,
        vec![10, 10, 20],
        "equal projected values should remain visible when DataKey identities differ",
    );
}

#[test]
fn load_distinct_union_resume_matrix_is_boundary_complete() {
    setup_pagination_test();

    let rows = [
        (39_301, 7, 10, "g7-r10"),
        (39_302, 7, 20, "g7-r20"),
        (39_303, 7, 30, "g7-r30"),
        (39_304, 7, 40, "g7-r40"),
        (39_305, 7, 50, "g7-r50"),
        (39_306, 8, 10, "g8-r10"),
        (39_307, 8, 20, "g8-r20"),
        (39_308, 8, 30, "g8-r30"),
        (39_309, 9, 10, "g9-r10"),
    ];
    seed_pushdown_rows(&rows);

    let ids = rows
        .iter()
        .map(|(id, _, _, _)| Ulid::from_u128(*id))
        .collect::<Vec<_>>();
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    for (case_name, order_direction) in
        [("asc", OrderDirection::Asc), ("desc", OrderDirection::Desc)]
    {
        let expected_ids = if order_direction == OrderDirection::Asc {
            ids.clone()
        } else {
            ids.iter().copied().rev().collect()
        };

        for limit in [1_u32, 2, 3] {
            let build_plan = || {
                ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery {
                    logical: LogicalPlan::Scalar(ScalarPlan {
                        mode: QueryMode::Load(LoadSpec::new()),
                        predicate: None,
                        order: Some(OrderSpec {
                            fields: vec![("id".to_string(), order_direction)],
                        }),
                        distinct: true,
                        delete_limit: None,
                        page: Some(PageSpec {
                            limit: Some(limit),
                            offset: 0,
                        }),
                        consistency: MissingRowPolicy::Ignore,
                    }),
                    access: AccessPlan::Union(vec![
                        AccessPlan::path(AccessPath::ByKeys(vec![
                            Ulid::from_u128(39_301),
                            Ulid::from_u128(39_302),
                            Ulid::from_u128(39_303),
                            Ulid::from_u128(39_304),
                            Ulid::from_u128(39_305),
                        ])),
                        AccessPlan::path(AccessPath::ByKeys(vec![
                            Ulid::from_u128(39_303),
                            Ulid::from_u128(39_304),
                            Ulid::from_u128(39_305),
                            Ulid::from_u128(39_306),
                            Ulid::from_u128(39_307),
                        ])),
                        AccessPlan::path(AccessPath::ByKeys(vec![
                            Ulid::from_u128(39_305),
                            Ulid::from_u128(39_306),
                            Ulid::from_u128(39_307),
                            Ulid::from_u128(39_308),
                            Ulid::from_u128(39_309),
                        ])),
                    ])
                    .into_value_plan(),
                    projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
                })
            };

            let (distinct_ids, boundaries) =
                collect_pushdown_pages_from_executable_plan(&load, build_plan, 30);
            assert_eq!(
                distinct_ids, expected_ids,
                "case '{case_name}' with limit={limit} should preserve distinct canonical ordering",
            );

            let unique: BTreeSet<Ulid> = distinct_ids.iter().copied().collect();
            assert_eq!(
                unique.len(),
                distinct_ids.len(),
                "case '{case_name}' with limit={limit} distinct pagination must not duplicate rows",
            );

            let context = format!("case '{case_name}' with limit={limit}");
            assert_pushdown_resume_suffixes_from_boundaries(
                &load,
                &build_plan,
                &boundaries,
                &expected_ids,
                context.as_str(),
            );
        }
    }
}

#[test]
fn load_distinct_desc_secondary_pushdown_resume_matrix_is_boundary_complete() {
    setup_pagination_test();

    let rows = [
        (39_401, 7, 10, "g7-r10"),
        (39_402, 7, 20, "g7-r20-a"),
        (39_403, 7, 20, "g7-r20-b"),
        (39_404, 7, 30, "g7-r30"),
        (39_405, 7, 40, "g7-r40"),
        (39_406, 8, 10, "g8-r10"),
        (39_407, 8, 20, "g8-r20"),
        (39_408, 8, 30, "g8-r30"),
        (39_409, 9, 10, "g9-r10"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let expected_ids = ordered_pushdown_ids_with_rank_and_id_direction(&rows, 7, true, true);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);

    for limit in [1_u32, 2, 3] {
        let (seed_page, seed_trace) = load
            .execute_paged_with_cursor_traced(
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .filter(predicate.clone())
                    .order_by_desc("rank")
                    .order_by_desc("id")
                    .distinct()
                    .limit(limit)
                    .plan()
                    .map(ExecutablePlan::from)
                    .expect("distinct secondary DESC seed plan should build"),
                None,
            )
            .expect("distinct secondary DESC seed page should execute");
        let seed_trace = seed_trace.expect("debug trace should be present");
        assert_eq!(
            seed_trace.optimization(),
            None,
            "distinct DESC residual-filter plan should remain materialized for limit={limit}",
        );
        let _ = seed_page;

        let build_plan = || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .order_by_desc("rank")
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .plan()
                .map(ExecutablePlan::from)
                .expect("distinct secondary DESC plan should build")
        };

        let (distinct_ids, boundaries) =
            collect_pushdown_pages_from_executable_plan(&load, build_plan, 20);
        assert_eq!(
            distinct_ids, expected_ids,
            "distinct DESC secondary pushdown should preserve canonical ordering for limit={limit}",
        );

        let unique: BTreeSet<Ulid> = distinct_ids.iter().copied().collect();
        assert_eq!(
            unique.len(),
            distinct_ids.len(),
            "distinct DESC secondary pagination must not emit duplicates for limit={limit}",
        );

        let context = format!("distinct DESC secondary limit={limit}");
        assert_pushdown_resume_suffixes_from_boundaries(
            &load,
            &build_plan,
            &boundaries,
            &expected_ids,
            context.as_str(),
        );
    }
}

#[test]
fn load_distinct_desc_secondary_fast_path_and_fallback_match_ids_and_boundaries() {
    setup_pagination_test();

    let rows = [
        (39_501, 7, 10, "g7-r10"),
        (39_502, 7, 20, "g7-r20-a"),
        (39_503, 7, 20, "g7-r20-b"),
        (39_504, 7, 30, "g7-r30"),
        (39_505, 7, 40, "g7-r40"),
        (39_506, 8, 10, "g8-r10"),
        (39_507, 8, 20, "g8-r20"),
        (39_508, 8, 30, "g8-r30"),
        (39_509, 9, 10, "g9-r10"),
    ];
    seed_pushdown_rows(&rows);

    let group7_ids = pushdown_group_ids(&rows, 7);
    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);

    for limit in [1_u32, 2, 3] {
        let (_fast_seed_page, fast_trace) = load
            .execute_paged_with_cursor_traced(
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .filter(predicate.clone())
                    .order_by_desc("rank")
                    .order_by_desc("id")
                    .distinct()
                    .limit(limit)
                    .plan()
                    .map(ExecutablePlan::from)
                    .expect("distinct DESC fast-path seed plan should build"),
                None,
            )
            .expect("distinct DESC fast-path seed page should execute");
        let fast_trace = fast_trace.expect("debug trace should be present");
        assert_eq!(
            fast_trace.optimization(),
            None,
            "distinct DESC residual-filter seed execution should remain materialized for limit={limit}",
        );

        let (_fallback_seed_page, fallback_trace) = load
            .execute_paged_with_cursor_traced(
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .by_ids(group7_ids.iter().copied())
                    .order_by_desc("rank")
                    .order_by_desc("id")
                    .distinct()
                    .limit(limit)
                    .plan()
                    .map(ExecutablePlan::from)
                    .expect("distinct DESC fallback seed plan should build"),
                None,
            )
            .expect("distinct DESC fallback seed page should execute");
        let fallback_trace = fallback_trace.expect("debug trace should be present");
        assert_eq!(
            fallback_trace.optimization(),
            None,
            "distinct DESC by-ids fallback seed execution should not report fast-path optimization for limit={limit}",
        );

        let build_fast_plan = || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .order_by_desc("rank")
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .plan()
                .map(ExecutablePlan::from)
                .expect("distinct DESC fast-path plan should build")
        };
        let build_fallback_plan = || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .by_ids(group7_ids.iter().copied())
                .order_by_desc("rank")
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .plan()
                .map(ExecutablePlan::from)
                .expect("distinct DESC fallback plan should build")
        };

        let (fast_ids, fast_boundaries, _fast_tokens) =
            collect_pushdown_pages_from_executable_plan_with_tokens(&load, build_fast_plan, 20);
        let (fallback_ids, fallback_boundaries, _fallback_tokens) =
            collect_pushdown_pages_from_executable_plan_with_tokens(&load, build_fallback_plan, 20);

        assert_eq!(
            fast_ids, fallback_ids,
            "distinct DESC fast-path and fallback ids should match for limit={limit}",
        );
        assert_eq!(
            fast_boundaries, fallback_boundaries,
            "distinct DESC fast-path and fallback boundaries should match for limit={limit}",
        );
    }
}

#[test]
fn load_distinct_mixed_direction_secondary_shape_rejects_pushdown_and_matches_fallback() {
    setup_pagination_test();

    let rows = [
        (39_701, 7, 10, "g7-r10"),
        (39_702, 7, 20, "g7-r20-a"),
        (39_703, 7, 20, "g7-r20-b"),
        (39_704, 7, 30, "g7-r30"),
        (39_705, 7, 40, "g7-r40"),
        (39_706, 8, 10, "g8-r10"),
        (39_707, 8, 20, "g8-r20"),
        (39_708, 8, 30, "g8-r30"),
        (39_709, 9, 10, "g9-r10"),
    ];
    seed_pushdown_rows(&rows);
    let group7_ids = pushdown_group_ids(&rows, 7);

    let predicate = pushdown_group_predicate(7);
    let explain = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by_desc("rank")
        .order_by("id")
        .distinct()
        .explain()
        .expect("distinct mixed-direction explain should build");
    assert!(
        matches!(
            explain.order_pushdown(),
            crate::db::query::explain::ExplainOrderPushdown::MissingModelContext
        ),
        "query-layer explain should not evaluate secondary pushdown eligibility",
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);
    for limit in [1_u32, 2, 3] {
        let (_index_seed_page, index_seed_trace) = load
            .execute_paged_with_cursor_traced(
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .filter(predicate.clone())
                    .order_by_desc("rank")
                    .order_by("id")
                    .distinct()
                    .limit(limit)
                    .plan()
                    .map(ExecutablePlan::from)
                    .expect("distinct mixed-direction index-shape seed plan should build"),
                None,
            )
            .expect("distinct mixed-direction index-shape seed page should execute");
        let index_seed_trace = index_seed_trace.expect("debug trace should be present");
        assert_eq!(
            index_seed_trace.optimization(),
            None,
            "distinct mixed-direction index-shape seed execution should not report fast-path optimization for limit={limit}",
        );

        let build_index_shape_plan = || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .order_by_desc("rank")
                .order_by("id")
                .distinct()
                .limit(limit)
                .plan()
                .map(ExecutablePlan::from)
                .expect("distinct mixed-direction index-shape plan should build")
        };
        let build_fallback_plan = || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .by_ids(group7_ids.iter().copied())
                .order_by_desc("rank")
                .order_by("id")
                .distinct()
                .limit(limit)
                .plan()
                .map(ExecutablePlan::from)
                .expect("distinct mixed-direction fallback plan should build")
        };

        let (index_shape_ids, index_shape_boundaries) =
            collect_pushdown_pages_from_executable_plan(&load, build_index_shape_plan, 20);
        let (fallback_ids, fallback_boundaries) =
            collect_pushdown_pages_from_executable_plan(&load, build_fallback_plan, 20);
        assert_eq!(
            index_shape_ids, fallback_ids,
            "distinct mixed-direction index-shape and fallback ids should match for limit={limit}",
        );
        assert_eq!(
            index_shape_boundaries, fallback_boundaries,
            "distinct mixed-direction index-shape and fallback boundaries should match for limit={limit}",
        );
    }
}

#[test]
fn load_distinct_desc_pk_fast_path_and_fallback_match_ids_and_boundaries() {
    setup_pagination_test();

    let keys = [
        39_651_u128,
        39_652_u128,
        39_653_u128,
        39_654_u128,
        39_655_u128,
        39_656_u128,
        39_657_u128,
    ];
    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [39_656_u128, 39_651, 39_655, 39_653, 39_657, 39_652, 39_654] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("distinct DESC pk parity seed save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    for limit in [1_u32, 2, 3] {
        let (_fast_seed_page, fast_trace) = load
            .execute_paged_with_cursor_traced(
                Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                    .order_by_desc("id")
                    .distinct()
                    .limit(limit)
                    .offset(1)
                    .plan()
                    .map(ExecutablePlan::from)
                    .expect("distinct DESC PK fast-path seed plan should build"),
                None,
            )
            .expect("distinct DESC PK fast-path seed page should execute");
        let fast_trace = fast_trace.expect("debug trace should be present");
        assert_eq!(
            fast_trace.optimization(),
            Some(crate::db::diagnostics::ExecutionOptimization::PrimaryKeyTopNSeek),
            "distinct DESC full-scan seed execution should use PK fast path for limit={limit}",
        );

        let (_fallback_seed_page, fallback_trace) = load
            .execute_paged_with_cursor_traced(
                Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                    .by_ids(keys.into_iter().map(Ulid::from_u128))
                    .order_by_desc("id")
                    .distinct()
                    .limit(limit)
                    .offset(1)
                    .plan()
                    .map(ExecutablePlan::from)
                    .expect("distinct DESC PK fallback seed plan should build"),
                None,
            )
            .expect("distinct DESC PK fallback seed page should execute");
        let fallback_trace = fallback_trace.expect("debug trace should be present");
        assert_eq!(
            fallback_trace.optimization(),
            None,
            "distinct DESC by-ids seed execution should not report fast-path optimization for limit={limit}",
        );

        let build_fast_plan = || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .offset(1)
                .plan()
                .map(ExecutablePlan::from)
                .expect("distinct DESC PK fast-path plan should build")
        };
        let build_fallback_plan = || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_ids(keys.into_iter().map(Ulid::from_u128))
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .offset(1)
                .plan()
                .map(ExecutablePlan::from)
                .expect("distinct DESC PK fallback plan should build")
        };

        let (fast_ids, fast_boundaries) =
            collect_simple_pages_from_executable_plan(&load, build_fast_plan, 20);
        let (fallback_ids, fallback_boundaries) =
            collect_simple_pages_from_executable_plan(&load, build_fallback_plan, 20);
        assert_eq!(
            fast_ids, fallback_ids,
            "distinct DESC PK fast-path and fallback ids should match for limit={limit}",
        );
        assert_eq!(
            fast_boundaries, fallback_boundaries,
            "distinct DESC PK fast-path and fallback boundaries should match for limit={limit}",
        );
    }
}

#[test]
fn load_distinct_desc_index_range_limit_pushdown_resume_matrix_and_fallback_parity() {
    setup_pagination_test();

    let rows = [
        (39_601, 10, "t10-a"),
        (39_602, 10, "t10-b"),
        (39_603, 20, "t20-a"),
        (39_604, 20, "t20-b"),
        (39_605, 25, "t25"),
        (39_606, 28, "t28-a"),
        (39_607, 28, "t28-b"),
        (39_608, 40, "t40"),
    ];
    seed_indexed_metrics_rows(&rows);

    let expected_ids =
        ordered_index_candidate_ids_for_direction(&rows, 10, 30, OrderDirection::Desc);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    for limit in [1_u32, 2, 3] {
        let (_seed_page, seed_trace) = load
            .execute_paged_with_cursor_traced(build_distinct_desc_index_range_plan(limit, 0), None)
            .expect("distinct DESC index-range seed page should execute");
        let seed_trace = seed_trace.expect("debug trace should be present");
        assert_eq!(
            seed_trace.optimization(),
            Some(crate::db::diagnostics::ExecutionOptimization::IndexRangeLimitPushdown),
            "distinct DESC index-range seed execution should use limit pushdown for limit={limit}",
        );

        let build_fast_plan = || build_distinct_desc_index_range_plan(limit, 0);
        let (fast_ids, fast_boundaries, fast_tokens) =
            collect_indexed_metric_pages_from_executable_plan_with_tokens(
                &load,
                build_fast_plan,
                20,
            );
        assert_eq!(
            fast_ids, expected_ids,
            "distinct DESC index-range pushdown should preserve canonical ordering for limit={limit}",
        );
        assert_indexed_metric_resume_suffixes_from_tokens(
            &load,
            &build_fast_plan,
            &fast_tokens,
            &expected_ids,
            "distinct DESC index-range token resume",
        );

        let build_fallback_plan = || {
            Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
                .by_ids(expected_ids.iter().copied())
                .order_by_desc("tag")
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .plan()
                .map(ExecutablePlan::from)
                .expect("distinct DESC index-range fallback plan should build")
        };
        let (fallback_ids, fallback_boundaries) =
            collect_indexed_metric_pages_from_executable_plan(&load, build_fallback_plan, 20);
        assert_eq!(
            fast_ids, fallback_ids,
            "distinct DESC index-range fast path and fallback ids should match for limit={limit}",
        );
        assert_eq!(
            fast_boundaries, fallback_boundaries,
            "distinct DESC index-range fast path and fallback boundaries should match for limit={limit}",
        );
    }
}

#[test]
fn load_distinct_offset_fast_path_and_fallback_match_ids_and_boundaries() {
    setup_pagination_test();

    let secondary_rows = [
        (42_301, 7, 10, "g7-r10"),
        (42_302, 7, 20, "g7-r20-a"),
        (42_303, 7, 20, "g7-r20-b"),
        (42_304, 7, 30, "g7-r30"),
        (42_305, 7, 40, "g7-r40"),
        (42_306, 8, 10, "g8-r10"),
        (42_307, 8, 20, "g8-r20"),
        (42_308, 8, 30, "g8-r30"),
        (42_309, 9, 10, "g9-r10"),
    ];
    seed_pushdown_rows(&secondary_rows);
    let secondary_predicate = pushdown_group_predicate(7);
    let secondary_group_ids = pushdown_group_ids(&secondary_rows, 7);

    let index_rows = [
        (42_401, 10, "t10-a"),
        (42_402, 10, "t10-b"),
        (42_403, 20, "t20-a"),
        (42_404, 20, "t20-b"),
        (42_405, 25, "t25"),
        (42_406, 28, "t28-a"),
        (42_407, 28, "t28-b"),
        (42_408, 40, "t40"),
    ];
    seed_indexed_metrics_rows(&index_rows);

    let load_secondary = LoadExecutor::<PushdownParityEntity>::new(DB, true);
    let load_index_range = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    for (case_name, direction) in [("asc", OrderDirection::Asc), ("desc", OrderDirection::Desc)] {
        assert_distinct_secondary_offset_parity_case(
            &load_secondary,
            secondary_predicate.clone(),
            &secondary_group_ids,
            direction,
            case_name,
        );
        assert_distinct_index_range_offset_parity_case(
            &load_index_range,
            &index_rows,
            direction,
            case_name,
        );
    }
}
