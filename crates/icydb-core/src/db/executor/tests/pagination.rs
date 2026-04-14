//! Module: db::executor::tests::pagination
//! Responsibility: executor-owned pagination contracts for the revived live test harness.
//! Does not own: old matrix wrappers or query-intent paging policy tests.
//! Boundary: covers small runtime pagination behaviors that are easiest to validate end-to-end.

use super::support::*;
use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        cursor::{ContinuationToken, CursorBoundary, CursorBoundarySlot},
        diagnostics::ExecutionOptimization,
        executor::PreparedExecutionPlan,
        executor::pipeline::contracts::{CursorPage, PageCursor},
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::explain::ExplainAccessPath,
        query::plan::{
            AccessPlannedQuery, LoadSpec, LogicalPlan, OrderDirection, OrderSpec, PageSpec,
            QueryMode, ScalarPlan,
        },
        response::EntityResponse,
    },
    types::Ulid,
    value::Value,
};
use proptest::prelude::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
};

trait PaginationTestEntityId {
    fn entity_id(&self) -> Ulid;
}

impl PaginationTestEntityId for IndexedMetricsEntity {
    fn entity_id(&self) -> Ulid {
        self.id
    }
}

impl PaginationTestEntityId for PushdownParityEntity {
    fn entity_id(&self) -> Ulid {
        self.id
    }
}

impl PaginationTestEntityId for UniqueIndexRangeEntity {
    fn entity_id(&self) -> Ulid {
        self.id
    }
}

fn ids_from_items<E>(response: &EntityResponse<E>) -> Vec<Ulid>
where
    E: PaginationTestEntityId + crate::traits::EntityKind,
{
    response
        .iter()
        .map(|row| row.entity_ref().entity_id())
        .collect()
}

fn scalar_boundary(cursor: &PageCursor) -> CursorBoundary {
    cursor
        .as_scalar()
        .expect("pagination test cursor should stay scalar")
        .boundary()
        .clone()
}

fn explain_contains_index_range(
    access: &ExplainAccessPath,
    expected_index: &str,
    expected_prefix_len: usize,
) -> bool {
    matches!(
        access,
        ExplainAccessPath::IndexRange {
            name,
            prefix_len,
            ..
        } if *name == expected_index && *prefix_len == expected_prefix_len
    )
}

fn setup_pagination_test() {
    reset_store();
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

fn seed_phase_rows(rows: &[PhaseEntity]) {
    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in rows {
        save.insert(row.clone())
            .expect("phase pagination seed save should succeed");
    }
}

fn build_scalar_limit_plan(
    access: AccessPlan<Ulid>,
    limit: u32,
    offset: u32,
) -> PreparedExecutionPlan<SimpleEntity> {
    PreparedExecutionPlan::new(AccessPlannedQuery {
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
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
    })
}

fn build_simple_union_page_plan(
    left: Vec<Ulid>,
    right: Vec<Ulid>,
    descending: bool,
    limit: u32,
    offset: u32,
    predicate: Option<Predicate>,
) -> PreparedExecutionPlan<SimpleEntity> {
    PreparedExecutionPlan::new(AccessPlannedQuery {
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
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
    })
}

fn build_simple_key_range_page_plan(
    start: u128,
    end: u128,
    direction: OrderDirection,
    limit: Option<u32>,
    offset: u32,
) -> PreparedExecutionPlan<SimpleEntity> {
    PreparedExecutionPlan::<SimpleEntity>::new(AccessPlannedQuery {
        logical: LogicalPlan::Scalar(ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), direction)],
            }),
            distinct: false,
            delete_limit: None,
            page: limit.map(|limit| PageSpec {
                limit: Some(limit),
                offset,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::KeyRange {
            start: Ulid::from_u128(start),
            end: Ulid::from_u128(end),
        })
        .into_value_plan(),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
    })
}

fn build_phase_rank_page_plan(descending: bool, limit: u32) -> PreparedExecutionPlan<PhaseEntity> {
    let base = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore).limit(limit);
    let ordered = if descending {
        base.order_by_desc("rank")
    } else {
        base.order_by("rank")
    };

    ordered
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("phase rank page plan should build")
}

fn execute_phase_rank_page(
    load: &LoadExecutor<PhaseEntity>,
    descending: bool,
    limit: u32,
    cursor: Option<&[u8]>,
) -> CursorPage<PhaseEntity> {
    let plan = build_phase_rank_page_plan(descending, limit);
    let boundary = build_phase_rank_page_plan(descending, limit)
        .prepare_cursor(cursor)
        .expect("phase rank page boundary should plan");

    load.execute_paged_with_cursor(plan, boundary)
        .expect("phase rank page should execute")
}

fn execute_page_ids_and_keys_scanned(
    load: &LoadExecutor<SimpleEntity>,
    plan: PreparedExecutionPlan<SimpleEntity>,
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

fn phase_ids_from_items(items: &crate::db::response::EntityResponse<PhaseEntity>) -> Vec<Ulid> {
    items.iter().map(|row| row.entity_ref().id).collect()
}

fn collect_simple_pages_from_executable_plan(
    load: &LoadExecutor<SimpleEntity>,
    build_plan: impl Fn() -> PreparedExecutionPlan<SimpleEntity>,
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

fn collect_simple_pages_from_executable_plan_with_tokens(
    load: &LoadExecutor<SimpleEntity>,
    build_plan: impl Fn() -> PreparedExecutionPlan<SimpleEntity>,
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
        let token = cursor
            .encode()
            .expect("simple continuation cursor should serialize");
        boundaries.push(scalar.boundary().clone());
        tokens.push(token.clone());
        encoded_cursor = Some(token);
    }

    (ids, boundaries, tokens)
}

fn assert_simple_resume_suffixes_from_tokens(
    load: &LoadExecutor<SimpleEntity>,
    build_plan: &impl Fn() -> PreparedExecutionPlan<SimpleEntity>,
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
                    .expect("simple token resume should plan"),
            )
            .expect("simple token resume should execute");
        let resumed_ids = simple_ids_from_items(&page.items);
        let first_resumed_id = *resumed_ids
            .first()
            .expect("simple resumed page should contain at least one row");
        let expected_start = expected_ids
            .iter()
            .position(|id| *id == first_resumed_id)
            .expect("simple resumed id should exist in the expected baseline");
        assert_eq!(
            resumed_ids.as_slice(),
            &expected_ids[expected_start..expected_start.saturating_add(resumed_ids.len())],
            "{context}: resumed simple page should preserve suffix order",
        );
    }
}

fn assert_simple_pagination_parity_matrix(
    load: &LoadExecutor<SimpleEntity>,
    limits: &[u32],
    build_left_plan: impl Fn(u32) -> PreparedExecutionPlan<SimpleEntity>,
    build_right_plan: impl Fn(u32) -> PreparedExecutionPlan<SimpleEntity>,
    context_prefix: &str,
) {
    for &limit in limits {
        let build_left_plan_for_limit = || build_left_plan(limit);
        let build_right_plan_for_limit = || build_right_plan(limit);
        let (left_ids, left_boundaries) =
            collect_simple_pages_from_executable_plan(load, build_left_plan_for_limit, 20);
        let (right_ids, right_boundaries) =
            collect_simple_pages_from_executable_plan(load, build_right_plan_for_limit, 20);

        assert_eq!(
            left_ids, right_ids,
            "{context_prefix} ids should match for limit={limit}",
        );
        assert_eq!(
            left_boundaries, right_boundaries,
            "{context_prefix} boundaries should match for limit={limit}",
        );
    }
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

fn distinct_secondary_pushdown_rows(prefix: u128) -> [(u128, u32, u32, &'static str); 9] {
    [
        (prefix + 1, 7, 10, "g7-r10"),
        (prefix + 2, 7, 20, "g7-r20-a"),
        (prefix + 3, 7, 20, "g7-r20-b"),
        (prefix + 4, 7, 30, "g7-r30"),
        (prefix + 5, 7, 40, "g7-r40"),
        (prefix + 6, 8, 10, "g8-r10"),
        (prefix + 7, 8, 20, "g8-r20"),
        (prefix + 8, 8, 30, "g8-r30"),
        (prefix + 9, 9, 10, "g9-r10"),
    ]
}

fn indexed_metric_range_rows(prefix: u128) -> [(u128, u32, &'static str); 8] {
    [
        (prefix + 1, 10, "t10-a"),
        (prefix + 2, 10, "t10-b"),
        (prefix + 3, 20, "t20-a"),
        (prefix + 4, 20, "t20-b"),
        (prefix + 5, 25, "t25"),
        (prefix + 6, 28, "t28-a"),
        (prefix + 7, 28, "t28-b"),
        (prefix + 8, 40, "t40"),
    ]
}

fn pushdown_rows_with_group9(prefix: u128) -> [(u128, u32, u32, &'static str); 8] {
    [
        (prefix + 1, 7, 10, "g7-r10"),
        (prefix + 2, 7, 20, "g7-r20-a"),
        (prefix + 3, 7, 20, "g7-r20-b"),
        (prefix + 4, 7, 30, "g7-r30"),
        (prefix + 5, 7, 40, "g7-r40"),
        (prefix + 6, 9, 10, "g9-r10"),
        (prefix + 7, 9, 30, "g9-r30"),
        (prefix + 8, 9, 50, "g9-r50"),
    ]
}

fn pushdown_rows_window(prefix: u128) -> [(u128, u32, u32, &'static str); 5] {
    [
        (prefix + 1, 7, 10, "g7-r10"),
        (prefix + 2, 7, 20, "g7-r20"),
        (prefix + 3, 7, 30, "g7-r30"),
        (prefix + 4, 8, 10, "g8-r10"),
        (prefix + 5, 8, 20, "g8-r20"),
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
    build_plan: impl Fn() -> PreparedExecutionPlan<IndexedMetricsEntity>,
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
    build_plan: impl Fn() -> PreparedExecutionPlan<IndexedMetricsEntity>,
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
    build_plan: &impl Fn() -> PreparedExecutionPlan<IndexedMetricsEntity>,
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

fn assert_indexed_metric_resume_and_fallback_parity_matrix(
    load: &LoadExecutor<IndexedMetricsEntity>,
    limits: &[u32],
    expected_ids: &[Ulid],
    build_fast_plan: impl Fn(u32) -> PreparedExecutionPlan<IndexedMetricsEntity>,
    build_fallback_plan: impl Fn(u32) -> PreparedExecutionPlan<IndexedMetricsEntity>,
    context_prefix: &str,
) {
    for &limit in limits {
        let build_fast_plan_for_limit = || build_fast_plan(limit);
        let (fast_ids, fast_boundaries, fast_tokens) =
            collect_indexed_metric_pages_from_executable_plan_with_tokens(
                load,
                build_fast_plan_for_limit,
                20,
            );
        assert_eq!(
            fast_ids, expected_ids,
            "{context_prefix} should preserve canonical ordering for limit={limit}",
        );

        let token_context = format!("{context_prefix} token resume limit={limit}");
        assert_indexed_metric_resume_suffixes_from_tokens(
            load,
            &build_fast_plan_for_limit,
            &fast_tokens,
            expected_ids,
            token_context.as_str(),
        );

        let build_fallback_plan_for_limit = || build_fallback_plan(limit);
        let (fallback_ids, fallback_boundaries) = collect_indexed_metric_pages_from_executable_plan(
            load,
            build_fallback_plan_for_limit,
            20,
        );
        assert_eq!(
            fast_ids, fallback_ids,
            "{context_prefix} fast path and fallback ids should match for limit={limit}",
        );
        assert_eq!(
            fast_boundaries, fallback_boundaries,
            "{context_prefix} fast path and fallback boundaries should match for limit={limit}",
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
) -> PreparedExecutionPlan<IndexedMetricsEntity> {
    PreparedExecutionPlan::<IndexedMetricsEntity>::new(AccessPlannedQuery {
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
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
    })
}

fn build_distinct_secondary_offset_fast_plan(
    direction: OrderDirection,
    predicate: Predicate,
) -> PreparedExecutionPlan<PushdownParityEntity> {
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
        .map(PreparedExecutionPlan::from)
        .expect("distinct secondary offset fast-path plan should build")
}

fn build_distinct_secondary_offset_fallback_plan(
    direction: OrderDirection,
    ids: &[Ulid],
) -> PreparedExecutionPlan<PushdownParityEntity> {
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
        .map(PreparedExecutionPlan::from)
        .expect("distinct secondary offset fallback plan should build")
}

fn build_distinct_index_range_offset_fast_plan(
    direction: OrderDirection,
) -> PreparedExecutionPlan<IndexedMetricsEntity> {
    PreparedExecutionPlan::<IndexedMetricsEntity>::new(AccessPlannedQuery {
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
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
    })
}

fn build_distinct_index_range_offset_fallback_plan(
    direction: OrderDirection,
    ids: &[Ulid],
) -> PreparedExecutionPlan<IndexedMetricsEntity> {
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
        .map(PreparedExecutionPlan::from)
        .expect("distinct index-range offset fallback plan should build")
}

fn assert_distinct_secondary_offset_parity_case(
    load: &LoadExecutor<PushdownParityEntity>,
    predicate: Predicate,
    group_ids: &[Ulid],
    expected_ids: &[Ulid],
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

    let (fast_ids, fast_boundaries) = collect_pushdown_pages_and_assert_token_resumes(
        load,
        build_fast_plan,
        expected_ids,
        case_name,
    );
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
}

fn assert_distinct_index_range_offset_parity_case(
    load: &LoadExecutor<IndexedMetricsEntity>,
    rows: &[(u128, u32, &str)],
    direction: OrderDirection,
    case_name: &str,
) {
    let build_fast_plan = || build_distinct_index_range_offset_fast_plan(direction);
    let candidate_ids = ordered_index_candidate_ids_for_direction(rows, 10, 30, direction);
    let expected_ids = candidate_ids.iter().copied().skip(1).collect::<Vec<_>>();
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

    assert_indexed_metric_resume_and_fallback_parity_matrix(
        load,
        &[2_u32],
        &expected_ids,
        |_| build_distinct_index_range_offset_fast_plan(direction),
        |_| build_distinct_index_range_offset_fallback_plan(direction, &candidate_ids),
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
            PreparedExecutionPlan::from,
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

fn decode_boundary(cursor_bytes: &[u8], context: &'static str) -> CursorBoundary {
    ContinuationToken::decode(cursor_bytes)
        .unwrap_or_else(|_| panic!("{context}"))
        .boundary()
        .clone()
}

fn assert_resume_from_terminal_entity_exhausts_range(
    terminal_entity: &PushdownParityEntity,
    context: &'static str,
) {
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(pushdown_group_predicate(7))
        .order_by("rank")
        .limit(2)
        .plan()
        .map(PreparedExecutionPlan::from)
        .expect("terminal resume plan should build");
    let boundary = Some(CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Uint(u64::from(terminal_entity.rank))),
            CursorBoundarySlot::Present(Value::Ulid(terminal_entity.id)),
        ],
    });
    let page = load
        .execute_paged_with_cursor(plan, boundary)
        .expect("terminal resume page should execute");

    assert!(
        page.items.is_empty(),
        "{context}: terminal boundary should yield an empty continuation page",
    );
    assert!(
        page.next_cursor.is_none(),
        "{context}: terminal boundary should not emit a continuation cursor",
    );
}

fn execute_full_query<E>(query: Query<E>) -> Vec<Ulid>
where
    E: crate::db::PersistedRow
        + crate::traits::EntityKind
        + crate::traits::EntityPlacement<Canister = TestCanister>
        + crate::traits::EntityValue
        + crate::traits::EntityKey<Key = Ulid>
        + PaginationTestEntityId,
{
    let load = LoadExecutor::<E>::new(DB, false);
    let items = load
        .execute(
            query
                .plan()
                .map(PreparedExecutionPlan::from)
                .expect("full-query plan should build"),
        )
        .expect("full-query execution should succeed");

    ids_from_items(&items)
}

fn execute_paged_query_ids<E>(
    build_query: &impl Fn() -> Query<E>,
    limit: u32,
    max_pages: usize,
) -> Vec<Ulid>
where
    E: crate::db::PersistedRow
        + crate::traits::EntityKind
        + crate::traits::EntityPlacement<Canister = TestCanister>
        + crate::traits::EntityValue
        + crate::traits::EntityKey<Key = Ulid>
        + PaginationTestEntityId,
{
    let load = LoadExecutor::<E>::new(DB, false);
    let mut encoded_cursor = None::<Vec<u8>>;
    let mut ids = Vec::new();

    for _ in 0..max_pages {
        let plan = build_query()
            .limit(limit)
            .plan()
            .map(PreparedExecutionPlan::from)
            .expect("paged limit-matrix plan should build");
        let boundary = plan
            .prepare_cursor(encoded_cursor.as_deref())
            .expect("paged limit-matrix boundary should plan");
        let page = load
            .execute_paged_with_cursor(plan, boundary)
            .expect("paged limit-matrix execution should succeed");
        ids.extend(ids_from_items(&page.items));

        let Some(cursor) = page.next_cursor else {
            break;
        };
        encoded_cursor = Some(
            cursor
                .encode()
                .expect("paged limit-matrix cursor should serialize"),
        );
    }

    ids
}

fn assert_limit_matrix<E>(build_query: impl Fn() -> Query<E>, limits: &[u32], max_pages: usize)
where
    E: crate::db::PersistedRow
        + crate::traits::EntityKind
        + crate::traits::EntityPlacement<Canister = TestCanister>
        + crate::traits::EntityValue
        + crate::traits::EntityKey<Key = Ulid>
        + PaginationTestEntityId,
{
    let expected_ids = execute_full_query(build_query());
    for limit in limits {
        let paged_ids = execute_paged_query_ids(&build_query, *limit, max_pages);
        let expected = if *limit == 0 {
            Vec::new()
        } else {
            expected_ids.clone()
        };
        assert_eq!(
            paged_ids, expected,
            "limit-matrix paged traversal should match the unbounded baseline across all pages for limit={limit}",
        );
    }
}

fn assert_pushdown_parity<E>(
    build_query: impl Fn() -> Query<E>,
    fallback_ids: Vec<Ulid>,
    order: impl Fn(Query<E>) -> Query<E>,
) where
    E: crate::db::PersistedRow
        + crate::traits::EntityKind
        + crate::traits::EntityPlacement<Canister = TestCanister>
        + crate::traits::EntityValue
        + crate::traits::EntityKey<Key = Ulid>
        + PaginationTestEntityId,
{
    let pushdown_ids = execute_full_query(build_query());
    let fallback_ids = execute_full_query(order(
        Query::<E>::new(MissingRowPolicy::Ignore).by_ids(fallback_ids.iter().copied()),
    ));

    assert_eq!(
        pushdown_ids, fallback_ids,
        "pushdown and by-ids fallback should return the same full ordered result",
    );
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
            PreparedExecutionPlan::from,
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
            PreparedExecutionPlan::from,
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
            PreparedExecutionPlan::from,
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

fn pushdown_group_rank_id_boundary(group: Option<u32>, rank: u32, id: u128) -> CursorBoundary {
    let mut slots = Vec::new();
    if let Some(group) = group {
        slots.push(CursorBoundarySlot::Present(Value::Uint(u64::from(group))));
    }
    slots.push(CursorBoundarySlot::Present(Value::Uint(u64::from(rank))));
    slots.push(CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(
        id,
    ))));

    CursorBoundary { slots }
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
                PreparedExecutionPlan::from,
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
                PreparedExecutionPlan::from,
            ),
        boundary,
    )
    .unwrap_or_else(|_| panic!("{context} should execute"))
}

fn collect_pushdown_paged_ids(
    load: &LoadExecutor<PushdownParityEntity>,
    build_plan: impl Fn() -> PreparedExecutionPlan<PushdownParityEntity>,
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
    build_plan: impl Fn() -> PreparedExecutionPlan<PushdownParityEntity>,
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
    build_plan: impl Fn() -> PreparedExecutionPlan<PushdownParityEntity>,
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
    build_plan: &impl Fn() -> PreparedExecutionPlan<PushdownParityEntity>,
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
    build_plan: &impl Fn() -> PreparedExecutionPlan<PushdownParityEntity>,
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

fn collect_pushdown_pages_and_assert_token_resumes(
    load: &LoadExecutor<PushdownParityEntity>,
    build_plan: impl Fn() -> PreparedExecutionPlan<PushdownParityEntity>,
    expected_ids: &[Ulid],
    context: &str,
) -> (Vec<Ulid>, Vec<CursorBoundary>) {
    let (ids, boundaries, tokens) =
        collect_pushdown_pages_from_executable_plan_with_tokens(load, &build_plan, 20);
    assert_eq!(
        ids, expected_ids,
        "{context}: traversal should preserve canonical ordering"
    );
    assert_pushdown_resume_suffixes_from_tokens(load, &build_plan, &tokens, expected_ids, context);

    (ids, boundaries)
}

fn assert_pushdown_distinct_resume_matrix(
    load: &LoadExecutor<PushdownParityEntity>,
    limits: &[u32],
    expected_ids: &[Ulid],
    build_plan: impl Fn(u32) -> PreparedExecutionPlan<PushdownParityEntity>,
    context_prefix: &str,
) {
    for &limit in limits {
        let build_plan_for_limit = || build_plan(limit);
        let (actual_ids, boundaries) =
            collect_pushdown_pages_from_executable_plan(load, build_plan_for_limit, 20);

        assert_eq!(
            actual_ids, expected_ids,
            "{context_prefix} should preserve canonical ordering for limit={limit}",
        );

        let unique: BTreeSet<Ulid> = actual_ids.iter().copied().collect();
        assert_eq!(
            unique.len(),
            actual_ids.len(),
            "{context_prefix} must not emit duplicates for limit={limit}",
        );

        let context = format!("{context_prefix} limit={limit}");
        assert_pushdown_resume_suffixes_from_boundaries(
            load,
            &build_plan_for_limit,
            &boundaries,
            expected_ids,
            context.as_str(),
        );
    }
}

// Compare two pagination shapes across a shared limit matrix and assert that
// they produce identical rows and continuation boundaries.
fn assert_pushdown_pagination_parity_matrix(
    load: &LoadExecutor<PushdownParityEntity>,
    limits: &[u32],
    build_left_plan: impl Fn(u32) -> PreparedExecutionPlan<PushdownParityEntity>,
    build_right_plan: impl Fn(u32) -> PreparedExecutionPlan<PushdownParityEntity>,
    context_prefix: &str,
) {
    for &limit in limits {
        let build_left_plan_for_limit = || build_left_plan(limit);
        let build_right_plan_for_limit = || build_right_plan(limit);
        let (left_ids, left_boundaries) =
            collect_pushdown_pages_from_executable_plan(load, build_left_plan_for_limit, 20);
        let (right_ids, right_boundaries) =
            collect_pushdown_pages_from_executable_plan(load, build_right_plan_for_limit, 20);

        assert_eq!(
            left_ids, right_ids,
            "{context_prefix} ids should match for limit={limit}",
        );
        assert_eq!(
            left_boundaries, right_boundaries,
            "{context_prefix} boundaries should match for limit={limit}",
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

fn build_rank_unique_pushdown_plan(
    predicate: Predicate,
    id_desc: bool,
    limit: u32,
) -> PreparedExecutionPlan<PushdownParityEntity> {
    let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("rank")
        .limit(limit);
    let query = if id_desc {
        query.order_by_desc("id")
    } else {
        query.order_by("id")
    };

    query
        .plan()
        .map(PreparedExecutionPlan::from)
        .expect("rank-unique pushdown plan should build")
}

fn assert_rank_unique_order_pushdown_explain_missing_model_context(
    predicate: Predicate,
    id_desc: bool,
    context: &'static str,
) {
    let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("rank");
    let query = if id_desc {
        query.order_by_desc("id")
    } else {
        query.order_by("id")
    };
    let explain = query.explain().expect("rank-unique explain should build");

    assert!(
        matches!(
            explain.order_pushdown(),
            crate::db::query::explain::ExplainOrderPushdown::MissingModelContext
        ),
        "{context}: query-layer explain should not evaluate secondary pushdown eligibility",
    );
}

fn build_mixed_direction_resume_plan(
    filter_group: Option<u32>,
    group_desc: Option<bool>,
    rank_desc: bool,
    id_desc: bool,
    limit: u32,
) -> PreparedExecutionPlan<PushdownParityEntity> {
    let mut query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore).limit(limit);

    if let Some(group) = filter_group {
        query = query.filter(pushdown_group_predicate(group));
    }

    if let Some(desc) = group_desc {
        query = if desc {
            query.order_by_desc("group")
        } else {
            query.order_by("group")
        };
    }

    query = if rank_desc {
        query.order_by_desc("rank")
    } else {
        query.order_by("rank")
    };

    query = if id_desc {
        query.order_by_desc("id")
    } else {
        query.order_by("id")
    };

    query
        .plan()
        .map(PreparedExecutionPlan::from)
        .expect("mixed-direction resume plan should build")
}

///
/// MixedDirectionResumeCase
///
/// Small owner-local fixture describing one mixed-direction ordering/resume
/// contract in the pagination matrix without reintroducing the old wrapper
/// harness.
///

struct MixedDirectionResumeCase {
    case_name: &'static str,
    filter_group: Option<u32>,
    group_desc: Option<bool>,
    rank_desc: bool,
    id_desc: bool,
    expected_ids: Vec<Ulid>,
}

fn assert_mixed_direction_resume_case(
    load: &LoadExecutor<PushdownParityEntity>,
    row_lookup: &BTreeMap<Ulid, (u128, u32, u32)>,
    case: &MixedDirectionResumeCase,
) {
    let build_plan = |limit| {
        build_mixed_direction_resume_plan(
            case.filter_group,
            case.group_desc,
            case.rank_desc,
            case.id_desc,
            limit,
        )
    };

    let base_page = load
        .execute_paged_with_cursor(build_plan(16), None)
        .expect("mixed-direction base page should execute");
    let base_ids = pushdown_ids_from_response(&base_page.items);
    assert_eq!(
        base_ids, case.expected_ids,
        "case '{}' should preserve mixed-direction canonical ordering",
        case.case_name,
    );

    for (idx, id) in case.expected_ids.iter().copied().enumerate() {
        let (raw_id, group, rank) = row_lookup
            .get(&id)
            .copied()
            .expect("resume case should only reference seeded rows");
        let resumed_page = load
            .execute_paged_with_cursor(
                build_plan(16),
                Some(pushdown_group_rank_id_boundary(
                    case.group_desc.map(|_| group),
                    rank,
                    raw_id,
                )),
            )
            .expect("mixed-direction boundary resume should execute");
        let resumed_ids = pushdown_ids_from_response(&resumed_page.items);
        assert_eq!(
            resumed_ids,
            case.expected_ids[idx + 1..].to_vec(),
            "case '{}' should resume from the row immediately after the boundary entity",
            case.case_name,
        );
    }

    for limit in [1_u32, 2, 3] {
        let (paged_ids, _) =
            collect_pushdown_pages_from_executable_plan(load, || build_plan(limit), 20);
        assert_eq!(
            paged_ids, case.expected_ids,
            "case '{}' with limit={limit} paged traversal should match unbounded mixed-direction ordering",
            case.case_name,
        );

        let unique: BTreeSet<Ulid> = paged_ids.iter().copied().collect();
        assert_eq!(
            unique.len(),
            paged_ids.len(),
            "case '{}' with limit={limit} mixed-direction pagination must not duplicate rows",
            case.case_name,
        );
    }
}

fn build_simple_ordered_page_plan(
    descending: bool,
    limit: u32,
    offset: u32,
) -> PreparedExecutionPlan<SimpleEntity> {
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
        .map(PreparedExecutionPlan::from)
        .expect("simple ordered pagination plan should build")
}

///
/// DescCursorResumeCase
///
/// One DESC cursor-resume parity row for the live pagination owner. Each row
/// binds one seeded access-shape family to its unbounded DESC baseline.
///

struct DescCursorResumeCase {
    label: &'static str,
    run: fn() -> (Vec<Ulid>, Vec<Ulid>),
    assert_strict_descending: bool,
}

fn closed_u32_range_predicate(
    field: &str,
    lower_inclusive: u32,
    upper_inclusive: u32,
) -> Predicate {
    Predicate::And(vec![
        strict_compare_predicate(
            field,
            CompareOp::Gte,
            Value::Uint(u64::from(lower_inclusive)),
        ),
        strict_compare_predicate(
            field,
            CompareOp::Lte,
            Value::Uint(u64::from(upper_inclusive)),
        ),
    ])
}

fn collect_desc_cursor_resume_ids(
    expected_desc_ids: Vec<Ulid>,
    mut fetch_page: impl FnMut(Option<&str>) -> (Vec<Ulid>, Option<String>),
) -> (Vec<Ulid>, Vec<Ulid>) {
    let mut resumed_desc_ids = Vec::new();
    let mut cursor_token = None::<String>;
    loop {
        let (page_ids, next_cursor) = fetch_page(cursor_token.as_deref());
        resumed_desc_ids.extend(page_ids);
        match next_cursor {
            Some(token) => {
                cursor_token = Some(token);
            }
            None => {
                break;
            }
        }
    }

    (resumed_desc_ids, expected_desc_ids)
}

fn run_desc_cursor_resume_simple_case() -> (Vec<Ulid>, Vec<Ulid>) {
    setup_pagination_test();
    seed_simple_rows(&[9971, 9972, 9973, 9974, 9975, 9976, 9977, 9978, 9979, 9980]);
    let session = DbSession::new(DB);
    let expected_desc_ids = session
        .load::<SimpleEntity>()
        .order_by_desc("id")
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("unbounded DESC execute should succeed")
        .ids()
        .map(|id| id.key())
        .collect::<Vec<_>>();

    collect_desc_cursor_resume_ids(expected_desc_ids, |cursor_token| {
        let mut paged_query = session.load::<SimpleEntity>().order_by_desc("id").limit(3);
        if let Some(token) = cursor_token {
            paged_query = paged_query.cursor(token);
        }
        let execution = paged_query
            .execute_paged()
            .expect("paged DESC execute should succeed");

        (
            execution
                .response()
                .ids()
                .map(|id| id.key())
                .collect::<Vec<_>>(),
            execution
                .continuation_cursor()
                .map(crate::db::encode_cursor),
        )
    })
}

fn run_desc_cursor_resume_secondary_index_case() -> (Vec<Ulid>, Vec<Ulid>) {
    setup_pagination_test();
    seed_pushdown_rows(&[
        (9981, 7, 40, "g7-r40"),
        (9982, 7, 30, "g7-r30-a"),
        (9983, 7, 30, "g7-r30-b"),
        (9984, 7, 20, "g7-r20-a"),
        (9985, 7, 20, "g7-r20-b"),
        (9986, 7, 10, "g7-r10"),
        (9987, 8, 50, "g8-r50"),
    ]);
    let session = DbSession::new(DB);
    let group_seven = pushdown_group_predicate(7);
    let expected_desc_ids = session
        .load::<PushdownParityEntity>()
        .filter(group_seven.clone())
        .order_by_desc("rank")
        .order_by_desc("id")
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("unbounded DESC secondary-index execute should succeed")
        .ids()
        .map(|id| id.key())
        .collect::<Vec<_>>();

    collect_desc_cursor_resume_ids(expected_desc_ids, |cursor_token| {
        let mut paged_query = session
            .load::<PushdownParityEntity>()
            .filter(group_seven.clone())
            .order_by_desc("rank")
            .order_by_desc("id")
            .limit(2);
        if let Some(token) = cursor_token {
            paged_query = paged_query.cursor(token);
        }
        let execution = paged_query
            .execute_paged()
            .expect("paged DESC secondary-index execute should succeed");

        (
            execution
                .response()
                .ids()
                .map(|id| id.key())
                .collect::<Vec<_>>(),
            execution
                .continuation_cursor()
                .map(crate::db::encode_cursor),
        )
    })
}

fn run_desc_cursor_resume_index_range_case() -> (Vec<Ulid>, Vec<Ulid>) {
    setup_pagination_test();
    seed_unique_index_range_rows(&[
        (9991, 200, "c200"),
        (9992, 201, "c201"),
        (9993, 202, "c202"),
        (9994, 203, "c203"),
        (9995, 204, "c204"),
        (9996, 205, "c205"),
    ]);
    let session = DbSession::new(DB);
    let range_predicate = closed_u32_range_predicate("code", 201, 206);
    let expected_desc_ids = session
        .load::<UniqueIndexRangeEntity>()
        .filter(range_predicate.clone())
        .order_by_desc("code")
        .order_by_desc("id")
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("unbounded DESC index-range execute should succeed")
        .ids()
        .map(|id| id.key())
        .collect::<Vec<_>>();

    collect_desc_cursor_resume_ids(expected_desc_ids, |cursor_token| {
        let mut paged_query = session
            .load::<UniqueIndexRangeEntity>()
            .filter(range_predicate.clone())
            .order_by_desc("code")
            .order_by_desc("id")
            .limit(2);
        if let Some(token) = cursor_token {
            paged_query = paged_query.cursor(token);
        }
        let execution = paged_query
            .execute_paged()
            .expect("paged DESC index-range execute should succeed");

        (
            execution
                .response()
                .ids()
                .map(|id| id.key())
                .collect::<Vec<_>>(),
            execution
                .continuation_cursor()
                .map(crate::db::encode_cursor),
        )
    })
}

fn desc_cursor_resume_cases() -> [DescCursorResumeCase; 3] {
    [
        DescCursorResumeCase {
            label: "simple_desc_cursor_resume",
            run: run_desc_cursor_resume_simple_case,
            assert_strict_descending: true,
        },
        DescCursorResumeCase {
            label: "secondary_index_desc_cursor_resume",
            run: run_desc_cursor_resume_secondary_index_case,
            assert_strict_descending: false,
        },
        DescCursorResumeCase {
            label: "index_range_desc_cursor_resume",
            run: run_desc_cursor_resume_index_range_case,
            assert_strict_descending: false,
        },
    ]
}

fn build_simple_by_ids_ordered_page_plan(
    ids: impl IntoIterator<Item = Ulid>,
    descending: bool,
    limit: u32,
    offset: u32,
) -> PreparedExecutionPlan<SimpleEntity> {
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
        .map(PreparedExecutionPlan::from)
        .expect("simple by-ids ordered pagination plan should build")
}

fn simple_pagination_keys() -> [u128; 5] {
    [5, 1, 4, 2, 3]
}

fn build_simple_fixed_by_ids_ordered_page_plan(
    keys: &[u128; 5],
    descending: bool,
    limit: u32,
    offset: u32,
) -> PreparedExecutionPlan<SimpleEntity> {
    build_simple_by_ids_ordered_page_plan(
        keys.iter().copied().map(Ulid::from_u128),
        descending,
        limit,
        offset,
    )
}

fn execute_simple_fast_and_fallback_seed_pages(
    load: &LoadExecutor<SimpleEntity>,
    keys: &[u128; 5],
    descending: bool,
    limit: u32,
    offset: u32,
) -> (CursorPage<SimpleEntity>, CursorPage<SimpleEntity>) {
    let fast_page = load
        .execute_paged_with_cursor(
            build_simple_ordered_page_plan(descending, limit, offset),
            None,
        )
        .expect("simple fast-path seed page should execute");
    let fallback_page = load
        .execute_paged_with_cursor(
            build_simple_fixed_by_ids_ordered_page_plan(keys, descending, limit, offset),
            None,
        )
        .expect("simple by-ids seed page should execute");

    (fast_page, fallback_page)
}

fn execute_simple_fast_and_fallback_replay_pages(
    load: &LoadExecutor<SimpleEntity>,
    keys: &[u128; 5],
    descending: bool,
    limit: u32,
    offset: u32,
    fast_cursor: &[u8],
    fallback_cursor: &[u8],
) -> (CursorPage<SimpleEntity>, CursorPage<SimpleEntity>) {
    let fast_page = load
        .execute_paged_with_cursor(
            build_simple_ordered_page_plan(descending, limit, offset),
            build_simple_ordered_page_plan(descending, limit, offset)
                .prepare_cursor(Some(fast_cursor))
                .expect("simple fast-path replay cursor should validate"),
        )
        .expect("simple fast-path replay page should execute");
    let fallback_page = load
        .execute_paged_with_cursor(
            build_simple_fixed_by_ids_ordered_page_plan(keys, descending, limit, offset),
            build_simple_fixed_by_ids_ordered_page_plan(keys, descending, limit, offset)
                .prepare_cursor(Some(fallback_cursor))
                .expect("simple by-ids replay cursor should validate"),
        )
        .expect("simple by-ids replay page should execute");

    (fast_page, fallback_page)
}

fn assert_simple_token_page1_contracts(
    case_name: &str,
    fast_cursor: &PageCursor,
    fallback_cursor: &PageCursor,
    fast_token: &[u8],
    fallback_token: &[u8],
) {
    assert_eq!(
        decode_boundary(fast_token, "fast token replay boundary should decode"),
        fast_cursor
            .as_scalar()
            .expect("fast token replay cursor should stay scalar")
            .boundary()
            .clone(),
        "fast token decode boundary should match emitted boundary for case={case_name}",
    );
    assert_eq!(
        decode_boundary(
            fallback_token,
            "fallback token replay boundary should decode"
        ),
        fallback_cursor
            .as_scalar()
            .expect("fallback token replay cursor should stay scalar")
            .boundary()
            .clone(),
        "fallback token decode boundary should match emitted boundary for case={case_name}",
    );
    assert_eq!(
        fast_cursor
            .as_scalar()
            .expect("fast token replay cursor should stay scalar")
            .boundary(),
        fallback_cursor
            .as_scalar()
            .expect("fallback token replay cursor should stay scalar")
            .boundary(),
        "token replay page1 boundaries should match across equivalent shapes for case={case_name}",
    );
    assert_ne!(
        fast_cursor
            .as_scalar()
            .expect("fast token replay cursor should stay scalar")
            .signature(),
        fallback_cursor
            .as_scalar()
            .expect("fallback token replay cursor should stay scalar")
            .signature(),
        "token replay page1 signatures must remain shape-specific for case={case_name}",
    );
}

fn assert_simple_token_page2_and_cross_shape_contracts(
    load: &LoadExecutor<SimpleEntity>,
    keys: &[u128; 5],
    descending: bool,
    case_name: &str,
    fast_token: &[u8],
    fallback_token: &[u8],
) {
    let (fast_page_2, fallback_page_2) = execute_simple_fast_and_fallback_replay_pages(
        load,
        keys,
        descending,
        2,
        1,
        fast_token,
        fallback_token,
    );
    assert_eq!(
        simple_ids_from_items(&fast_page_2.items),
        simple_ids_from_items(&fallback_page_2.items),
        "token replay page2 rows should match across equivalent shapes for case={case_name}",
    );
    assert_eq!(
        fast_page_2.next_cursor.is_some(),
        fallback_page_2.next_cursor.is_some(),
        "token replay page2 cursor presence should match across equivalent shapes for case={case_name}",
    );
    if let (Some(fast_cursor), Some(fallback_cursor)) =
        (&fast_page_2.next_cursor, &fallback_page_2.next_cursor)
    {
        assert_eq!(
            fast_cursor
                .as_scalar()
                .expect("fast token replay page2 cursor should stay scalar")
                .boundary()
                .clone(),
            fallback_cursor
                .as_scalar()
                .expect("fallback token replay page2 cursor should stay scalar")
                .boundary()
                .clone(),
            "token replay page2 boundaries should match across equivalent shapes for case={case_name}",
        );
    }

    let fallback_cross_shape_err =
        build_simple_fixed_by_ids_ordered_page_plan(keys, descending, 2, 1)
            .prepare_cursor(Some(fast_token))
            .expect_err("fallback shape should reject fast token");
    assert!(
        matches!(
            fallback_cross_shape_err,
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::cursor::CursorPlanError::ContinuationCursorSignatureMismatch { .. }
                )
        ),
        "cross-shape fallback token replay should fail with signature mismatch for case={case_name}",
    );

    let fast_cross_shape_err = build_simple_ordered_page_plan(descending, 2, 1)
        .prepare_cursor(Some(fallback_token))
        .expect_err("fast shape should reject fallback token");
    assert!(
        matches!(
            fast_cross_shape_err,
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::cursor::CursorPlanError::ContinuationCursorSignatureMismatch { .. }
                )
        ),
        "cross-shape fast token replay should fail with signature mismatch for case={case_name}",
    );
}

fn build_simple_access_ordered_page_plan(
    access: AccessPlan<Ulid>,
    descending: bool,
    limit: u32,
) -> PreparedExecutionPlan<SimpleEntity> {
    PreparedExecutionPlan::new(AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
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
                offset: 0,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access: access.into_value_plan(),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
    })
}

fn build_pushdown_access_ordered_page_plan(
    access: AccessPlan<Ulid>,
    rank_direction: OrderDirection,
    id_direction: OrderDirection,
    limit: u32,
) -> PreparedExecutionPlan<PushdownParityEntity> {
    PreparedExecutionPlan::new(AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![
                    ("rank".to_string(), rank_direction),
                    ("id".to_string(), id_direction),
                ],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(limit),
                offset: 0,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access: access.into_value_plan(),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
    })
}

fn assert_pushdown_access_permutation_case(
    build_left_plan: impl Fn() -> PreparedExecutionPlan<PushdownParityEntity>,
    build_right_plan: impl Fn() -> PreparedExecutionPlan<PushdownParityEntity>,
    case_name: &'static str,
) {
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let (left_ids, left_boundaries) =
        collect_pushdown_pages_from_executable_plan(&load, build_left_plan, 20);
    let (right_ids, right_boundaries) =
        collect_pushdown_pages_from_executable_plan(&load, build_right_plan, 20);

    assert_eq!(
        left_ids, right_ids,
        "{case_name}: child-plan permutation must not change paged row sequence",
    );
    assert_eq!(
        left_boundaries, right_boundaries,
        "{case_name}: child-plan permutation must not change continuation boundaries",
    );
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
fn load_offset_pagination_continuation_token_bytes_are_stable_for_same_plan_shape() {
    setup_pagination_test();
    seed_simple_rows(&[5, 1, 4, 2, 3]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let query_signature = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("query signature plan should build")
        .into_inner()
        .continuation_signature(SimpleEntity::PATH);

    let page_plan_a = build_simple_ordered_page_plan(false, 2, 1);
    let page_boundary_a = page_plan_a
        .prepare_cursor(None)
        .expect("page boundary A should plan");
    let page_a = load
        .execute_paged_with_cursor(page_plan_a, page_boundary_a)
        .expect("page A should execute");
    let token_a = page_a
        .next_cursor
        .as_ref()
        .expect("page A should emit continuation cursor");
    let bytes_a = token_a
        .encode()
        .expect("continuation cursor A should serialize");

    let page_plan_b = build_simple_ordered_page_plan(false, 2, 1);
    let page_boundary_b = page_plan_b
        .prepare_cursor(None)
        .expect("page boundary B should plan");
    let page_b = load
        .execute_paged_with_cursor(page_plan_b, page_boundary_b)
        .expect("page B should execute");
    let token_b = page_b
        .next_cursor
        .as_ref()
        .expect("page B should emit continuation cursor");
    let bytes_b = token_b
        .encode()
        .expect("continuation cursor B should serialize");

    assert_eq!(
        token_a
            .as_scalar()
            .expect("page A cursor should stay scalar")
            .signature(),
        query_signature,
        "continuation token must carry the query-derived continuation signature (run A)",
    );
    assert_eq!(
        token_b
            .as_scalar()
            .expect("page B cursor should stay scalar")
            .signature(),
        query_signature,
        "continuation token must carry the query-derived continuation signature (run B)",
    );
    assert_eq!(
        bytes_a, bytes_b,
        "continuation token bytes should remain stable for the same plan shape and dataset",
    );
}

#[test]
fn load_cursor_initial_to_continuation_matrix_covers_direction_and_window_semantics() {
    setup_pagination_test();
    seed_simple_rows(&[6, 1, 5, 2, 4, 3, 7]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    for (case_name, descending, offset, limit) in [
        ("asc_offset0_limit2", false, 0_u32, 2_u32),
        ("asc_offset1_limit2", false, 1_u32, 2_u32),
        ("desc_offset0_limit2", true, 0_u32, 2_u32),
        ("desc_offset1_limit2", true, 1_u32, 2_u32),
    ] {
        let build_plan = || build_simple_ordered_page_plan(descending, limit, offset);

        // Phase 1: execute the initial page from an empty cursor boundary.
        let page_1_plan = build_plan();
        let page_1_cursor = page_1_plan
            .prepare_cursor(None)
            .expect("page-1 cursor should plan");
        let page_1 = load
            .execute_paged_with_cursor(page_1_plan, page_1_cursor)
            .expect("page-1 execution should succeed");

        // Phase 2: derive expected canonical window and assert first-page output.
        let mut expected_ids = (1_u128..=7).map(Ulid::from_u128).collect::<Vec<_>>();
        if descending {
            expected_ids.reverse();
        }
        let expected_ids = expected_ids
            .into_iter()
            .skip(offset as usize)
            .collect::<Vec<_>>();
        let expected_page_1 = expected_ids
            .iter()
            .copied()
            .take(limit as usize)
            .collect::<Vec<_>>();
        assert_eq!(
            simple_ids_from_items(&page_1.items),
            expected_page_1,
            "first-page rows must match canonical cursor window for case={case_name}",
        );

        // Phase 3: resume from continuation and assert strict suffix progression.
        let expected_page_2 = expected_ids
            .iter()
            .copied()
            .skip(limit as usize)
            .take(limit as usize)
            .collect::<Vec<_>>();
        if expected_page_2.is_empty() {
            assert!(
                page_1.next_cursor.is_none(),
                "terminal first page should not emit continuation for case={case_name}",
            );
            continue;
        }

        let cursor = page_1
            .next_cursor
            .expect("non-terminal first page should emit continuation cursor");
        let page_2_plan = build_plan();
        let page_2_cursor = page_2_plan
            .prepare_cursor(Some(
                cursor
                    .encode()
                    .expect("continuation cursor should serialize")
                    .as_slice(),
            ))
            .expect("page-2 cursor should plan");
        let page_2 = load
            .execute_paged_with_cursor(page_2_plan, page_2_cursor)
            .expect("page-2 execution should succeed");
        assert_eq!(
            simple_ids_from_items(&page_2.items),
            expected_page_2,
            "resumed rows must continue canonical window without offset replay for case={case_name}",
        );
    }
}

#[test]
fn load_cursor_with_offset_fallback_resume_matrix_is_boundary_complete() {
    setup_pagination_test();
    seed_simple_rows(&[42_201, 42_202, 42_203, 42_204, 42_205, 42_206, 42_207]);

    let fallback_ids = vec![
        Ulid::from_u128(42_204),
        Ulid::from_u128(42_201),
        Ulid::from_u128(42_207),
        Ulid::from_u128(42_202),
        Ulid::from_u128(42_206),
        Ulid::from_u128(42_203),
        Ulid::from_u128(42_205),
    ];
    let load = LoadExecutor::<SimpleEntity>::new(DB, true);

    for (case_name, descending) in [("asc", false), ("desc", true)] {
        let build_plan = || {
            build_simple_by_ids_ordered_page_plan(fallback_ids.iter().copied(), descending, 2, 1)
        };

        let (_seed_page, seed_trace) = load
            .execute_paged_with_cursor_traced(build_plan(), None)
            .expect("fallback offset seed page should execute");
        let seed_trace = seed_trace.expect("debug trace should be present");
        assert_eq!(
            seed_trace.optimization(),
            None,
            "fallback by-ids offset shape should remain non-optimized for case={case_name}",
        );

        let mut expected_ids = fallback_ids.clone();
        expected_ids.sort();
        if descending {
            expected_ids.reverse();
        }
        let expected_ids = expected_ids.into_iter().skip(1).collect::<Vec<_>>();

        let (ids, _boundaries, tokens) =
            collect_simple_pages_from_executable_plan_with_tokens(&load, build_plan, 20);
        assert_eq!(
            ids, expected_ids,
            "fallback offset traversal must preserve canonical order for case={case_name}",
        );
        assert_simple_resume_suffixes_from_tokens(
            &load,
            &build_plan,
            &tokens,
            &expected_ids,
            case_name,
        );
    }
}

#[test]
fn load_cursor_pagination_pk_fast_path_matches_non_fast_with_same_cursor_boundary() {
    setup_pagination_test();

    // Phase 1: seed rows with non-sorted insertion order.
    let keys = [7_u128, 1_u128, 6_u128, 2_u128, 5_u128, 3_u128, 4_u128];
    seed_simple_rows(&keys);

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    // Phase 2: capture one canonical cursor boundary from an initial fast-path page.
    let page1_plan = build_simple_ordered_page_plan(false, 3, 0);
    let page1_boundary = page1_plan
        .prepare_cursor(None)
        .expect("cursor source boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("cursor source page should execute");
    let cursor_bytes = page1
        .next_cursor
        .as_ref()
        .expect("cursor source page should emit continuation cursor");
    let shared_boundary = cursor_bytes
        .as_scalar()
        .expect("cursor source page should stay scalar")
        .boundary()
        .clone();

    // Phase 3: execute page-2 parity checks with the same typed cursor boundary.
    let fast_page2_plan = build_simple_ordered_page_plan(false, 2, 0);
    let fast_page2 = load
        .execute_paged_with_cursor(fast_page2_plan, Some(shared_boundary.clone()))
        .expect("fast page2 should execute");

    let non_fast_page2_plan =
        build_simple_by_ids_ordered_page_plan(keys.into_iter().map(Ulid::from_u128), false, 2, 0);
    let non_fast_page2 = load
        .execute_paged_with_cursor(non_fast_page2_plan, Some(shared_boundary))
        .expect("non-fast page2 should execute");

    let fast_ids = simple_ids_from_items(&fast_page2.items);
    let non_fast_ids = simple_ids_from_items(&non_fast_page2.items);
    assert_eq!(
        fast_ids, non_fast_ids,
        "fast and non-fast paths must return identical rows for the same cursor boundary",
    );

    assert_eq!(
        fast_page2.next_cursor.is_some(),
        non_fast_page2.next_cursor.is_some(),
        "cursor presence must match between fast and non-fast paths",
    );

    let fast_next = fast_page2
        .next_cursor
        .as_ref()
        .expect("fast page2 should emit continuation cursor");
    let non_fast_next = non_fast_page2
        .next_cursor
        .as_ref()
        .expect("non-fast page2 should emit continuation cursor");
    let fast_next_boundary = fast_next
        .as_scalar()
        .expect("fast page2 cursor should stay scalar")
        .boundary()
        .clone();
    let non_fast_next_boundary = non_fast_next
        .as_scalar()
        .expect("non-fast page2 cursor should stay scalar")
        .boundary()
        .clone();
    assert_eq!(
        &fast_next_boundary, &non_fast_next_boundary,
        "fast and non-fast paths must emit the same continuation boundary",
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
        .map(PreparedExecutionPlan::from)
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
        .map(PreparedExecutionPlan::from)
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
        .map(PreparedExecutionPlan::from)
        .expect("pushdown plan should build");
    let pushdown_page = load
        .execute_paged_with_cursor(pushdown_plan, None)
        .expect("pushdown page should execute");

    let fallback_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .by_ids(group7_ids.iter().copied())
        .order_by("rank")
        .limit(2)
        .plan()
        .map(PreparedExecutionPlan::from)
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
        .map(PreparedExecutionPlan::from)
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
        .map(PreparedExecutionPlan::from)
        .expect("pushdown page2 plan should build");
    let pushdown_page2 = load
        .execute_paged_with_cursor(pushdown_page2_plan, Some(shared_boundary.clone()))
        .expect("pushdown page2 should execute");

    let fallback_page2_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .by_ids(group7_ids.iter().copied())
        .order_by("rank")
        .limit(2)
        .plan()
        .map(PreparedExecutionPlan::from)
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
        .map(PreparedExecutionPlan::from)
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
        .map(PreparedExecutionPlan::from)
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
                .map(PreparedExecutionPlan::from)
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
        .map(PreparedExecutionPlan::from)
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
        .map(PreparedExecutionPlan::from)
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
                .map(PreparedExecutionPlan::from)
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
        .map(PreparedExecutionPlan::from)
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
fn load_cursor_pagination_pk_order_key_range_respects_bounds() {
    setup_pagination_test();
    seed_simple_rows(&[1, 2, 3, 4, 5]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let page_1_plan = build_simple_key_range_page_plan(2, 4, OrderDirection::Asc, Some(2), 0);
    let page_1_boundary = page_1_plan
        .prepare_cursor(None)
        .expect("pk-range page1 boundary should plan");
    let page_1 = load
        .execute_paged_with_cursor(page_1_plan, page_1_boundary)
        .expect("pk-range page1 should execute");
    assert_eq!(
        simple_ids_from_items(&page_1.items),
        vec![Ulid::from_u128(2), Ulid::from_u128(3)],
        "key-range page1 should stay inside the declared primary-key bounds",
    );

    let page_2_plan = build_simple_key_range_page_plan(2, 4, OrderDirection::Asc, Some(2), 0);
    let page_2_boundary = page_2_plan
        .prepare_cursor(Some(
            page_1
                .next_cursor
                .as_ref()
                .expect("pk-range page1 should emit continuation cursor")
                .encode()
                .expect("pk-range continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("pk-range page2 boundary should plan");
    let page_2 = load
        .execute_paged_with_cursor(page_2_plan, page_2_boundary)
        .expect("pk-range page2 should execute");
    assert_eq!(
        simple_ids_from_items(&page_2.items),
        vec![Ulid::from_u128(4)],
        "pk-range continuation should resume within the same declared bounds",
    );
    assert!(
        page_2.next_cursor.is_none(),
        "final bounded key-range page should not emit continuation",
    );
}

#[test]
fn load_cursor_pagination_pk_order_key_range_cursor_past_end_returns_empty_page() {
    setup_pagination_test();
    seed_simple_rows(&[1, 2, 3]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let page = load
        .execute_paged_with_cursor(
            build_simple_key_range_page_plan(1, 2, OrderDirection::Asc, Some(2), 0),
            Some(CursorBoundary {
                slots: vec![CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(
                    99,
                )))],
            }),
        )
        .expect("pk-range cursor past end should execute");

    assert!(
        page.items.is_empty(),
        "cursor beyond range end should produce an empty page",
    );
    assert!(
        page.next_cursor.is_none(),
        "empty page should not emit a continuation cursor",
    );
}

#[test]
fn load_cursor_pagination_pk_order_inverted_key_range_returns_empty_without_scan() {
    setup_pagination_test();
    seed_simple_rows(&[1, 2, 3, 4]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    for (case_name, direction) in [("asc", OrderDirection::Asc), ("desc", OrderDirection::Desc)] {
        let err = load
            .execute_paged_with_cursor_traced(
                build_simple_key_range_page_plan(4, 2, direction, Some(2), 0),
                None,
            )
            .expect_err("inverted manual key-range should fail closed");
        assert_eq!(
            err.class,
            crate::error::ErrorClass::InvariantViolation,
            "inverted manual key-range should classify as an invariant violation for case={case_name}",
        );
        assert_eq!(
            err.origin,
            crate::error::ErrorOrigin::Query,
            "inverted manual key-range should stay query-owned for case={case_name}",
        );
        assert!(
            err.message.contains("key range start is greater than end"),
            "inverted manual key-range should report the start/end invariant for case={case_name}: {err:?}",
        );
    }
}

#[test]
fn load_cursor_pagination_pk_trace_reports_non_top_n_variant_without_page_limit() {
    setup_pagination_test();
    seed_simple_rows(&[1, 2, 3, 4]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("pk non-top-n trace plan should build");

    let (page, trace) = load
        .execute_paged_with_cursor_traced(plan, None)
        .expect("pk non-top-n trace execution should succeed");
    let trace = trace.expect("debug trace should be present");

    assert_eq!(
        trace.optimization(),
        Some(ExecutionOptimization::PrimaryKey),
        "unpaged PK ordered shapes should report non-top-n PK optimization labels",
    );
    assert_eq!(
        page.items.len(),
        4,
        "unpaged PK ordered execution should return all seeded rows",
    );
}

#[test]
fn load_cursor_pagination_pk_order_missing_slot_is_unsupported() {
    setup_pagination_test();
    seed_simple_rows(&[1, 2]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = build_simple_ordered_page_plan(false, 1, 0);
    let err = load
        .execute_paged_with_cursor(
            plan,
            Some(CursorBoundary {
                slots: vec![CursorBoundarySlot::Missing],
            }),
        )
        .expect_err("missing pk slot should be rejected as unsupported cursor input");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "missing pk slot should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Cursor,
        "missing pk slot should originate from cursor validation checks",
    );
    assert!(
        err.message
            .contains("continuation cursor primary key type mismatch"),
        "missing pk slot should return a clear cursor mismatch message: {err:?}",
    );
}

#[test]
fn load_cursor_pagination_pk_order_type_mismatch_is_unsupported() {
    setup_pagination_test();
    seed_simple_rows(&[1, 2]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = build_simple_ordered_page_plan(false, 1, 0);
    let err = load
        .execute_paged_with_cursor(
            plan,
            Some(CursorBoundary {
                slots: vec![CursorBoundarySlot::Present(Value::Text(
                    "not-a-ulid".to_string(),
                ))],
            }),
        )
        .expect_err("pk slot type mismatch should be rejected as unsupported cursor input");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "pk slot mismatch should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Cursor,
        "pk slot mismatch should originate from cursor validation checks",
    );
    assert!(
        err.message
            .contains("continuation cursor primary key type mismatch"),
        "pk slot mismatch should return a clear cursor mismatch message: {err:?}",
    );
}

#[test]
fn load_cursor_pagination_pk_order_arity_mismatch_is_unsupported() {
    setup_pagination_test();
    seed_simple_rows(&[1, 2]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = build_simple_ordered_page_plan(false, 1, 0);
    let err = load
        .execute_paged_with_cursor(
            plan,
            Some(CursorBoundary {
                slots: vec![
                    CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(1))),
                    CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(2))),
                ],
            }),
        )
        .expect_err("pk slot arity mismatch should be rejected as unsupported cursor input");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "pk slot arity mismatch should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Cursor,
        "pk slot arity mismatch should originate from cursor validation checks",
    );
    assert!(
        err.message
            .contains("continuation cursor boundary arity mismatch"),
        "pk slot arity mismatch should return a clear cursor mismatch message: {err:?}",
    );
}

#[test]
fn load_cursor_pagination_skips_strictly_before_limit() {
    setup_pagination_test();
    seed_phase_rows(&[
        PhaseEntity {
            id: Ulid::from_u128(1100),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![1],
            label: "r10".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1101),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![2],
            label: "r20-a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1102),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![3],
            label: "r20-b".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1103),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![4],
            label: "r30".to_string(),
        },
    ]);

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let page_1 = execute_phase_rank_page(&load, false, 1, None);
    assert_eq!(page_1.items.len(), 1, "page1 should return one row");
    assert_eq!(page_1.items[0].entity_ref().id, Ulid::from_u128(1100));

    let page_2 = execute_phase_rank_page(
        &load,
        false,
        1,
        Some(
            page_1
                .next_cursor
                .as_ref()
                .expect("page1 should emit a continuation cursor")
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ),
    );
    assert_eq!(page_2.items.len(), 1, "page2 should return one row");
    assert_eq!(
        page_2.items[0].entity_ref().id,
        Ulid::from_u128(1101),
        "cursor boundary must be applied before limit using strict ordering",
    );

    let page_3 = execute_phase_rank_page(
        &load,
        false,
        1,
        Some(
            page_2
                .next_cursor
                .as_ref()
                .expect("page2 should emit a continuation cursor")
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ),
    );
    assert_eq!(page_3.items.len(), 1, "page3 should return one row");
    assert_eq!(
        page_3.items[0].entity_ref().id,
        Ulid::from_u128(1102),
        "strict cursor continuation must advance beyond the last returned row",
    );
}

#[test]
fn load_cursor_next_cursor_uses_last_returned_row_boundary() {
    setup_pagination_test();
    seed_phase_rows(&[
        PhaseEntity {
            id: Ulid::from_u128(1200),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![1],
            label: "r10".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1201),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![2],
            label: "r20-a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1202),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![3],
            label: "r20-b".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1203),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![4],
            label: "r30".to_string(),
        },
    ]);

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let page_1 = execute_phase_rank_page(&load, false, 2, None);
    assert_eq!(page_1.items.len(), 2, "page1 should return two rows");
    assert_eq!(page_1.items[0].entity_ref().id, Ulid::from_u128(1200));
    assert_eq!(
        page_1.items[1].entity_ref().id,
        Ulid::from_u128(1201),
        "page1 second row should be the PK tie-break winner for rank=20",
    );

    let token = page_1
        .next_cursor
        .as_ref()
        .expect("page1 should include next cursor")
        .as_scalar()
        .expect("phase continuation should stay scalar");
    assert_eq!(
        token.boundary(),
        &pushdown_rank_id_boundary(20, 1201),
        "next cursor must encode the last returned row boundary",
    );

    let page_2 = execute_phase_rank_page(
        &load,
        false,
        2,
        Some(
            page_1
                .next_cursor
                .as_ref()
                .expect("page1 should include next cursor")
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ),
    );
    assert_eq!(
        phase_ids_from_items(&page_2.items),
        vec![Ulid::from_u128(1202), Ulid::from_u128(1203)],
        "page2 should resume strictly after page1's final row",
    );
    assert!(
        page_2.next_cursor.is_none(),
        "final page should not emit a continuation cursor",
    );
}

#[test]
fn load_cursor_pagination_desc_order_resumes_strictly_after_boundary() {
    setup_pagination_test();
    seed_phase_rows(&[
        PhaseEntity {
            id: Ulid::from_u128(1400),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![1],
            label: "r10".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1401),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![2],
            label: "r20-a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1402),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![3],
            label: "r20-b".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1403),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![4],
            label: "r30".to_string(),
        },
    ]);

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let page_1 = execute_phase_rank_page(&load, true, 2, None);
    assert_eq!(
        phase_ids_from_items(&page_1.items),
        vec![Ulid::from_u128(1403), Ulid::from_u128(1401)],
        "descending page1 should apply rank DESC then canonical PK tie-break",
    );

    let page_2 = execute_phase_rank_page(
        &load,
        true,
        2,
        Some(
            page_1
                .next_cursor
                .as_ref()
                .expect("descending page1 should emit continuation cursor")
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ),
    );
    assert_eq!(
        phase_ids_from_items(&page_2.items),
        vec![Ulid::from_u128(1402), Ulid::from_u128(1400)],
        "descending continuation must resume strictly after the boundary row",
    );
    assert!(
        page_2.next_cursor.is_none(),
        "final descending page should not emit a continuation cursor",
    );
}

#[test]
fn load_desc_order_uses_primary_key_tie_break_for_equal_rank_rows() {
    setup_pagination_test();
    seed_phase_rows(&[
        PhaseEntity {
            id: Ulid::from_u128(14_500),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![1],
            label: "r30".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(14_503),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![2],
            label: "r20-c".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(14_501),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![3],
            label: "r20-a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(14_502),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![4],
            label: "r20-b".to_string(),
        },
    ]);

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let page = load
        .execute_paged_with_cursor(
            Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
                .order_by_desc("rank")
                .limit(4)
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("descending tie-break plan should build"),
            None,
        )
        .expect("descending tie-break page should execute");

    assert_eq!(
        phase_ids_from_items(&page.items),
        vec![
            Ulid::from_u128(14_500),
            Ulid::from_u128(14_501),
            Ulid::from_u128(14_502),
            Ulid::from_u128(14_503),
        ],
        "descending primary comparator must preserve canonical PK tie-break ordering",
    );
}

#[test]
fn load_cursor_rejects_signature_mismatch() {
    setup_pagination_test();
    seed_phase_rows(&[
        PhaseEntity {
            id: Ulid::from_u128(1300),
            opt_rank: Some(1),
            rank: 1,
            tags: vec![1],
            label: "a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1301),
            opt_rank: Some(2),
            rank: 2,
            tags: vec![2],
            label: "b".to_string(),
        },
    ]);

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let asc_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .limit(1)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("ascending cursor plan should build");
    let asc_page = load
        .execute_paged_with_cursor(
            asc_plan,
            Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
                .order_by("rank")
                .limit(1)
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("ascending boundary plan should build")
                .prepare_cursor(None)
                .expect("ascending boundary should plan"),
        )
        .expect("ascending cursor page should execute");
    let cursor = asc_page
        .next_cursor
        .expect("ascending page should emit cursor");

    let desc_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("rank")
        .limit(1)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("descending plan should build");
    let err = desc_plan
        .prepare_cursor(Some(
            cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect_err("cursor from different canonical plan should be rejected");
    assert!(
        matches!(
            err,
            crate::db::executor::ExecutorPlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::cursor::CursorPlanError::ContinuationCursorSignatureMismatch { .. }
                )
        ),
        "planning should reject plan-signature mismatch",
    );
}

#[test]
fn load_cursor_rejects_signature_mismatch_between_pk_fast_and_by_ids_shapes() {
    setup_pagination_test();
    let keys = simple_pagination_keys();
    seed_simple_rows(&keys);

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let fast_seed_page = load
        .execute_paged_with_cursor(build_simple_ordered_page_plan(false, 2, 1), None)
        .expect("fast seed page should execute");
    let fast_cursor = fast_seed_page
        .next_cursor
        .as_ref()
        .expect("fast seed page should emit continuation cursor");
    let fallback_seed_page = load
        .execute_paged_with_cursor(
            build_simple_fixed_by_ids_ordered_page_plan(&keys, false, 2, 1),
            None,
        )
        .expect("fallback seed page should execute");
    let fallback_cursor = fallback_seed_page
        .next_cursor
        .as_ref()
        .expect("fallback seed page should emit continuation cursor");
    assert_eq!(
        fast_cursor
            .as_scalar()
            .expect("fast seed cursor should stay scalar")
            .boundary(),
        fallback_cursor
            .as_scalar()
            .expect("fallback seed cursor should stay scalar")
            .boundary(),
        "fast and fallback cursor boundaries should match for the same ordered window",
    );

    let err = build_simple_fixed_by_ids_ordered_page_plan(&keys, false, 2, 1)
        .prepare_cursor(Some(
            fast_cursor
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
        "cross-shape cursor replay should fail with signature mismatch",
    );
}

#[test]
fn load_cursor_resume_parity_holds_between_pk_fast_and_by_ids_with_shape_local_tokens() {
    setup_pagination_test();
    let keys = simple_pagination_keys();
    seed_simple_rows(&keys);

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    for (case_name, descending) in [("asc", false), ("desc", true)] {
        let (fast_page_1, fallback_page_1) =
            execute_simple_fast_and_fallback_seed_pages(&load, &keys, descending, 2, 1);
        let fast_cursor = fast_page_1
            .next_cursor
            .as_ref()
            .expect("fast page1 should emit continuation cursor");
        let fallback_cursor = fallback_page_1
            .next_cursor
            .as_ref()
            .expect("fallback page1 should emit continuation cursor");

        assert_eq!(
            simple_ids_from_items(&fast_page_1.items),
            simple_ids_from_items(&fallback_page_1.items),
            "page1 rows should match across equivalent fast/fallback shapes for case={case_name}",
        );
        assert_eq!(
            fast_cursor
                .as_scalar()
                .expect("fast page1 cursor should stay scalar")
                .boundary(),
            fallback_cursor
                .as_scalar()
                .expect("fallback page1 cursor should stay scalar")
                .boundary(),
            "page1 cursor boundaries should match across equivalent fast/fallback shapes for case={case_name}",
        );
        assert_ne!(
            fast_cursor
                .as_scalar()
                .expect("fast page1 cursor should stay scalar")
                .signature(),
            fallback_cursor
                .as_scalar()
                .expect("fallback page1 cursor should stay scalar")
                .signature(),
            "equivalent semantics across different access shapes must keep distinct continuation signatures for case={case_name}",
        );

        let fast_token = fast_cursor
            .encode()
            .expect("fast continuation cursor should serialize");
        let fallback_token = fallback_cursor
            .encode()
            .expect("fallback continuation cursor should serialize");
        let (fast_page_2, fallback_page_2) = execute_simple_fast_and_fallback_replay_pages(
            &load,
            &keys,
            descending,
            2,
            1,
            fast_token.as_slice(),
            fallback_token.as_slice(),
        );

        assert_eq!(
            simple_ids_from_items(&fast_page_2.items),
            simple_ids_from_items(&fallback_page_2.items),
            "page2 rows should match after local-token replay for case={case_name}",
        );
        assert_eq!(
            fast_page_2.next_cursor.is_some(),
            fallback_page_2.next_cursor.is_some(),
            "cursor emission parity should match after page2 replay for case={case_name}",
        );
    }
}

#[test]
fn load_cursor_token_replay_parity_holds_between_pk_fast_and_by_ids_shapes() {
    setup_pagination_test();
    let keys = simple_pagination_keys();
    seed_simple_rows(&keys);

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    for (case_name, descending) in [("asc", false), ("desc", true)] {
        let (fast_page_1, fallback_page_1) =
            execute_simple_fast_and_fallback_seed_pages(&load, &keys, descending, 2, 1);
        let fast_cursor = fast_page_1
            .next_cursor
            .as_ref()
            .expect("fast token replay page1 should emit continuation cursor");
        let fast_token = encode_token(
            fast_cursor,
            "fast token replay cursor should serialize for replay",
        );
        let fallback_cursor = fallback_page_1
            .next_cursor
            .as_ref()
            .expect("fallback token replay page1 should emit continuation cursor");
        let fallback_token = encode_token(
            fallback_cursor,
            "fallback token replay cursor should serialize for replay",
        );

        assert_simple_token_page1_contracts(
            case_name,
            fast_cursor,
            fallback_cursor,
            fast_token.as_slice(),
            fallback_token.as_slice(),
        );
        assert_simple_token_page2_and_cross_shape_contracts(
            &load,
            &keys,
            descending,
            case_name,
            fast_token.as_slice(),
            fallback_token.as_slice(),
        );
    }
}

#[test]
fn load_cursor_with_offset_desc_secondary_pushdown_resume_matrix_is_boundary_complete() {
    setup_pagination_test();

    let rows = distinct_secondary_pushdown_rows(42_000);
    seed_pushdown_rows(&rows);
    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);
    for (case_name, descending) in [("asc", false), ("desc", true)] {
        let expected_optimization = if descending {
            None
        } else {
            Some(ExecutionOptimization::SecondaryOrderTopNSeek)
        };
        let build_plan = || {
            let base = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .limit(2)
                .offset(1);
            let ordered = if descending {
                base.order_by_desc("rank").order_by_desc("id")
            } else {
                base.order_by("rank").order_by("id")
            };

            ordered
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("secondary offset continuation plan should build")
        };

        let (_seed_page, seed_trace) = load
            .execute_paged_with_cursor_traced(build_plan(), None)
            .expect("secondary offset seed page should execute");
        let seed_trace = seed_trace.expect("debug trace should be present");
        assert_eq!(
            seed_trace.optimization(),
            expected_optimization,
            "secondary offset continuation shape should report the admitted bounded secondary optimization split for case={case_name}",
        );

        let expected_ids =
            ordered_pushdown_ids_with_rank_and_id_direction(&rows, 7, descending, descending)
                .into_iter()
                .skip(1)
                .collect::<Vec<_>>();
        let _ = collect_pushdown_pages_and_assert_token_resumes(
            &load,
            build_plan,
            &expected_ids,
            case_name,
        );
    }
}

#[test]
fn load_cursor_with_offset_index_range_pushdown_resume_matrix_is_boundary_complete() {
    setup_pagination_test();

    let rows = indexed_metric_range_rows(42_100);
    seed_indexed_metrics_rows(&rows);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    for (case_name, direction) in [("asc", OrderDirection::Asc), ("desc", OrderDirection::Desc)] {
        let build_plan = || {
            PreparedExecutionPlan::<IndexedMetricsEntity>::new(AccessPlannedQuery {
                logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                    mode: QueryMode::Load(LoadSpec::new()),
                    predicate: None,
                    order: Some(OrderSpec {
                        fields: vec![
                            ("tag".to_string(), direction),
                            ("id".to_string(), direction),
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
                access: AccessPlan::path(AccessPath::index_range(
                    INDEXED_METRICS_INDEX_MODELS[0],
                    Vec::new(),
                    Bound::Included(Value::Uint(10)),
                    Bound::Excluded(Value::Uint(30)),
                )),
                projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
                access_choice:
                    crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
                planner_route_profile:
                    crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(false),
                static_planning_shape: None,
            })
        };

        let (_seed_page, seed_trace) = load
            .execute_paged_with_cursor_traced(build_plan(), None)
            .expect("index-range offset seed page should execute");
        let seed_trace = seed_trace.expect("debug trace should be present");
        assert_eq!(
            seed_trace.optimization(),
            Some(ExecutionOptimization::IndexRangeLimitPushdown),
            "index-range offset shape should use limit pushdown for case={case_name}",
        );

        let expected_ids = ordered_index_candidate_ids_for_direction(&rows, 10, 30, direction)
            .into_iter()
            .skip(1)
            .collect::<Vec<_>>();
        let (ids, _boundaries, tokens) =
            collect_indexed_metric_pages_from_executable_plan_with_tokens(&load, build_plan, 20);
        assert_eq!(
            ids, expected_ids,
            "index-range offset traversal must preserve canonical order for case={case_name}",
        );
        assert_indexed_metric_resume_suffixes_from_tokens(
            &load,
            &build_plan,
            &tokens,
            &expected_ids,
            case_name,
        );
    }
}

#[test]
fn load_cursor_pagination_pk_fast_path_scan_accounting_tracks_access_candidates() {
    setup_pagination_test();
    seed_simple_rows(&[6, 1, 5, 2, 4, 3]);

    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    for (case_name, descending) in [("asc", false), ("desc", true)] {
        let base = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
            .limit(2)
            .offset(1);
        let plan = if descending {
            base.order_by_desc("id")
        } else {
            base.order_by("id")
        }
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("pk fast-path budget plan should build");

        let (_page, trace) = load
            .execute_paged_with_cursor_traced(plan, None)
            .expect("pk fast-path budget execution should succeed");
        let trace = trace.expect("debug trace should be present");
        assert_eq!(
            trace.optimization(),
            Some(ExecutionOptimization::PrimaryKeyTopNSeek),
            "pk trace should report PK fast path for case={case_name}",
        );
        assert_eq!(
            trace.keys_scanned(),
            6,
            "pk fast-path trace should count all access candidates for case={case_name}",
        );
    }
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
        .map(PreparedExecutionPlan::from)
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
        .map(PreparedExecutionPlan::from)
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
        .map(PreparedExecutionPlan::from)
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

    let plan = PreparedExecutionPlan::<PushdownParityEntity>::new(AccessPlannedQuery {
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
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
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

    let plan = PreparedExecutionPlan::<SimpleEntity>::new(AccessPlannedQuery {
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
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
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
        PreparedExecutionPlan::<PushdownParityEntity>::new(AccessPlannedQuery {
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
            access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
            planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
                false,
            ),
            static_planning_shape: None,
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
        .map(PreparedExecutionPlan::from)
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
                .map(PreparedExecutionPlan::from)
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
        .map(PreparedExecutionPlan::from)
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
                .map(PreparedExecutionPlan::from)
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
                .map(PreparedExecutionPlan::from)
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
        .map(PreparedExecutionPlan::from)
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
                .map(PreparedExecutionPlan::from)
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
                .map(PreparedExecutionPlan::from)
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
fn desc_cursor_resume_matrix_matches_unbounded_execution() {
    for case in desc_cursor_resume_cases() {
        let (resumed_desc_ids, expected_desc_ids) = (case.run)();
        assert_eq!(
            resumed_desc_ids, expected_desc_ids,
            "DESC cursor resume matrix mismatch for case={}",
            case.label
        );

        if case.assert_strict_descending {
            assert!(
                resumed_desc_ids
                    .windows(2)
                    .all(|window| window[0] > window[1]),
                "DESC cursor resume sequence should stay strictly descending without duplicates for case={}",
                case.label
            );
        }
    }
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
                .map(PreparedExecutionPlan::from)
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
            .map(PreparedExecutionPlan::from)
            .expect("composite monotonicity boundary plan should build");
        let page = load
            .execute_paged_with_cursor(
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .filter(predicate.clone())
                    .order_by("rank")
                    .limit(3)
                    .plan()
                    .map(PreparedExecutionPlan::from)
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
                .map(PreparedExecutionPlan::from)
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
                .map(PreparedExecutionPlan::from)
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
                .map(PreparedExecutionPlan::from)
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
                .map(PreparedExecutionPlan::from)
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
                    .map(PreparedExecutionPlan::from)
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
                PreparedExecutionPlan::from,
            ))
        } else {
            load.execute(query.order_by("tag").plan().map_or_else(
                |_| panic!("single-field {} asc plan should build", case.name),
                PreparedExecutionPlan::from,
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
                PreparedExecutionPlan::from,
            ))
        } else {
            load.execute(query.order_by("rank").plan().map_or_else(
                |_| panic!("composite {} asc plan should build", case.name),
                PreparedExecutionPlan::from,
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
        expected_optimization: Option<ExecutionOptimization>,
    }

    let cases = [
        Case {
            name: "accepted_ascending",
            prefix: 16_000,
            order: [("rank", OrderDirection::Asc), ("id", OrderDirection::Asc)],
            include_filter: true,
            expected_optimization: Some(ExecutionOptimization::SecondaryOrderTopNSeek),
        },
        Case {
            name: "accepted_with_filter",
            prefix: 17_000,
            order: [("rank", OrderDirection::Asc), ("id", OrderDirection::Asc)],
            include_filter: true,
            expected_optimization: Some(ExecutionOptimization::SecondaryOrderTopNSeek),
        },
        Case {
            name: "rejected_descending",
            prefix: 18_000,
            order: [("rank", OrderDirection::Desc), ("id", OrderDirection::Asc)],
            include_filter: true,
            expected_optimization: None,
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
                    .map(PreparedExecutionPlan::from)
                    .expect("trace outcome plan should build for case"),
                None,
            )
            .expect("trace outcome execution should succeed for case");
        let trace = trace.expect("debug trace should be present");
        assert_eq!(
            trace.optimization(),
            case.expected_optimization,
            "trace should emit expected secondary-order pushdown outcome for case '{}'",
            case.name,
        );
    }
}

#[test]
fn load_trace_marks_composite_index_range_pushdown_rejection_outcome() {
    setup_pagination_test();
    seed_pushdown_rows(&pushdown_trace_rows(22_000));

    let plan = PreparedExecutionPlan::<PushdownParityEntity>::new(AccessPlannedQuery {
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
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
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
        PreparedExecutionPlan::<PushdownParityEntity>::new(AccessPlannedQuery {
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
            access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
            planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
                false,
            ),
            static_planning_shape: None,
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
                .map(PreparedExecutionPlan::from)
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

        assert_pushdown_distinct_resume_matrix(
            &load,
            &[1_u32, 2, 3],
            &expected_ids,
            |limit| {
                PreparedExecutionPlan::<PushdownParityEntity>::new(AccessPlannedQuery {
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
                    access_choice:
                        crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
                    planner_route_profile:
                        crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(false),
                    static_planning_shape: None,
                })
            },
            format!("case '{case_name}'").as_str(),
        );
    }
}

#[test]
fn load_distinct_desc_secondary_pushdown_resume_matrix_is_boundary_complete() {
    setup_pagination_test();

    let rows = distinct_secondary_pushdown_rows(39_400);
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
                    .map(PreparedExecutionPlan::from)
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
    }

    assert_pushdown_distinct_resume_matrix(
        &load,
        &[1_u32, 2, 3],
        &expected_ids,
        |limit| {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .order_by_desc("rank")
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .plan()
                .map(PreparedExecutionPlan::from)
                .expect("distinct secondary DESC plan should build")
        },
        "distinct DESC secondary pushdown",
    );
}

#[test]
fn load_distinct_desc_secondary_fast_path_and_fallback_match_ids_and_boundaries() {
    setup_pagination_test();

    let rows = distinct_secondary_pushdown_rows(39_500);
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
                    .map(PreparedExecutionPlan::from)
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
                    .map(PreparedExecutionPlan::from)
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
    }

    assert_pushdown_pagination_parity_matrix(
        &load,
        &[1_u32, 2, 3],
        |limit| {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .order_by_desc("rank")
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .plan()
                .map(PreparedExecutionPlan::from)
                .expect("distinct DESC fast-path plan should build")
        },
        |limit| {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .by_ids(group7_ids.iter().copied())
                .order_by_desc("rank")
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .plan()
                .map(PreparedExecutionPlan::from)
                .expect("distinct DESC fallback plan should build")
        },
        "distinct DESC fast-path and fallback",
    );
}

#[test]
fn load_distinct_mixed_direction_secondary_shape_rejects_pushdown_and_matches_fallback() {
    setup_pagination_test();

    let rows = distinct_secondary_pushdown_rows(39_700);
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
                    .map(PreparedExecutionPlan::from)
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
    }

    assert_pushdown_pagination_parity_matrix(
        &load,
        &[1_u32, 2, 3],
        |limit| {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .order_by_desc("rank")
                .order_by("id")
                .distinct()
                .limit(limit)
                .plan()
                .map(PreparedExecutionPlan::from)
                .expect("distinct mixed-direction index-shape plan should build")
        },
        |limit| {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .by_ids(group7_ids.iter().copied())
                .order_by_desc("rank")
                .order_by("id")
                .distinct()
                .limit(limit)
                .plan()
                .map(PreparedExecutionPlan::from)
                .expect("distinct mixed-direction fallback plan should build")
        },
        "distinct mixed-direction index-shape and fallback",
    );
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
                    .map(PreparedExecutionPlan::from)
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
                    .map(PreparedExecutionPlan::from)
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
    }

    assert_simple_pagination_parity_matrix(
        &load,
        &[1_u32, 2, 3],
        |limit| {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .offset(1)
                .plan()
                .map(PreparedExecutionPlan::from)
                .expect("distinct DESC PK fast-path plan should build")
        },
        |limit| {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_ids(keys.into_iter().map(Ulid::from_u128))
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .offset(1)
                .plan()
                .map(PreparedExecutionPlan::from)
                .expect("distinct DESC PK fallback plan should build")
        },
        "distinct DESC PK fast-path and fallback",
    );
}

#[test]
fn load_distinct_desc_index_range_limit_pushdown_resume_matrix_and_fallback_parity() {
    setup_pagination_test();

    let rows = indexed_metric_range_rows(39_600);
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
    }

    assert_indexed_metric_resume_and_fallback_parity_matrix(
        &load,
        &[1_u32, 2, 3],
        &expected_ids,
        |limit| build_distinct_desc_index_range_plan(limit, 0),
        |limit| {
            Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
                .by_ids(expected_ids.iter().copied())
                .order_by_desc("tag")
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .plan()
                .map(PreparedExecutionPlan::from)
                .expect("distinct DESC index-range fallback plan should build")
        },
        "distinct DESC index-range",
    );
}

#[test]
fn load_distinct_offset_fast_path_and_fallback_match_ids_and_boundaries() {
    setup_pagination_test();

    let secondary_rows = distinct_secondary_pushdown_rows(42_300);
    seed_pushdown_rows(&secondary_rows);
    let secondary_predicate = pushdown_group_predicate(7);
    let secondary_group_ids = pushdown_group_ids(&secondary_rows, 7);

    let index_rows = indexed_metric_range_rows(42_400);
    seed_indexed_metrics_rows(&index_rows);

    let load_secondary = LoadExecutor::<PushdownParityEntity>::new(DB, true);
    let load_index_range = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    for (case_name, direction) in [("asc", OrderDirection::Asc), ("desc", OrderDirection::Desc)] {
        let descending = matches!(direction, OrderDirection::Desc);
        let secondary_expected_ids = ordered_pushdown_ids_with_rank_and_id_direction(
            &secondary_rows,
            7,
            descending,
            descending,
        )
        .into_iter()
        .skip(1)
        .collect::<Vec<_>>();
        assert_distinct_secondary_offset_parity_case(
            &load_secondary,
            secondary_predicate.clone(),
            &secondary_group_ids,
            &secondary_expected_ids,
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

#[test]
fn load_union_child_order_permutation_preserves_rows_and_continuation_boundaries() {
    setup_pagination_test();
    seed_simple_rows(&[
        37_901, 37_902, 37_903, 37_904, 37_905, 37_906, 37_907, 37_908,
    ]);

    let id1 = Ulid::from_u128(37_901);
    let id2 = Ulid::from_u128(37_902);
    let id3 = Ulid::from_u128(37_903);
    let id4 = Ulid::from_u128(37_904);
    let id5 = Ulid::from_u128(37_905);
    let id6 = Ulid::from_u128(37_906);
    let id7 = Ulid::from_u128(37_907);
    let id8 = Ulid::from_u128(37_908);

    let build_union_abc = || {
        build_simple_access_ordered_page_plan(
            AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4])),
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6])),
                AccessPlan::path(AccessPath::ByKeys(vec![id6, id7, id8])),
            ]),
            true,
            2,
        )
    };
    let build_union_cab = || {
        build_simple_access_ordered_page_plan(
            AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id6, id7, id8])),
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4])),
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6])),
            ]),
            true,
            2,
        )
    };

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let (ids_abc, boundaries_abc) =
        collect_simple_pages_from_executable_plan(&load, build_union_abc, 12);
    let (ids_cab, boundaries_cab) =
        collect_simple_pages_from_executable_plan(&load, build_union_cab, 12);

    assert_eq!(
        ids_abc, ids_cab,
        "union child-plan order permutation must not change paged row sequence",
    );
    assert_eq!(
        boundaries_abc, boundaries_cab,
        "union child-plan order permutation must not change continuation boundaries",
    );
}

#[test]
fn load_intersection_child_order_permutation_preserves_rows_and_continuation_boundaries() {
    setup_pagination_test();
    seed_simple_rows(&[
        38_001, 38_002, 38_003, 38_004, 38_005, 38_006, 38_007, 38_008,
    ]);

    let id1 = Ulid::from_u128(38_001);
    let id2 = Ulid::from_u128(38_002);
    let id3 = Ulid::from_u128(38_003);
    let id4 = Ulid::from_u128(38_004);
    let id5 = Ulid::from_u128(38_005);
    let id6 = Ulid::from_u128(38_006);
    let id7 = Ulid::from_u128(38_007);
    let id8 = Ulid::from_u128(38_008);

    let build_intersection_abc = || {
        build_simple_access_ordered_page_plan(
            AccessPlan::Intersection(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4, id5, id6])),
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6, id7])),
                AccessPlan::path(AccessPath::ByKeys(vec![id2, id4, id5, id6, id8])),
            ]),
            true,
            1,
        )
    };
    let build_intersection_bca = || {
        build_simple_access_ordered_page_plan(
            AccessPlan::Intersection(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6, id7])),
                AccessPlan::path(AccessPath::ByKeys(vec![id2, id4, id5, id6, id8])),
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4, id5, id6])),
            ]),
            true,
            1,
        )
    };

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let (ids_abc, boundaries_abc) =
        collect_simple_pages_from_executable_plan(&load, build_intersection_abc, 12);
    let (ids_bca, boundaries_bca) =
        collect_simple_pages_from_executable_plan(&load, build_intersection_bca, 12);

    assert_eq!(
        ids_abc, ids_bca,
        "intersection child-plan order permutation must not change paged row sequence",
    );
    assert_eq!(
        boundaries_abc, boundaries_bca,
        "intersection child-plan order permutation must not change continuation boundaries",
    );
}

#[test]
fn load_union_child_order_permutation_preserves_rows_and_boundaries_under_mixed_direction() {
    setup_pagination_test();

    let rows = [
        (40_001, 7, 30, "g7-r30-a"),
        (40_002, 7, 20, "g7-r20-a"),
        (40_003, 7, 20, "g7-r20-b"),
        (40_004, 7, 10, "g7-r10"),
        (40_005, 7, 30, "g7-r30-b"),
        (40_006, 7, 40, "g7-r40"),
        (40_007, 7, 20, "g7-r20-c"),
        (40_008, 8, 15, "g8-r15"),
    ];
    seed_pushdown_rows(&rows);

    let id1 = Ulid::from_u128(40_001);
    let id2 = Ulid::from_u128(40_002);
    let id3 = Ulid::from_u128(40_003);
    let id4 = Ulid::from_u128(40_004);
    let id5 = Ulid::from_u128(40_005);
    let id6 = Ulid::from_u128(40_006);
    let id7 = Ulid::from_u128(40_007);
    let id8 = Ulid::from_u128(40_008);

    let build_union_abc = || {
        build_pushdown_access_ordered_page_plan(
            AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4])),
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id5, id6])),
                AccessPlan::path(AccessPath::ByKeys(vec![id2, id7, id8])),
            ]),
            OrderDirection::Desc,
            OrderDirection::Asc,
            2,
        )
    };
    let build_union_cab = || {
        build_pushdown_access_ordered_page_plan(
            AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id2, id7, id8])),
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4])),
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id5, id6])),
            ]),
            OrderDirection::Desc,
            OrderDirection::Asc,
            2,
        )
    };

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let (ids_abc, boundaries_abc) =
        collect_pushdown_pages_from_executable_plan(&load, build_union_abc, 12);
    let (ids_cab, boundaries_cab) =
        collect_pushdown_pages_from_executable_plan(&load, build_union_cab, 12);

    assert_eq!(
        ids_abc, ids_cab,
        "mixed-direction union child-plan permutation must not change paged row sequence",
    );
    assert_eq!(
        boundaries_abc, boundaries_cab,
        "mixed-direction union child-plan permutation must not change continuation boundaries",
    );
}

#[test]
fn load_intersection_child_order_permutation_preserves_rows_and_boundaries_under_mixed_direction() {
    setup_pagination_test();

    let rows = [
        (40_101, 7, 50, "g7-r50"),
        (40_102, 7, 40, "g7-r40"),
        (40_103, 7, 30, "g7-r30-a"),
        (40_104, 7, 30, "g7-r30-b"),
        (40_105, 7, 20, "g7-r20-a"),
        (40_106, 7, 20, "g7-r20-b"),
        (40_107, 7, 10, "g7-r10"),
        (40_108, 8, 5, "g8-r5"),
    ];
    seed_pushdown_rows(&rows);

    let id1 = Ulid::from_u128(40_101);
    let id2 = Ulid::from_u128(40_102);
    let id3 = Ulid::from_u128(40_103);
    let id4 = Ulid::from_u128(40_104);
    let id5 = Ulid::from_u128(40_105);
    let id6 = Ulid::from_u128(40_106);
    let id7 = Ulid::from_u128(40_107);
    let id8 = Ulid::from_u128(40_108);

    let build_intersection_abc = || {
        build_pushdown_access_ordered_page_plan(
            AccessPlan::Intersection(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4, id5, id6])),
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6, id7])),
                AccessPlan::path(AccessPath::ByKeys(vec![id2, id3, id4, id5, id6, id8])),
            ]),
            OrderDirection::Asc,
            OrderDirection::Desc,
            1,
        )
    };
    let build_intersection_bca = || {
        build_pushdown_access_ordered_page_plan(
            AccessPlan::Intersection(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6, id7])),
                AccessPlan::path(AccessPath::ByKeys(vec![id2, id3, id4, id5, id6, id8])),
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4, id5, id6])),
            ]),
            OrderDirection::Asc,
            OrderDirection::Desc,
            1,
        )
    };

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let (ids_abc, boundaries_abc) =
        collect_pushdown_pages_from_executable_plan(&load, build_intersection_abc, 12);
    let (ids_bca, boundaries_bca) =
        collect_pushdown_pages_from_executable_plan(&load, build_intersection_bca, 12);

    assert_eq!(
        ids_abc, ids_bca,
        "mixed-direction intersection child-plan permutation must not change paged row sequence",
    );
    assert_eq!(
        boundaries_abc, boundaries_bca,
        "mixed-direction intersection child-plan permutation must not change continuation boundaries",
    );
}

#[test]
fn load_secondary_order_top_n_seek_trace_optimization_is_explicit() {
    setup_pagination_test();

    let rows = [
        (42_201, 40, "code-40"),
        (42_202, 10, "code-10"),
        (42_203, 30, "code-30"),
        (42_204, 20, "code-20"),
        (42_205, 50, "code-50"),
    ];
    seed_unique_index_range_rows(&rows);

    let mut logical_plan = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: UNIQUE_INDEX_RANGE_INDEX_MODELS[0],
            values: vec![Value::Uint(20)],
        },
        MissingRowPolicy::Ignore,
    );
    logical_plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("code".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    logical_plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, true);
    let plan = PreparedExecutionPlan::<UniqueIndexRangeEntity>::new(logical_plan);

    let (_page, trace) = load
        .execute_paged_with_cursor_traced(plan, None)
        .expect("unique secondary top-n trace execution should succeed");
    let trace = trace.expect("debug trace should be present");
    assert_eq!(
        trace.optimization(),
        Some(ExecutionOptimization::SecondaryOrderTopNSeek),
        "secondary ordered limit windows should report explicit top-n-assisted secondary optimization",
    );
}

#[test]
fn load_secondary_order_trace_reports_non_top_n_variant_without_page_limit() {
    setup_pagination_test();

    let rows = [
        (42_301, 10, "code-10"),
        (42_302, 20, "code-20"),
        (42_303, 30, "code-30"),
    ];
    seed_unique_index_range_rows(&rows);

    let mut logical_plan = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: UNIQUE_INDEX_RANGE_INDEX_MODELS[0],
            values: vec![Value::Uint(20)],
        },
        MissingRowPolicy::Ignore,
    );
    logical_plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("code".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });

    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, true);
    let plan = PreparedExecutionPlan::<UniqueIndexRangeEntity>::new(logical_plan);

    let (page, trace) = load
        .execute_paged_with_cursor_traced(plan, None)
        .expect("unique secondary non-top-n trace execution should succeed");
    let trace = trace.expect("debug trace should be present");
    assert_eq!(
        trace.optimization(),
        Some(ExecutionOptimization::SecondaryOrderPushdown),
        "unpaged secondary ordered shapes should report non-top-n secondary optimization labels",
    );
    assert_eq!(
        page.items.len(),
        1,
        "unpaged secondary ordered execution should return the prefix-matching seeded row",
    );
}

#[test]
fn load_mixed_direction_fallback_matches_uniform_fast_path_when_rank_is_unique() {
    setup_pagination_test();

    let rows = [
        (41_901, 8, 5, "g8-r5"),
        (41_902, 7, 10, "g7-r10"),
        (41_903, 7, 20, "g7-r20"),
        (41_904, 7, 30, "g7-r30"),
        (41_905, 9, 40, "g9-r40"),
    ];
    seed_pushdown_rows(&rows);
    let predicate = pushdown_group_predicate(7);
    let expected_ids = vec![
        Ulid::from_u128(41_902),
        Ulid::from_u128(41_903),
        Ulid::from_u128(41_904),
    ];
    let build_mixed_plan = || build_rank_unique_pushdown_plan(predicate.clone(), true, 2);
    let build_uniform_plan = || build_rank_unique_pushdown_plan(predicate.clone(), false, 2);

    // Phase 1: query-surface explain still lacks model context, so both shapes
    // report the same unresolved order-pushdown status there.
    assert_rank_unique_order_pushdown_explain_missing_model_context(
        predicate.clone(),
        true,
        "mixed-direction",
    );
    assert_rank_unique_order_pushdown_explain_missing_model_context(
        predicate.clone(),
        false,
        "uniform-direction",
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);

    // Phase 2: both shapes still execute through the materialized lane in the
    // current runtime, so traces should stay fallback-only.
    let (_seed_mixed, mixed_trace) = load
        .execute_paged_with_cursor_traced(build_mixed_plan(), None)
        .expect("mixed-direction seed page should execute");
    let mixed_trace = mixed_trace.expect("debug trace should be present");
    assert_eq!(
        mixed_trace.optimization(),
        None,
        "mixed-direction execution should remain fallback-only",
    );

    let (_seed_uniform, uniform_trace) = load
        .execute_paged_with_cursor_traced(build_uniform_plan(), None)
        .expect("uniform-direction seed page should execute");
    let uniform_trace = uniform_trace.expect("debug trace should be present");
    assert_eq!(
        uniform_trace.optimization(),
        Some(ExecutionOptimization::SecondaryOrderTopNSeek),
        "uniform-direction unique-suffix execution should report the bounded secondary top-N route",
    );

    // Phase 3: row order, emitted boundaries, and token resumes must all stay
    // aligned across both paths.
    let (mixed_ids, mixed_boundaries) = collect_pushdown_pages_and_assert_token_resumes(
        &load,
        build_mixed_plan,
        &expected_ids,
        "mixed-direction fallback resumes",
    );
    let (uniform_ids, uniform_boundaries) = collect_pushdown_pages_and_assert_token_resumes(
        &load,
        build_uniform_plan,
        &expected_ids,
        "uniform-direction pushdown resumes",
    );
    assert_eq!(
        mixed_ids, uniform_ids,
        "mixed-direction fallback and uniform pushdown should return identical ids",
    );
    assert_eq!(
        mixed_boundaries, uniform_boundaries,
        "mixed-direction fallback and uniform pushdown should emit identical boundaries",
    );
}

#[test]
fn load_mixed_direction_resume_matrix_is_boundary_complete() {
    setup_pagination_test();

    let rows = [
        (39_101, 7, 10, "g7-r10"),
        (39_102, 7, 20, "g7-r20-a"),
        (39_103, 7, 20, "g7-r20-b"),
        (39_104, 7, 30, "g7-r30-a"),
        (39_105, 7, 30, "g7-r30-b"),
        (39_106, 7, 40, "g7-r40"),
        (39_107, 8, 20, "g8-r20"),
        (39_108, 8, 30, "g8-r30"),
    ];
    seed_pushdown_rows(&rows);
    let row_lookup = rows
        .iter()
        .map(|(id, group, rank, _)| (Ulid::from_u128(*id), (*id, *group, *rank)))
        .collect::<BTreeMap<_, _>>();

    let id1 = Ulid::from_u128(39_101);
    let id2 = Ulid::from_u128(39_102);
    let id3 = Ulid::from_u128(39_103);
    let id4 = Ulid::from_u128(39_104);
    let id5 = Ulid::from_u128(39_105);
    let id6 = Ulid::from_u128(39_106);
    let id7 = Ulid::from_u128(39_107);
    let id8 = Ulid::from_u128(39_108);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    for case in [
        MixedDirectionResumeCase {
            case_name: "rank_desc_id_asc",
            filter_group: Some(7),
            group_desc: None,
            rank_desc: true,
            id_desc: false,
            expected_ids: vec![id6, id4, id5, id2, id3, id1],
        },
        MixedDirectionResumeCase {
            case_name: "rank_asc_id_desc",
            filter_group: Some(7),
            group_desc: None,
            rank_desc: false,
            id_desc: true,
            expected_ids: vec![id1, id3, id2, id5, id4, id6],
        },
        MixedDirectionResumeCase {
            case_name: "rank_desc_id_desc",
            filter_group: Some(7),
            group_desc: None,
            rank_desc: true,
            id_desc: true,
            expected_ids: vec![id6, id5, id4, id3, id2, id1],
        },
        MixedDirectionResumeCase {
            case_name: "rank_asc_id_asc",
            filter_group: Some(7),
            group_desc: None,
            rank_desc: false,
            id_desc: false,
            expected_ids: vec![id1, id2, id3, id4, id5, id6],
        },
        MixedDirectionResumeCase {
            case_name: "group_asc_rank_desc_id_asc",
            filter_group: None,
            group_desc: Some(false),
            rank_desc: true,
            id_desc: false,
            expected_ids: vec![id6, id4, id5, id2, id3, id1, id8, id7],
        },
        MixedDirectionResumeCase {
            case_name: "group_desc_rank_asc_id_desc",
            filter_group: None,
            group_desc: Some(true),
            rank_desc: false,
            id_desc: true,
            expected_ids: vec![id7, id8, id1, id3, id2, id5, id4, id6],
        },
    ] {
        assert_mixed_direction_resume_case(&load, &row_lookup, &case);
    }
}

#[test]
fn load_union_child_order_permutation_matrix_preserves_rows_and_boundaries_under_mixed_direction() {
    setup_pagination_test();

    let rows = [
        (41_001, 7, 60, "g7-r60"),
        (41_002, 7, 50, "g7-r50-a"),
        (41_003, 7, 50, "g7-r50-b"),
        (41_004, 7, 40, "g7-r40"),
        (41_005, 7, 30, "g7-r30"),
        (41_006, 7, 20, "g7-r20-a"),
        (41_007, 7, 20, "g7-r20-b"),
        (41_008, 8, 70, "g8-r70"),
    ];
    seed_pushdown_rows(&rows);

    let id1 = Ulid::from_u128(41_001);
    let id2 = Ulid::from_u128(41_002);
    let id3 = Ulid::from_u128(41_003);
    let id4 = Ulid::from_u128(41_004);
    let id5 = Ulid::from_u128(41_005);
    let id6 = Ulid::from_u128(41_006);
    let id7 = Ulid::from_u128(41_007);
    let id8 = Ulid::from_u128(41_008);

    for (case_name, rank_direction, id_direction, limit) in [
        (
            "rank_desc_id_asc_limit1",
            OrderDirection::Desc,
            OrderDirection::Asc,
            1,
        ),
        (
            "rank_desc_id_asc_limit3",
            OrderDirection::Desc,
            OrderDirection::Asc,
            3,
        ),
        (
            "rank_asc_id_desc_limit2",
            OrderDirection::Asc,
            OrderDirection::Desc,
            2,
        ),
    ] {
        let build_union_abc = || {
            build_pushdown_access_ordered_page_plan(
                AccessPlan::Union(vec![
                    AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id4, id6])),
                    AccessPlan::path(AccessPath::ByKeys(vec![id3, id5, id6, id7])),
                    AccessPlan::path(AccessPath::ByKeys(vec![id2, id3, id8])),
                ]),
                rank_direction,
                id_direction,
                limit,
            )
        };
        let build_union_cab = || {
            build_pushdown_access_ordered_page_plan(
                AccessPlan::Union(vec![
                    AccessPlan::path(AccessPath::ByKeys(vec![id2, id3, id8])),
                    AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id4, id6])),
                    AccessPlan::path(AccessPath::ByKeys(vec![id3, id5, id6, id7])),
                ]),
                rank_direction,
                id_direction,
                limit,
            )
        };

        assert_pushdown_access_permutation_case(build_union_abc, build_union_cab, case_name);
    }
}

#[test]
fn load_intersection_child_order_permutation_matrix_preserves_rows_and_boundaries_under_mixed_direction()
 {
    setup_pagination_test();

    let rows = [
        (41_101, 7, 70, "g7-r70"),
        (41_102, 7, 60, "g7-r60"),
        (41_103, 7, 50, "g7-r50-a"),
        (41_104, 7, 50, "g7-r50-b"),
        (41_105, 7, 40, "g7-r40-a"),
        (41_106, 7, 40, "g7-r40-b"),
        (41_107, 7, 30, "g7-r30"),
        (41_108, 7, 20, "g7-r20"),
        (41_109, 8, 10, "g8-r10"),
        (41_110, 8, 5, "g8-r5"),
    ];
    seed_pushdown_rows(&rows);

    let id1 = Ulid::from_u128(41_101);
    let id2 = Ulid::from_u128(41_102);
    let id3 = Ulid::from_u128(41_103);
    let id4 = Ulid::from_u128(41_104);
    let id5 = Ulid::from_u128(41_105);
    let id6 = Ulid::from_u128(41_106);
    let id7 = Ulid::from_u128(41_107);
    let id8 = Ulid::from_u128(41_108);
    let id9 = Ulid::from_u128(41_109);
    let id10 = Ulid::from_u128(41_110);

    for (case_name, rank_direction, id_direction, limit) in [
        (
            "rank_desc_id_asc_limit1",
            OrderDirection::Desc,
            OrderDirection::Asc,
            1,
        ),
        (
            "rank_asc_id_desc_limit2",
            OrderDirection::Asc,
            OrderDirection::Desc,
            2,
        ),
        (
            "rank_asc_id_desc_limit3",
            OrderDirection::Asc,
            OrderDirection::Desc,
            3,
        ),
    ] {
        let build_intersection_abc = || {
            build_pushdown_access_ordered_page_plan(
                AccessPlan::Intersection(vec![
                    AccessPlan::path(AccessPath::ByKeys(vec![
                        id1, id2, id3, id4, id5, id6, id7, id8,
                    ])),
                    AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6, id7, id9])),
                    AccessPlan::path(AccessPath::ByKeys(vec![id2, id3, id4, id5, id6, id7, id10])),
                ]),
                rank_direction,
                id_direction,
                limit,
            )
        };
        let build_intersection_bca = || {
            build_pushdown_access_ordered_page_plan(
                AccessPlan::Intersection(vec![
                    AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6, id7, id9])),
                    AccessPlan::path(AccessPath::ByKeys(vec![id2, id3, id4, id5, id6, id7, id10])),
                    AccessPlan::path(AccessPath::ByKeys(vec![
                        id1, id2, id3, id4, id5, id6, id7, id8,
                    ])),
                ]),
                rank_direction,
                id_direction,
                limit,
            )
        };

        assert_pushdown_access_permutation_case(
            build_intersection_abc,
            build_intersection_bca,
            case_name,
        );
    }
}

#[test]
fn load_index_desc_order_with_ties_matches_for_index_and_by_ids_paths() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(14_000);
    seed_pushdown_rows(&rows);
    let group7_ids = pushdown_group_ids(&rows, 7);

    let predicate = pushdown_group_predicate(7);
    let explain = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by_desc("rank")
        .explain()
        .expect("desc explain should build");
    assert!(
        matches!(
            explain.order_pushdown(),
            crate::db::query::explain::ExplainOrderPushdown::MissingModelContext
        ),
        "query-layer explain should not evaluate secondary pushdown eligibility"
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let index_path_page1_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("index-path desc page1 plan should build");
    let index_path_page1 = load
        .execute_paged_with_cursor(index_path_page1_plan, None)
        .expect("index-path desc page1 should execute");

    let by_ids_page1_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .by_ids(group7_ids.iter().copied())
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("by-ids desc page1 plan should build");
    let by_ids_page1 = load
        .execute_paged_with_cursor(by_ids_page1_plan, None)
        .expect("by-ids desc page1 should execute");

    let index_path_page1_ids: Vec<Ulid> = ids_from_items(&index_path_page1.items);
    let by_ids_page1_ids: Vec<Ulid> = ids_from_items(&by_ids_page1.items);
    assert_eq!(
        index_path_page1_ids, by_ids_page1_ids,
        "descending page1 should match across index-prefix and by-ids paths"
    );

    let shared_boundary = index_path_page1
        .next_cursor
        .as_ref()
        .expect("index-path desc page1 should emit cursor")
        .as_scalar()
        .expect("index-path desc page1 should stay scalar")
        .boundary()
        .clone();
    let index_path_page2_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("index-path desc page2 plan should build");
    let index_path_page2 = load
        .execute_paged_with_cursor(index_path_page2_plan, Some(shared_boundary.clone()))
        .expect("index-path desc page2 should execute");

    let by_ids_page2_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .by_ids(group7_ids.iter().copied())
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("by-ids desc page2 plan should build");
    let by_ids_page2 = load
        .execute_paged_with_cursor(by_ids_page2_plan, Some(shared_boundary))
        .expect("by-ids desc page2 should execute");

    let index_path_page2_ids: Vec<Ulid> = ids_from_items(&index_path_page2.items);
    let by_ids_page2_ids: Vec<Ulid> = ids_from_items(&by_ids_page2.items);
    assert_eq!(
        index_path_page2_ids, by_ids_page2_ids,
        "descending page2 should match across index-prefix and by-ids paths"
    );
}

#[test]
fn load_index_prefix_window_cursor_past_end_returns_empty_page() {
    setup_pagination_test();

    let rows = pushdown_rows_window(15_000);
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let page1_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("prefix window page1 plan should build");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, None)
        .expect("prefix window page1 should execute");

    let page1_cursor = page1
        .next_cursor
        .as_ref()
        .expect("prefix window page1 should emit continuation cursor");
    let page2_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("prefix window page2 plan should build");
    let page2_boundary = page2_plan
        .prepare_cursor(Some(
            page1_cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("prefix window page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("prefix window page2 should execute");
    assert_eq!(page2.items.len(), 1, "page2 should return final row only");
    assert!(
        page2.next_cursor.is_none(),
        "final prefix window page should not emit continuation cursor"
    );

    let terminal_entity = page2.items[0].entity_ref();
    assert_resume_from_terminal_entity_exhausts_range(
        terminal_entity,
        "cursor boundary at final prefix row should yield an empty continuation page",
    );
}

#[test]
fn load_single_field_range_pushdown_matches_by_ids_fallback() {
    setup_pagination_test();

    let rows = [
        (18_001, 30, "t30"),
        (18_002, 10, "t10-a"),
        (18_003, 10, "t10-b"),
        (18_004, 20, "t20"),
        (18_005, 40, "t40"),
        (18_006, 5, "t5"),
    ];
    seed_indexed_metrics_rows(&rows);

    let predicate = tag_range_predicate(10, 30);
    let explain = Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("tag")
        .explain()
        .expect("single-field range explain should build");
    assert!(
        explain_contains_index_range(explain.access(), INDEXED_METRICS_INDEX_MODELS[0].name(), 0),
        "single-field range should plan an IndexRange access path"
    );

    let fallback_ids = indexed_metrics_ids_in_tag_range(&rows, 10, 30);
    assert_pushdown_parity(
        || {
            Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .order_by("tag")
        },
        fallback_ids,
        |query| query.order_by("tag"),
    );
}

#[test]
fn load_composite_prefix_range_pushdown_matches_by_ids_fallback() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(19_000);
    seed_pushdown_rows(&rows);

    let predicate = group_rank_range_predicate(7, 10, 30);
    let explain = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("rank")
        .explain()
        .expect("composite range explain should build");
    assert!(
        explain_contains_index_range(explain.access(), PUSHDOWN_PARITY_INDEX_MODELS[0].name(), 1),
        "composite prefix+range should plan an IndexRange access path"
    );

    let fallback_ids = pushdown_ids_in_group_rank_range(&rows, 7, 10, 30);
    assert_pushdown_parity(
        || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .order_by("rank")
        },
        fallback_ids,
        |query| query.order_by("rank"),
    );
}

#[test]
fn load_single_field_range_full_asc_reversed_equals_full_desc() {
    setup_pagination_test();

    let rows = [
        (20_101, 10, "t10"),
        (20_102, 20, "t20"),
        (20_103, 30, "t30"),
        (20_104, 40, "t40"),
        (20_105, 50, "t50"),
    ];
    seed_indexed_metrics_rows(&rows);

    let predicate = tag_range_predicate(10, 50);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);

    let explain = Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("tag")
        .explain()
        .expect("single-field asc explain should build");
    assert!(
        explain_contains_index_range(explain.access(), INDEXED_METRICS_INDEX_MODELS[0].name(), 0),
        "single-field asc query should plan an IndexRange access path"
    );

    let asc = load
        .execute(
            Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .order_by("tag")
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("single-field asc plan should build"),
        )
        .expect("single-field asc execution should succeed");
    let desc = load
        .execute(
            Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate)
                .order_by_desc("tag")
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("single-field desc plan should build"),
        )
        .expect("single-field desc execution should succeed");

    let mut asc_ids = ids_from_items(&asc);
    asc_ids.reverse();

    assert_eq!(
        asc_ids,
        ids_from_items(&desc),
        "full DESC result stream should match reversed full ASC result stream"
    );
}

#[test]
fn load_composite_range_full_asc_reversed_equals_full_desc() {
    setup_pagination_test();

    let rows = [
        (20_201, 7, 10, "g7-r10"),
        (20_202, 7, 20, "g7-r20"),
        (20_203, 7, 30, "g7-r30"),
        (20_204, 7, 40, "g7-r40"),
        (20_205, 7, 50, "g7-r50"),
        (20_206, 8, 30, "g8-r30"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = group_rank_range_predicate(7, 10, 60);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let explain = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("rank")
        .explain()
        .expect("composite asc explain should build");
    assert!(
        explain_contains_index_range(explain.access(), PUSHDOWN_PARITY_INDEX_MODELS[0].name(), 1),
        "composite asc query should plan an IndexRange access path"
    );

    let asc = load
        .execute(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .order_by("rank")
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("composite asc plan should build"),
        )
        .expect("composite asc execution should succeed");
    let desc = load
        .execute(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate)
                .order_by_desc("rank")
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("composite desc plan should build"),
        )
        .expect("composite desc execution should succeed");

    let mut asc_ids = ids_from_items(&asc);
    asc_ids.reverse();

    assert_eq!(
        asc_ids,
        ids_from_items(&desc),
        "full DESC composite stream should match reversed full ASC stream"
    );
}

#[test]
fn load_unique_index_range_full_asc_reversed_equals_full_desc() {
    setup_pagination_test();

    let rows = [
        (20_301, 10, "c10"),
        (20_302, 20, "c20"),
        (20_303, 30, "c30"),
        (20_304, 40, "c40"),
        (20_305, 50, "c50"),
        (20_306, 70, "c70"),
    ];
    seed_unique_index_range_rows(&rows);

    let predicate = unique_code_range_predicate(10, 60);
    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);

    let explain = Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("code")
        .explain()
        .expect("unique asc explain should build");
    assert!(
        explain_contains_index_range(
            explain.access(),
            UNIQUE_INDEX_RANGE_INDEX_MODELS[0].name(),
            0
        ),
        "unique asc query should plan an IndexRange access path"
    );

    let asc = load
        .execute(
            Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .order_by("code")
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("unique asc plan should build"),
        )
        .expect("unique asc execution should succeed");
    let desc = load
        .execute(
            Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate)
                .order_by_desc("code")
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("unique desc plan should build"),
        )
        .expect("unique desc execution should succeed");

    let mut asc_ids = ids_from_items(&asc);
    asc_ids.reverse();

    assert_eq!(
        asc_ids,
        ids_from_items(&desc),
        "full DESC unique stream should match reversed full ASC stream"
    );
}

#[test]
fn load_single_field_range_limit_matrix_matches_unbounded() {
    setup_pagination_test();

    let rows = [
        (31_001, 30, "t30"),
        (31_002, 10, "t10-a"),
        (31_003, 10, "t10-b"),
        (31_004, 20, "t20"),
        (31_005, 25, "t25"),
        (31_006, 40, "t40"),
        (31_007, 5, "t5"),
    ];
    seed_indexed_metrics_rows(&rows);

    let predicate = tag_range_predicate(10, 30);
    let explain = Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("tag")
        .limit(2)
        .explain()
        .expect("single-field limit matrix explain should build");
    assert!(
        explain_contains_index_range(explain.access(), INDEXED_METRICS_INDEX_MODELS[0].name(), 0),
        "single-field limit matrix should plan an IndexRange access path"
    );

    assert_limit_matrix(
        || {
            Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .order_by("tag")
        },
        &[0_u32, 1_u32, 2_u32, 4_u32, 16_u32],
        16,
    );
}

#[test]
fn load_composite_range_limit_matrix_matches_unbounded() {
    setup_pagination_test();

    let rows = [
        (32_001, 7, 10, "g7-r10-a"),
        (32_002, 7, 10, "g7-r10-b"),
        (32_003, 7, 20, "g7-r20-a"),
        (32_004, 7, 20, "g7-r20-b"),
        (32_005, 7, 25, "g7-r25"),
        (32_006, 7, 30, "g7-r30"),
        (32_007, 7, 35, "g7-r35"),
        (32_008, 8, 10, "g8-r10"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = group_rank_range_predicate(7, 10, 40);
    let explain = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .explain()
        .expect("composite limit matrix explain should build");
    assert!(
        explain_contains_index_range(explain.access(), PUSHDOWN_PARITY_INDEX_MODELS[0].name(), 1),
        "composite limit matrix should plan an IndexRange access path"
    );

    assert_limit_matrix(
        || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(predicate.clone())
                .order_by("rank")
        },
        &[0_u32, 1_u32, 2_u32, 3_u32, 16_u32],
        20,
    );
}

#[test]
fn load_single_field_range_limit_exact_size_returns_single_page_without_cursor() {
    setup_pagination_test();

    let rows = [
        (33_001, 10, "t10-a"),
        (33_002, 10, "t10-b"),
        (33_003, 20, "t20"),
        (33_004, 25, "t25"),
        (33_005, 40, "t40"),
    ];
    seed_indexed_metrics_rows(&rows);

    let predicate = tag_range_predicate(10, 30);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);
    let page_plan = Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("tag")
        .limit(4)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("single-field exact-size page plan should build");
    let planned_cursor = page_plan
        .prepare_cursor(None)
        .expect("single-field exact-size cursor should plan");
    let page = load
        .execute_paged_with_cursor(page_plan, planned_cursor)
        .expect("single-field exact-size page should execute");

    let page_ids: Vec<Ulid> = ids_from_items(&page.items);
    let expected_ids = indexed_metrics_ids_in_tag_range(&rows, 10, 30);
    assert_eq!(
        page_ids, expected_ids,
        "exact-size single-field range page should return the full bounded result set"
    );
    assert!(
        page.next_cursor.is_none(),
        "exact-size single-field range page should not emit a continuation cursor"
    );
}

#[test]
fn load_composite_range_limit_terminal_page_suppresses_cursor() {
    setup_pagination_test();

    let rows = [
        (34_001, 7, 10, "g7-r10-a"),
        (34_002, 7, 10, "g7-r10-b"),
        (34_003, 7, 20, "g7-r20-a"),
        (34_004, 7, 20, "g7-r20-b"),
        (34_005, 7, 25, "g7-r25"),
        (34_006, 7, 30, "g7-r30"),
        (34_007, 7, 35, "g7-r35"),
        (34_008, 8, 10, "g8-r10"),
    ];
    seed_pushdown_rows(&rows);

    let predicate = group_rank_range_predicate(7, 10, 40);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let mut cursor: Option<Vec<u8>> = None;
    let mut page_sizes = Vec::new();
    let mut pages = 0usize;

    loop {
        pages = pages.saturating_add(1);
        assert!(pages <= 8, "composite terminal-page test must terminate");

        let page_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(predicate.clone())
            .order_by("rank")
            .limit(3)
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("composite terminal-page plan should build");
        let planned_cursor = page_plan
            .prepare_cursor(cursor.as_deref())
            .expect("composite terminal-page cursor should plan");
        let page = load
            .execute_paged_with_cursor(page_plan, planned_cursor)
            .expect("composite terminal-page execution should succeed");

        page_sizes.push(page.items.len());

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(encode_token(
            &next_cursor,
            "continuation cursor should serialize for terminal-page resume",
        ));
    }

    assert_eq!(
        page_sizes,
        vec![3, 3, 1],
        "composite limited pagination should end with one terminal page item"
    );
}

#[test]
fn load_index_range_limit_pushdown_trace_reports_limited_access_rows_for_eligible_plan() {
    setup_pagination_test();

    let rows = [
        (35_001, 10, "t10-a"),
        (35_002, 10, "t10-b"),
        (35_003, 20, "t20"),
        (35_004, 25, "t25"),
        (35_005, 28, "t28"),
        (35_006, 40, "t40"),
    ];
    seed_indexed_metrics_rows(&rows);

    let mut logical = AccessPlannedQuery::new(
        AccessPath::index_range(
            INDEXED_METRICS_INDEX_MODELS[0],
            Vec::new(),
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    logical.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    logical.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let page_plan = PreparedExecutionPlan::<IndexedMetricsEntity>::new(logical);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    let (_page, trace) = load
        .execute_paged_with_cursor_traced(page_plan, None)
        .expect("trace limit-pushdown execution should succeed");

    let access_rows = trace.map(|trace| trace.keys_scanned());

    assert_eq!(
        access_rows,
        Some(3),
        "limit=2 index-range pushdown should scan only offset+limit+1 rows in access phase"
    );
}

#[test]
fn load_index_range_limit_pushdown_trace_reports_limited_access_rows_for_desc_eligible_plan() {
    setup_pagination_test();

    let rows = [
        (35_051, 10, "t10-a"),
        (35_052, 10, "t10-b"),
        (35_053, 20, "t20"),
        (35_054, 25, "t25"),
        (35_055, 28, "t28"),
        (35_056, 40, "t40"),
    ];
    seed_indexed_metrics_rows(&rows);

    let mut logical = AccessPlannedQuery::new(
        AccessPath::index_range(
            INDEXED_METRICS_INDEX_MODELS[0],
            Vec::new(),
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    logical.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    logical.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let page_plan = PreparedExecutionPlan::<IndexedMetricsEntity>::new(logical);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    let (_page, trace) = load
        .execute_paged_with_cursor_traced(page_plan, None)
        .expect("trace descending limit-pushdown execution should succeed");

    let access_rows = trace.map(|trace| trace.keys_scanned());

    assert_eq!(
        access_rows,
        Some(3),
        "descending limit=2 index-range pushdown should scan only offset+limit+1 rows in access phase"
    );
}

#[test]
// This migrated matrix case keeps both directions in one runtime contract.
#[expect(clippy::too_many_lines)]
fn load_index_range_limit_pushdown_continuation_replay_matches_fallback_for_asc_and_desc() {
    setup_pagination_test();

    let rows = [
        (35_081, 5, "t5-outside"),
        (35_082, 10, "t10-a"),
        (35_083, 15, "t15"),
        (35_084, 20, "t20"),
        (35_085, 25, "t25"),
        (35_086, 30, "t30"),
        (35_087, 35, "t35"),
        (35_088, 50, "t50-outside"),
    ];
    seed_indexed_metrics_rows(&rows);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    let cases = [("asc", false), ("desc", true)];

    for (case_name, descending) in cases {
        let direction = if descending {
            OrderDirection::Desc
        } else {
            OrderDirection::Asc
        };
        let build_fast_plan = || {
            let mut logical = AccessPlannedQuery::new(
                AccessPath::index_range(
                    INDEXED_METRICS_INDEX_MODELS[0],
                    Vec::new(),
                    Bound::Included(Value::Uint(10)),
                    Bound::Excluded(Value::Uint(50)),
                ),
                MissingRowPolicy::Ignore,
            );
            logical.scalar_plan_mut().order = Some(OrderSpec {
                fields: vec![
                    ("tag".to_string(), direction),
                    ("id".to_string(), direction),
                ],
            });
            logical.scalar_plan_mut().page = Some(PageSpec {
                limit: Some(2),
                offset: 0,
            });
            PreparedExecutionPlan::<IndexedMetricsEntity>::new(logical)
        };
        let build_fallback_plan = || {
            let mut logical =
                AccessPlannedQuery::new(AccessPath::FullScan, MissingRowPolicy::Ignore);
            logical.scalar_plan_mut().predicate = Some(Predicate::And(vec![
                strict_compare_predicate("tag", CompareOp::Gte, Value::Uint(10)),
                strict_compare_predicate("tag", CompareOp::Lt, Value::Uint(50)),
            ]));
            logical.scalar_plan_mut().order = Some(OrderSpec {
                fields: vec![
                    ("tag".to_string(), direction),
                    ("id".to_string(), direction),
                ],
            });
            logical.scalar_plan_mut().page = Some(PageSpec {
                limit: Some(2),
                offset: 0,
            });
            PreparedExecutionPlan::<IndexedMetricsEntity>::new(logical)
        };

        let (fast_page1, fast_trace1) = load
            .execute_paged_with_cursor_traced(build_fast_plan(), None)
            .expect("fast limit-pushdown page1 should execute");
        let fast_trace1 = fast_trace1.expect("debug trace should be present");
        let (fallback_page1, fallback_trace1) = load
            .execute_paged_with_cursor_traced(build_fallback_plan(), None)
            .expect("fallback page1 should execute");
        let fallback_trace1 = fallback_trace1.expect("debug trace should be present");
        assert_eq!(
            fast_trace1.optimization(),
            Some(ExecutionOptimization::IndexRangeLimitPushdown),
            "eligible page1 should report index-range limit pushdown for case={case_name}",
        );
        assert_eq!(
            fallback_trace1.optimization(),
            None,
            "fallback page1 should remain non-optimized for case={case_name}",
        );
        assert_eq!(
            ids_from_items(&fast_page1.items),
            ids_from_items(&fallback_page1.items),
            "limit-pushdown page1 rows should match fallback for case={case_name}",
        );
        assert_eq!(
            fast_page1.next_cursor.is_some(),
            fallback_page1.next_cursor.is_some(),
            "limit-pushdown page1 continuation presence should match fallback for case={case_name}",
        );
        let shared_boundary = scalar_boundary(
            fast_page1
                .next_cursor
                .as_ref()
                .expect("page1 should emit continuation cursor for this matrix"),
        );
        let fallback_page1_boundary = scalar_boundary(
            fallback_page1
                .next_cursor
                .as_ref()
                .expect("fallback page1 should emit continuation cursor for this matrix"),
        );
        assert_eq!(
            shared_boundary, fallback_page1_boundary,
            "page1 continuation boundary should match fallback for case={case_name}",
        );

        let (fast_page2, _fast_trace2) = load
            .execute_paged_with_cursor_traced(build_fast_plan(), Some(shared_boundary.clone()))
            .expect("fast continuation replay should execute");
        let (fallback_page2, _fallback_trace2) = load
            .execute_paged_with_cursor_traced(build_fallback_plan(), Some(shared_boundary))
            .expect("fallback continuation replay should execute");
        assert_eq!(
            ids_from_items(&fast_page2.items),
            ids_from_items(&fallback_page2.items),
            "limit-pushdown continuation replay rows should match fallback for case={case_name}",
        );
        assert_eq!(
            fast_page2.next_cursor.is_some(),
            fallback_page2.next_cursor.is_some(),
            "limit-pushdown continuation replay cursor presence should match fallback for case={case_name}",
        );
        if let (Some(fast_cursor), Some(fallback_cursor)) =
            (&fast_page2.next_cursor, &fallback_page2.next_cursor)
        {
            assert_eq!(
                scalar_boundary(fast_cursor),
                scalar_boundary(fallback_cursor),
                "limit-pushdown continuation replay boundary should match fallback for case={case_name}",
            );
        }
    }
}

#[test]
// This migrated matrix case keeps replay, decode, and cross-shape rejection together.
#[expect(clippy::too_many_lines)]
fn load_index_range_limit_pushdown_token_replay_matches_fallback_for_asc_and_desc() {
    setup_pagination_test();

    let rows = [
        (35_091, 5, "t5-outside"),
        (35_092, 10, "t10-a"),
        (35_093, 15, "t15"),
        (35_094, 20, "t20"),
        (35_095, 25, "t25"),
        (35_096, 30, "t30"),
        (35_097, 35, "t35"),
        (35_098, 50, "t50-outside"),
    ];
    seed_indexed_metrics_rows(&rows);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    let cases = [("asc", false), ("desc", true)];

    for (case_name, descending) in cases {
        let direction = if descending {
            OrderDirection::Desc
        } else {
            OrderDirection::Asc
        };
        let build_fast_plan = || {
            let mut logical = AccessPlannedQuery::new(
                AccessPath::index_range(
                    INDEXED_METRICS_INDEX_MODELS[0],
                    Vec::new(),
                    Bound::Included(Value::Uint(10)),
                    Bound::Excluded(Value::Uint(50)),
                ),
                MissingRowPolicy::Ignore,
            );
            logical.scalar_plan_mut().order = Some(OrderSpec {
                fields: vec![
                    ("tag".to_string(), direction),
                    ("id".to_string(), direction),
                ],
            });
            logical.scalar_plan_mut().page = Some(PageSpec {
                limit: Some(2),
                offset: 0,
            });
            PreparedExecutionPlan::<IndexedMetricsEntity>::new(logical)
        };
        let build_fallback_plan = || {
            let mut logical =
                AccessPlannedQuery::new(AccessPath::FullScan, MissingRowPolicy::Ignore);
            logical.scalar_plan_mut().predicate = Some(Predicate::And(vec![
                strict_compare_predicate("tag", CompareOp::Gte, Value::Uint(10)),
                strict_compare_predicate("tag", CompareOp::Lt, Value::Uint(50)),
            ]));
            logical.scalar_plan_mut().order = Some(OrderSpec {
                fields: vec![
                    ("tag".to_string(), direction),
                    ("id".to_string(), direction),
                ],
            });
            logical.scalar_plan_mut().page = Some(PageSpec {
                limit: Some(2),
                offset: 0,
            });
            PreparedExecutionPlan::<IndexedMetricsEntity>::new(logical)
        };

        let (fast_page1, _fast_trace1) = load
            .execute_paged_with_cursor_traced(build_fast_plan(), None)
            .expect("fast token replay page1 should execute");
        let (fallback_page1, _fallback_trace1) = load
            .execute_paged_with_cursor_traced(build_fallback_plan(), None)
            .expect("fallback token replay page1 should execute");
        let fast_cursor = fast_page1
            .next_cursor
            .as_ref()
            .expect("fast token replay page1 should emit continuation cursor");
        let fallback_cursor = fallback_page1
            .next_cursor
            .as_ref()
            .expect("fallback token replay page1 should emit continuation cursor");
        let fast_token = encode_token(
            fast_cursor,
            "fast token replay cursor should serialize for replay",
        );
        let fallback_token = encode_token(
            fallback_cursor,
            "fallback token replay cursor should serialize for replay",
        );
        assert_eq!(
            decode_boundary(
                fast_token.as_slice(),
                "fast token replay boundary should decode",
            ),
            scalar_boundary(fast_cursor),
            "fast token replay boundary decode should match emitted boundary for case={case_name}",
        );
        assert_eq!(
            decode_boundary(
                fallback_token.as_slice(),
                "fallback token replay boundary should decode",
            ),
            scalar_boundary(fallback_cursor),
            "fallback token replay boundary decode should match emitted boundary for case={case_name}",
        );

        let fast_page2_plan = build_fast_plan();
        let fast_page2_boundary = fast_page2_plan
            .prepare_cursor(Some(fast_token.as_slice()))
            .expect("fast token replay boundary should prepare");
        let (fast_page2, _fast_trace2) = load
            .execute_paged_with_cursor_traced(fast_page2_plan, fast_page2_boundary)
            .expect("fast token replay continuation should execute");

        let fallback_page2_plan = build_fallback_plan();
        let fallback_page2_boundary = fallback_page2_plan
            .prepare_cursor(Some(fallback_token.as_slice()))
            .expect("fallback token replay boundary should prepare");
        let (fallback_page2, _fallback_trace2) = load
            .execute_paged_with_cursor_traced(fallback_page2_plan, fallback_page2_boundary)
            .expect("fallback token replay continuation should execute");

        assert_eq!(
            ids_from_items(&fast_page2.items),
            ids_from_items(&fallback_page2.items),
            "token replay continuation rows should match fallback for case={case_name}",
        );
        assert_eq!(
            fast_page2.next_cursor.is_some(),
            fallback_page2.next_cursor.is_some(),
            "token replay continuation cursor presence should match fallback for case={case_name}",
        );
        if let (Some(fast_cursor), Some(fallback_cursor)) =
            (&fast_page2.next_cursor, &fallback_page2.next_cursor)
        {
            assert_eq!(
                scalar_boundary(fast_cursor),
                scalar_boundary(fallback_cursor),
                "token replay continuation boundary should match fallback for case={case_name}",
            );
        }

        let fallback_cross_shape_plan = build_fallback_plan();
        let fallback_cross_shape_err = fallback_cross_shape_plan
            .prepare_cursor(Some(fast_token.as_slice()))
            .expect_err("cross-shape fallback replay should reject fast token");
        assert!(
            matches!(
                fallback_cross_shape_err,
                crate::db::executor::ExecutorPlanError::Cursor(inner)
                    if matches!(
                        inner.as_ref(),
                        crate::db::cursor::CursorPlanError::ContinuationCursorSignatureMismatch { .. }
                    )
            ),
            "cross-shape fallback token replay should fail with signature mismatch for case={case_name}",
        );

        let fast_cross_shape_plan = build_fast_plan();
        let fast_cross_shape_err = fast_cross_shape_plan
            .prepare_cursor(Some(fallback_token.as_slice()))
            .expect_err("cross-shape fast replay should reject fallback token");
        assert!(
            matches!(
                fast_cross_shape_err,
                crate::db::executor::ExecutorPlanError::Cursor(inner)
                    if matches!(
                        inner.as_ref(),
                        crate::db::cursor::CursorPlanError::ContinuationCursorSignatureMismatch { .. }
                    )
            ),
            "cross-shape fast token replay should fail with signature mismatch for case={case_name}",
        );
    }
}

#[test]
fn load_index_range_limit_zero_short_circuits_access_scan_for_eligible_plan() {
    setup_pagination_test();

    let rows = [
        (35_101, 10, "t10-a"),
        (35_102, 20, "t20"),
        (35_103, 25, "t25"),
    ];
    seed_indexed_metrics_rows(&rows);

    let mut logical = AccessPlannedQuery::new(
        AccessPath::index_range(
            INDEXED_METRICS_INDEX_MODELS[0],
            Vec::new(),
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    logical.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    logical.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(0),
        offset: 0,
    });
    let page_plan = PreparedExecutionPlan::<IndexedMetricsEntity>::new(logical);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    let (page, trace) = load
        .execute_paged_with_cursor_traced(page_plan, None)
        .expect("limit=0 trace execution should succeed");

    let access_rows = trace.map(|trace| trace.keys_scanned());

    assert_eq!(
        access_rows,
        Some(0),
        "limit=0 index-range pushdown should not scan access rows"
    );
    assert!(
        page.items.is_empty(),
        "limit=0 should return an empty page for eligible index-range plans",
    );
    assert!(
        page.next_cursor.is_none(),
        "limit=0 should not emit a continuation cursor",
    );
}

#[test]
fn load_index_range_limit_zero_with_offset_short_circuits_access_scan_for_eligible_plan() {
    setup_pagination_test();

    let rows = [
        (35_201, 10, "t10-a"),
        (35_202, 20, "t20"),
        (35_203, 25, "t25"),
        (35_204, 28, "t28"),
    ];
    seed_indexed_metrics_rows(&rows);

    let mut logical = AccessPlannedQuery::new(
        AccessPath::index_range(
            INDEXED_METRICS_INDEX_MODELS[0],
            Vec::new(),
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    logical.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    logical.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(0),
        offset: 2,
    });
    let page_plan = PreparedExecutionPlan::<IndexedMetricsEntity>::new(logical);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    let (page, trace) = load
        .execute_paged_with_cursor_traced(page_plan, None)
        .expect("limit=0 with offset trace execution should succeed");

    let access_rows = trace.map(|trace| trace.keys_scanned());

    assert_eq!(
        access_rows,
        Some(0),
        "limit=0 should short-circuit access scanning even when offset is non-zero"
    );
    assert!(
        page.items.is_empty(),
        "limit=0 with offset should return an empty page for eligible index-range plans",
    );
    assert!(
        page.next_cursor.is_none(),
        "limit=0 with offset should not emit a continuation cursor",
    );
}

#[test]
fn load_index_range_limit_pushdown_with_residual_predicate_reduces_access_rows() {
    setup_pagination_test();

    let rows = [
        (35_301, 10, "keep-t10"),
        (35_302, 12, "keep-t12"),
        (35_303, 14, "drop-t14"),
        (35_304, 16, "keep-t16"),
        (35_305, 18, "keep-t18"),
        (35_306, 20, "drop-t20"),
    ];
    seed_indexed_metrics_rows(&rows);

    let label_contains_keep = Predicate::TextContainsCi {
        field: "label".to_string(),
        value: Value::Text("keep".to_string()),
    };
    let mut fast_logical = AccessPlannedQuery::new(
        AccessPath::index_range(
            INDEXED_METRICS_INDEX_MODELS[0],
            Vec::new(),
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(21)),
        ),
        MissingRowPolicy::Ignore,
    );
    fast_logical.scalar_plan_mut().predicate = Some(label_contains_keep.clone());
    fast_logical.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    fast_logical.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let fast_plan = PreparedExecutionPlan::<IndexedMetricsEntity>::new(fast_logical);

    let mut fallback_logical =
        AccessPlannedQuery::new(AccessPath::FullScan, MissingRowPolicy::Ignore);
    fallback_logical.scalar_plan_mut().predicate = Some(Predicate::And(vec![
        strict_compare_predicate("tag", CompareOp::Gte, Value::Uint(10)),
        strict_compare_predicate("tag", CompareOp::Lt, Value::Uint(21)),
        label_contains_keep,
    ]));
    fallback_logical.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    fallback_logical.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let fallback_plan = PreparedExecutionPlan::<IndexedMetricsEntity>::new(fallback_logical);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);

    let (fast_page, fast_trace) = load
        .execute_paged_with_cursor_traced(fast_plan, None)
        .expect("fast residual limit execution should succeed");
    let fast_trace = fast_trace.expect("debug trace should be present");

    let (fallback_page, fallback_trace) = load
        .execute_paged_with_cursor_traced(fallback_plan, None)
        .expect("fallback residual limit execution should succeed");
    let fallback_trace = fallback_trace.expect("debug trace should be present");

    assert_eq!(
        ids_from_items(&fast_page.items),
        ids_from_items(&fallback_page.items),
        "residual-filter index-range pushdown must preserve fallback row parity",
    );
    assert_eq!(
        fast_trace.optimization(),
        Some(ExecutionOptimization::IndexRangeLimitPushdown),
        "residual-filter fast path should remain on bounded index-range limit pushdown even when it widens to preserve continuation correctness",
    );
    assert_eq!(
        fast_page.next_cursor.is_some(),
        fallback_page.next_cursor.is_some(),
        "residual-filter fast path should preserve continuation presence parity when bounded retries are needed to prove the next page (fast={fast_trace:?}, fallback={fallback_trace:?})",
    );
}

#[test]
fn load_index_range_limit_pushdown_residual_underfill_widens_bounded_fetch() {
    setup_pagination_test();

    let rows = [
        (35_401, 10, "drop-t10"),
        (35_402, 11, "drop-t11"),
        (35_403, 12, "drop-t12"),
        (35_404, 13, "keep-t13"),
        (35_405, 14, "keep-t14"),
        (35_406, 15, "keep-t15"),
    ];
    seed_indexed_metrics_rows(&rows);

    let label_contains_keep = Predicate::TextContainsCi {
        field: "label".to_string(),
        value: Value::Text("keep".to_string()),
    };
    let mut fast_logical = AccessPlannedQuery::new(
        AccessPath::index_range(
            INDEXED_METRICS_INDEX_MODELS[0],
            Vec::new(),
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(16)),
        ),
        MissingRowPolicy::Ignore,
    );
    fast_logical.scalar_plan_mut().predicate = Some(label_contains_keep.clone());
    fast_logical.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    fast_logical.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let fast_plan = PreparedExecutionPlan::<IndexedMetricsEntity>::new(fast_logical);

    let mut fallback_logical =
        AccessPlannedQuery::new(AccessPath::FullScan, MissingRowPolicy::Ignore);
    fallback_logical.scalar_plan_mut().predicate = Some(Predicate::And(vec![
        strict_compare_predicate("tag", CompareOp::Gte, Value::Uint(10)),
        strict_compare_predicate("tag", CompareOp::Lt, Value::Uint(16)),
        label_contains_keep,
    ]));
    fallback_logical.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    fallback_logical.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let fallback_plan = PreparedExecutionPlan::<IndexedMetricsEntity>::new(fallback_logical);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);

    let (fast_page, fast_trace) = load
        .execute_paged_with_cursor_traced(fast_plan, None)
        .expect("fast residual underfill execution should succeed");
    let fast_trace = fast_trace.expect("debug trace should be present");

    let (fallback_page, fallback_trace) = load
        .execute_paged_with_cursor_traced(fallback_plan, None)
        .expect("fallback residual underfill execution should succeed");
    let _fallback_trace = fallback_trace.expect("debug trace should be present");

    assert_eq!(
        ids_from_items(&fast_page.items),
        ids_from_items(&fallback_page.items),
        "residual underfill bounded widening must preserve fallback row parity",
    );
    assert_eq!(
        fast_trace.optimization(),
        Some(ExecutionOptimization::IndexRangeLimitPushdown),
        "residual underfill should widen the bounded pushdown window before degrading to the unbounded fallback path",
    );
    assert!(
        fast_trace.keys_scanned() > 3,
        "residual underfill should rescan beyond the initial bounded fetch window when the first bounded probe under-fills",
    );
}

#[test]
fn load_index_range_limit_pushdown_residual_predicate_parity_matches_canonical_fallback_matrix() {
    setup_pagination_test();

    let rows = [
        (35_501, 10, "drop-t10"),
        (35_502, 11, "drop-t11"),
        (35_503, 12, "drop-t12"),
        (35_504, 13, "keep-t13"),
        (35_505, 14, "keep-t14"),
        (35_506, 15, "keep-t15"),
        (35_507, 16, "keep-t16"),
        (35_508, 17, "drop-t17"),
        (35_509, 18, "keep-t18"),
        (35_510, 19, "keep-t19"),
    ];
    seed_indexed_metrics_rows(&rows);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    let cases = [
        ("bounded-satisfied-without-retry", 13u64, 20u64, 2u32, 0u32),
        ("bounded-underfill-retry-required", 10u64, 16u64, 2u32, 0u32),
    ];

    for (case_name, lower, upper, limit, offset) in cases {
        let label_contains_keep = Predicate::TextContainsCi {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        };
        let mut fast_logical = AccessPlannedQuery::new(
            AccessPath::index_range(
                INDEXED_METRICS_INDEX_MODELS[0],
                Vec::new(),
                Bound::Included(Value::Uint(lower)),
                Bound::Excluded(Value::Uint(upper)),
            ),
            MissingRowPolicy::Ignore,
        );
        fast_logical.scalar_plan_mut().predicate = Some(label_contains_keep.clone());
        fast_logical.scalar_plan_mut().order = Some(OrderSpec {
            fields: vec![
                ("tag".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });
        fast_logical.scalar_plan_mut().page = Some(PageSpec {
            limit: Some(limit),
            offset,
        });
        let fast_plan = PreparedExecutionPlan::<IndexedMetricsEntity>::new(fast_logical);

        let mut fallback_logical =
            AccessPlannedQuery::new(AccessPath::FullScan, MissingRowPolicy::Ignore);
        fallback_logical.scalar_plan_mut().predicate = Some(Predicate::And(vec![
            strict_compare_predicate("tag", CompareOp::Gte, Value::Uint(lower)),
            strict_compare_predicate("tag", CompareOp::Lt, Value::Uint(upper)),
            label_contains_keep,
        ]));
        fallback_logical.scalar_plan_mut().order = Some(OrderSpec {
            fields: vec![
                ("tag".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });
        fallback_logical.scalar_plan_mut().page = Some(PageSpec {
            limit: Some(limit),
            offset,
        });
        let fallback_plan = PreparedExecutionPlan::<IndexedMetricsEntity>::new(fallback_logical);

        let (fast_page, _fast_trace) = load
            .execute_paged_with_cursor_traced(fast_plan, None)
            .expect("fast residual matrix execution should succeed");
        let (fallback_page, _fallback_trace) = load
            .execute_paged_with_cursor_traced(fallback_plan, None)
            .expect("fallback residual matrix execution should succeed");

        assert_eq!(
            ids_from_items(&fast_page.items),
            ids_from_items(&fallback_page.items),
            "residual range matrix case should preserve fallback row parity: case={case_name}",
        );
        assert_eq!(
            fast_page.next_cursor.is_some(),
            fallback_page.next_cursor.is_some(),
            "residual range matrix case should preserve continuation presence parity: case={case_name}",
        );
        if let (Some(fast_cursor), Some(fallback_cursor)) =
            (&fast_page.next_cursor, &fallback_page.next_cursor)
        {
            assert_eq!(
                scalar_boundary(fast_cursor),
                scalar_boundary(fallback_cursor),
                "residual range matrix case should preserve continuation boundary parity: case={case_name}",
            );
        }
    }
}

#[test]
fn load_index_only_predicate_reduces_access_rows_vs_fallback() {
    setup_pagination_test();

    let rows = [
        (36_001, 7, 10, "g7-r10"),
        (36_002, 7, 20, "g7-r20-a"),
        (36_003, 7, 20, "g7-r20-b"),
        (36_004, 7, 30, "g7-r30"),
        (36_005, 7, 40, "g7-r40"),
        (36_006, 8, 20, "g8-r20"),
    ];
    seed_pushdown_rows(&rows);

    let rank_not_20_strict = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Ne,
        Value::Uint(20),
        CoercionId::Strict,
    ));
    let rank_not_20_fallback = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Ne,
        Value::Uint(20),
        CoercionId::NumericWiden,
    ));
    let group_eq_fallback = Predicate::Compare(ComparePredicate::with_coercion(
        "group",
        CompareOp::Eq,
        Value::Uint(7),
        CoercionId::NumericWiden,
    ));
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);

    let (fast_page, fast_trace) = load
        .execute_paged_with_cursor_traced(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(Predicate::And(vec![
                    pushdown_group_predicate(7),
                    rank_not_20_strict,
                ]))
                .order_by("rank")
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("index-shape plan should build"),
            None,
        )
        .expect("index-shape execution should succeed");
    let fast_trace = fast_trace.expect("debug trace should be present");

    let (fallback_page, fallback_trace) = load
        .execute_paged_with_cursor_traced(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(Predicate::And(vec![
                    group_eq_fallback,
                    rank_not_20_fallback,
                ]))
                .order_by("rank")
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("fallback plan should build"),
            None,
        )
        .expect("fallback execution should succeed");
    let fallback_trace = fallback_trace.expect("debug trace should be present");

    assert_eq!(
        ids_from_items(&fast_page.items),
        ids_from_items(&fallback_page.items),
        "index-only predicate path must preserve fallback result parity",
    );
    assert!(
        fast_trace.index_predicate_applied(),
        "index-backed strict predicate should activate index-only evaluation"
    );
    assert!(
        !fallback_trace.index_predicate_applied(),
        "by-ids fallback path must not report index-only predicate activation"
    );
    assert!(
        fast_trace.index_predicate_keys_rejected() > 0,
        "index-only path should report rejected index keys for non-matching predicate rows",
    );
    assert_eq!(
        fallback_trace.index_predicate_keys_rejected(),
        0,
        "fallback path must not report index-only rejected-key counts",
    );
    assert_eq!(
        fast_trace.distinct_keys_deduped(),
        0,
        "non-distinct plans must not report DISTINCT dedup activity",
    );
    assert_eq!(
        fallback_trace.distinct_keys_deduped(),
        0,
        "non-distinct fallback plans must not report DISTINCT dedup activity",
    );
    assert!(
        fast_trace.keys_scanned() < fallback_trace.keys_scanned(),
        "index-only predicate activation should reduce scanned rows for this shape",
    );
}

#[test]
// This migrated DISTINCT continuation contract is intentionally kept end-to-end in one test.
#[expect(clippy::too_many_lines)]
fn load_index_only_predicate_distinct_continuation_matches_fallback() {
    setup_pagination_test();

    let rows = [
        (36_101, 7, 10, "g7-r10"),
        (36_102, 7, 20, "g7-r20-a"),
        (36_103, 7, 20, "g7-r20-b"),
        (36_104, 7, 30, "g7-r30"),
        (36_105, 7, 40, "g7-r40"),
        (36_106, 8, 1, "g8-r1"),
    ];
    seed_pushdown_rows(&rows);

    let rank_not_20_strict = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Ne,
        Value::Uint(20),
        CoercionId::Strict,
    ));
    let rank_not_20_fallback = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Ne,
        Value::Uint(20),
        CoercionId::NumericWiden,
    ));
    let group_eq_fallback = Predicate::Compare(ComparePredicate::with_coercion(
        "group",
        CompareOp::Eq,
        Value::Uint(7),
        CoercionId::NumericWiden,
    ));
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);

    let build_fast_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(Predicate::And(vec![
                pushdown_group_predicate(7),
                rank_not_20_strict.clone(),
            ]))
            .order_by("rank")
            .distinct()
            .limit(2)
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("fast distinct plan should build")
    };
    let build_fallback_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(Predicate::And(vec![
                group_eq_fallback.clone(),
                rank_not_20_fallback.clone(),
            ]))
            .order_by("rank")
            .distinct()
            .limit(2)
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("fallback distinct plan should build")
    };

    let (fast_page1, fast_trace1) = load
        .execute_paged_with_cursor_traced(build_fast_plan(), None)
        .expect("fast distinct page1 should execute");
    let fast_trace1 = fast_trace1.expect("debug trace should be present");
    let (fallback_page1, fallback_trace1) = load
        .execute_paged_with_cursor_traced(build_fallback_plan(), None)
        .expect("fallback distinct page1 should execute");
    let fallback_trace1 = fallback_trace1.expect("debug trace should be present");

    assert_eq!(
        ids_from_items(&fast_page1.items),
        ids_from_items(&fallback_page1.items),
        "fast and fallback distinct page1 rows should match",
    );
    assert!(
        fast_trace1.index_predicate_applied() && !fast_trace1.continuation_applied(),
        "first index-only page should report activation without continuation"
    );
    assert!(
        !fallback_trace1.index_predicate_applied(),
        "fallback distinct page1 must not report index-only activation"
    );
    assert_eq!(
        fallback_trace1.optimization(),
        None,
        "fallback distinct page1 should remain non-optimized",
    );
    assert!(
        fast_trace1.index_predicate_keys_rejected() > 0,
        "index-only distinct page1 should report rejected index keys",
    );
    assert_eq!(
        fallback_trace1.index_predicate_keys_rejected(),
        0,
        "fallback distinct page1 must not report index-only rejected-key counts",
    );
    assert_eq!(
        fast_trace1.distinct_keys_deduped(),
        fallback_trace1.distinct_keys_deduped(),
        "fast and fallback distinct page1 should report the same DISTINCT dedup count",
    );

    let fast_cursor = fast_page1
        .next_cursor
        .as_ref()
        .expect("fast distinct page1 should emit continuation cursor");
    let fallback_cursor = fallback_page1
        .next_cursor
        .as_ref()
        .expect("fallback distinct page1 should emit continuation cursor");
    let shared_boundary = scalar_boundary(fast_cursor);
    assert_eq!(
        scalar_boundary(fast_cursor),
        scalar_boundary(fallback_cursor),
        "fast and fallback distinct page1 cursors should encode the same boundary",
    );

    let (fast_page2, fast_trace2) = load
        .execute_paged_with_cursor_traced(build_fast_plan(), Some(shared_boundary.clone()))
        .expect("fast distinct page2 should execute");
    let fast_trace2 = fast_trace2.expect("debug trace should be present");
    let (fallback_page2, fallback_trace2) = load
        .execute_paged_with_cursor_traced(build_fallback_plan(), Some(shared_boundary))
        .expect("fallback distinct page2 should execute");
    let fallback_trace2 = fallback_trace2.expect("debug trace should be present");

    assert_eq!(
        ids_from_items(&fast_page2.items),
        ids_from_items(&fallback_page2.items),
        "fast and fallback distinct page2 rows should match",
    );
    assert!(
        fast_trace2.index_predicate_applied() && fast_trace2.continuation_applied(),
        "continued index-only page should report both activation and continuation"
    );
    assert!(
        !fallback_trace2.index_predicate_applied(),
        "fallback distinct page2 must not report index-only activation"
    );
    assert_eq!(
        fallback_trace2.optimization(),
        None,
        "fallback distinct page2 should remain non-optimized",
    );
    assert_eq!(
        fallback_trace2.index_predicate_keys_rejected(),
        0,
        "fallback distinct page2 must not report index-only rejected-key counts",
    );
    assert_eq!(
        fast_trace2.distinct_keys_deduped(),
        fallback_trace2.distinct_keys_deduped(),
        "fast and fallback distinct page2 should report the same DISTINCT dedup count",
    );
    assert_eq!(
        fast_page2.next_cursor.is_some(),
        fallback_page2.next_cursor.is_some(),
        "fast and fallback distinct page2 continuation presence should match",
    );
}

#[test]
// This migrated DESC DISTINCT continuation contract is intentionally kept end-to-end in one test.
#[expect(clippy::too_many_lines)]
fn load_index_only_predicate_distinct_desc_continuation_matches_fallback() {
    setup_pagination_test();

    let rows = [
        (36_201, 7, 10, "g7-r10"),
        (36_202, 7, 20, "g7-r20-a"),
        (36_203, 7, 20, "g7-r20-b"),
        (36_204, 7, 30, "g7-r30"),
        (36_205, 7, 40, "g7-r40"),
        (36_206, 8, 1, "g8-r1"),
    ];
    seed_pushdown_rows(&rows);

    let rank_not_20_strict = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Ne,
        Value::Uint(20),
        CoercionId::Strict,
    ));
    let rank_not_20_fallback = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Ne,
        Value::Uint(20),
        CoercionId::NumericWiden,
    ));
    let group_eq_fallback = Predicate::Compare(ComparePredicate::with_coercion(
        "group",
        CompareOp::Eq,
        Value::Uint(7),
        CoercionId::NumericWiden,
    ));
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);

    let build_fast_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(Predicate::And(vec![
                pushdown_group_predicate(7),
                rank_not_20_strict.clone(),
            ]))
            .order_by_desc("rank")
            .distinct()
            .limit(2)
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("fast descending distinct plan should build")
    };
    let build_fallback_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(Predicate::And(vec![
                group_eq_fallback.clone(),
                rank_not_20_fallback.clone(),
            ]))
            .order_by_desc("rank")
            .distinct()
            .limit(2)
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("fallback descending distinct plan should build")
    };

    let (fast_page1, fast_trace1) = load
        .execute_paged_with_cursor_traced(build_fast_plan(), None)
        .expect("fast descending distinct page1 should execute");
    let fast_trace1 = fast_trace1.expect("debug trace should be present");
    let (fallback_page1, fallback_trace1) = load
        .execute_paged_with_cursor_traced(build_fallback_plan(), None)
        .expect("fallback descending distinct page1 should execute");
    let fallback_trace1 = fallback_trace1.expect("debug trace should be present");

    assert_eq!(
        ids_from_items(&fast_page1.items),
        ids_from_items(&fallback_page1.items),
        "fast and fallback descending distinct page1 rows should match",
    );
    assert!(
        fast_trace1.index_predicate_applied() && !fast_trace1.continuation_applied(),
        "first descending index-only page should report activation without continuation"
    );
    assert!(
        !fallback_trace1.index_predicate_applied(),
        "fallback descending distinct page1 must not report index-only activation"
    );
    assert_eq!(
        fallback_trace1.optimization(),
        None,
        "fallback descending distinct page1 should remain non-optimized",
    );

    let fast_cursor = fast_page1
        .next_cursor
        .as_ref()
        .expect("fast descending distinct page1 should emit continuation cursor");
    let fallback_cursor = fallback_page1
        .next_cursor
        .as_ref()
        .expect("fallback descending distinct page1 should emit continuation cursor");
    let shared_boundary = scalar_boundary(fast_cursor);
    assert_eq!(
        scalar_boundary(fast_cursor),
        scalar_boundary(fallback_cursor),
        "fast and fallback descending distinct page1 cursors should encode the same boundary",
    );

    let (fast_page2, fast_trace2) = load
        .execute_paged_with_cursor_traced(build_fast_plan(), Some(shared_boundary.clone()))
        .expect("fast descending distinct page2 should execute");
    let fast_trace2 = fast_trace2.expect("debug trace should be present");
    let (fallback_page2, fallback_trace2) = load
        .execute_paged_with_cursor_traced(build_fallback_plan(), Some(shared_boundary))
        .expect("fallback descending distinct page2 should execute");
    let fallback_trace2 = fallback_trace2.expect("debug trace should be present");

    assert_eq!(
        ids_from_items(&fast_page2.items),
        ids_from_items(&fallback_page2.items),
        "fast and fallback descending distinct page2 rows should match",
    );
    assert!(
        fast_trace2.index_predicate_applied() && fast_trace2.continuation_applied(),
        "continued descending index-only page should report both activation and continuation"
    );
    assert!(
        !fallback_trace2.index_predicate_applied(),
        "fallback descending distinct page2 must not report index-only activation"
    );
    assert_eq!(
        fallback_trace2.optimization(),
        None,
        "fallback descending distinct page2 should remain non-optimized",
    );
    assert_eq!(
        fast_page2.next_cursor.is_some(),
        fallback_page2.next_cursor.is_some(),
        "fast and fallback descending distinct page2 continuation presence should match",
    );
}

#[test]
fn load_index_only_predicate_in_constants_reduces_access_rows_vs_fallback() {
    setup_pagination_test();

    let rows = [
        (36_301, 7, 10, "keep-g7-r10"),
        (36_302, 7, 20, "drop-g7-r20"),
        (36_303, 7, 20, "keep-g7-r20"),
        (36_304, 7, 30, "keep-g7-r30"),
        (36_305, 7, 40, "keep-g7-r40"),
        (36_306, 7, 50, "keep-g7-r50"),
        (36_307, 7, 60, "drop-g7-r60"),
        (36_308, 8, 20, "keep-g8-r20"),
    ];
    seed_pushdown_rows(&rows);

    let rank_in_strict = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::In,
        Value::List(vec![Value::Uint(20), Value::Uint(40), Value::Uint(50)]),
        CoercionId::Strict,
    ));
    let rank_in_fallback = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::In,
        Value::List(vec![Value::Uint(20), Value::Uint(40), Value::Uint(50)]),
        CoercionId::NumericWiden,
    ));
    let group_eq_fallback = Predicate::Compare(ComparePredicate::with_coercion(
        "group",
        CompareOp::Eq,
        Value::Uint(7),
        CoercionId::NumericWiden,
    ));
    let label_contains_keep = Predicate::TextContainsCi {
        field: "label".to_string(),
        value: Value::Text("keep".to_string()),
    };
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);

    let (fast_page, fast_trace) = load
        .execute_paged_with_cursor_traced(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(Predicate::And(vec![
                    pushdown_group_predicate(7),
                    rank_in_strict,
                    label_contains_keep.clone(),
                ]))
                .order_by("rank")
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("strict IN fast plan should build"),
            None,
        )
        .expect("strict IN fast execution should succeed");
    let fast_trace = fast_trace.expect("debug trace should be present");

    let (fallback_page, fallback_trace) = load
        .execute_paged_with_cursor_traced(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(Predicate::And(vec![
                    group_eq_fallback,
                    rank_in_fallback,
                    label_contains_keep,
                ]))
                .order_by("rank")
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("fallback IN plan should build"),
            None,
        )
        .expect("fallback IN execution should succeed");
    let fallback_trace = fallback_trace.expect("debug trace should be present");

    assert_eq!(
        ids_from_items(&fast_page.items),
        ids_from_items(&fallback_page.items),
        "strict IN index-only execution must preserve fallback row parity",
    );
    assert!(
        fast_trace.index_predicate_applied(),
        "strict IN predicate should activate index-only filtering"
    );
    assert!(
        !fallback_trace.index_predicate_applied(),
        "fallback IN path must keep index-only filtering disabled",
    );
    assert!(
        fast_trace.index_predicate_keys_rejected() > 0,
        "strict IN index-only path should reject non-matching index keys",
    );
    assert_eq!(
        fallback_trace.index_predicate_keys_rejected(),
        0,
        "fallback IN path must not report index-only rejected-key counts",
    );
    assert!(
        fast_trace.keys_scanned() < fallback_trace.keys_scanned(),
        "strict IN index-only filtering should reduce scanned rows for this shape",
    );
}

#[test]
// This migrated bounded-range DISTINCT matrix keeps asc/desc parity in one runtime test.
#[expect(clippy::similar_names)]
#[expect(clippy::too_many_lines)]
fn load_index_only_predicate_bounded_range_distinct_continuation_matches_fallback_for_asc_and_desc()
{
    setup_pagination_test();

    let rows = [
        (36_401, 7, 10, "keep-g7-r10"),
        (36_402, 7, 20, "drop-g7-r20"),
        (36_403, 7, 20, "keep-g7-r20"),
        (36_404, 7, 30, "keep-g7-r30"),
        (36_405, 7, 40, "drop-g7-r40"),
        (36_406, 7, 40, "keep-g7-r40"),
        (36_407, 7, 50, "keep-g7-r50"),
        (36_408, 8, 30, "keep-g8-r30"),
    ];
    seed_pushdown_rows(&rows);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);

    for descending in [false, true] {
        let rank_gte_20_strict = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Gte,
            Value::Uint(20),
            CoercionId::Strict,
        ));
        let rank_lte_40_strict = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Lte,
            Value::Uint(40),
            CoercionId::Strict,
        ));
        let rank_gte_20_fallback = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Gte,
            Value::Uint(20),
            CoercionId::NumericWiden,
        ));
        let rank_lte_40_fallback = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Lte,
            Value::Uint(40),
            CoercionId::NumericWiden,
        ));
        let group_eq_fallback = Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::NumericWiden,
        ));
        let label_contains_keep = Predicate::TextContainsCi {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        };

        let build_fast_plan = || {
            let base = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(Predicate::And(vec![
                    pushdown_group_predicate(7),
                    rank_gte_20_strict.clone(),
                    rank_lte_40_strict.clone(),
                    label_contains_keep.clone(),
                ]))
                .distinct()
                .limit(2);

            if descending {
                base.order_by_desc("rank")
                    .plan()
                    .map(crate::db::executor::PreparedExecutionPlan::from)
                    .expect("fast bounded-range descending plan should build")
            } else {
                base.order_by("rank")
                    .plan()
                    .map(crate::db::executor::PreparedExecutionPlan::from)
                    .expect("fast bounded-range ascending plan should build")
            }
        };
        let build_fallback_plan = || {
            let base = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(Predicate::And(vec![
                    group_eq_fallback.clone(),
                    rank_gte_20_fallback.clone(),
                    rank_lte_40_fallback.clone(),
                    label_contains_keep.clone(),
                ]))
                .distinct()
                .limit(2);

            if descending {
                base.order_by_desc("rank")
                    .plan()
                    .map(crate::db::executor::PreparedExecutionPlan::from)
                    .expect("fallback bounded-range descending plan should build")
            } else {
                base.order_by("rank")
                    .plan()
                    .map(crate::db::executor::PreparedExecutionPlan::from)
                    .expect("fallback bounded-range ascending plan should build")
            }
        };

        let (fast_page1, fast_trace1) = load
            .execute_paged_with_cursor_traced(build_fast_plan(), None)
            .expect("fast bounded-range page1 should execute");
        let fast_trace1 = fast_trace1.expect("debug trace should be present");
        let (fallback_page1, fallback_trace1) = load
            .execute_paged_with_cursor_traced(build_fallback_plan(), None)
            .expect("fallback bounded-range page1 should execute");
        let fallback_trace1 = fallback_trace1.expect("debug trace should be present");

        assert_eq!(
            ids_from_items(&fast_page1.items),
            ids_from_items(&fallback_page1.items),
            "fast and fallback bounded-range page1 rows should match for descending={descending}",
        );
        assert!(
            fast_trace1.index_predicate_applied() && !fast_trace1.continuation_applied(),
            "fast bounded-range page1 should report index-only activation for descending={descending}",
        );
        assert!(
            !fallback_trace1.index_predicate_applied(),
            "fallback bounded-range page1 must not report index-only activation for descending={descending}",
        );
        assert_eq!(
            fallback_trace1.optimization(),
            None,
            "fallback bounded-range page1 should remain non-optimized for descending={descending}",
        );
        assert!(
            fast_trace1.index_predicate_keys_rejected() > 0,
            "fast bounded-range page1 should reject non-matching index keys for descending={descending}",
        );
        assert_eq!(
            fallback_trace1.index_predicate_keys_rejected(),
            0,
            "fallback bounded-range page1 must not report index-only rejected-key counts for descending={descending}",
        );

        let fast_cursor = fast_page1
            .next_cursor
            .as_ref()
            .expect("fast bounded-range page1 should emit continuation cursor");
        let fallback_cursor = fallback_page1
            .next_cursor
            .as_ref()
            .expect("fallback bounded-range page1 should emit continuation cursor");
        let shared_boundary = scalar_boundary(fast_cursor);
        assert_eq!(
            scalar_boundary(fast_cursor),
            scalar_boundary(fallback_cursor),
            "fast and fallback bounded-range page1 cursors should match for descending={descending}",
        );

        let (fast_page2, fast_trace2) = load
            .execute_paged_with_cursor_traced(build_fast_plan(), Some(shared_boundary.clone()))
            .expect("fast bounded-range page2 should execute");
        let fast_trace2 = fast_trace2.expect("debug trace should be present");
        let (fallback_page2, fallback_trace2) = load
            .execute_paged_with_cursor_traced(build_fallback_plan(), Some(shared_boundary))
            .expect("fallback bounded-range page2 should execute");
        let fallback_trace2 = fallback_trace2.expect("debug trace should be present");

        assert_eq!(
            ids_from_items(&fast_page2.items),
            ids_from_items(&fallback_page2.items),
            "fast and fallback bounded-range page2 rows should match for descending={descending}",
        );
        assert!(
            fast_trace2.index_predicate_applied() && fast_trace2.continuation_applied(),
            "fast bounded-range page2 should report activation with continuation for descending={descending}",
        );
        assert!(
            !fallback_trace2.index_predicate_applied(),
            "fallback bounded-range page2 must not report index-only activation for descending={descending}",
        );
        assert_eq!(
            fallback_trace2.optimization(),
            None,
            "fallback bounded-range page2 should remain non-optimized for descending={descending}",
        );
        assert_eq!(
            fallback_trace2.index_predicate_keys_rejected(),
            0,
            "fallback bounded-range page2 must not report index-only rejected-key counts for descending={descending}",
        );
        assert_eq!(
            fast_trace1.distinct_keys_deduped(),
            fallback_trace1.distinct_keys_deduped(),
            "fast and fallback bounded-range page1 distinct counts should match for descending={descending}",
        );
        assert_eq!(
            fast_trace2.distinct_keys_deduped(),
            fallback_trace2.distinct_keys_deduped(),
            "fast and fallback bounded-range page2 distinct counts should match for descending={descending}",
        );
        assert_eq!(
            fast_page2.next_cursor.is_some(),
            fallback_page2.next_cursor.is_some(),
            "fast and fallback bounded-range page2 continuation presence should match for descending={descending}",
        );

        let fast_scanned_total = fast_trace1
            .keys_scanned()
            .saturating_add(fast_trace2.keys_scanned());
        let fallback_scanned_total = fallback_trace1
            .keys_scanned()
            .saturating_add(fallback_trace2.keys_scanned());
        assert!(
            fast_scanned_total < fallback_scanned_total,
            "fast bounded-range index-only filtering should reduce total scanned rows for descending={descending}",
        );
    }
}
