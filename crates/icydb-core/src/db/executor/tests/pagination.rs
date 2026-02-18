#![expect(clippy::too_many_lines)]
use super::*;
use crate::{
    db::{
        index::RawIndexKey,
        query::{
            intent::{LoadSpec, QueryMode},
            plan::{
                AccessPath, AccessPlan, ExecutablePlan, ExplainAccessPath, ExplainOrderPushdown,
                LogicalPlan, OrderDirection, OrderSpec, PageSpec, PlanError,
                validate::SecondaryOrderPushdownRejection,
            },
        },
    },
    error::{ErrorClass, ErrorOrigin},
    serialize::serialize,
    types::Id,
};
use std::{collections::BTreeSet, ops::Bound};

// Resolve ids directly from the `(group, rank)` index prefix in raw index-key order.
fn ordered_ids_from_group_rank_index(group: u32) -> Vec<Ulid> {
    // Phase 1: read candidate keys from canonical index traversal order.
    let data_keys = DB
        .with_store_registry(|reg| {
            reg.try_get_store(TestDataStore::PATH).and_then(|store| {
                store.with_index(|index_store| {
                    index_store.resolve_data_values::<PushdownParityEntity>(
                        &PUSHDOWN_PARITY_INDEX_MODELS[0],
                        &[Value::Uint(u64::from(group))],
                    )
                })
            })
        })
        .expect("index prefix resolution should succeed");

    // Phase 2: decode typed ids while preserving traversal order.
    data_keys
        .into_iter()
        .map(|data_key| data_key.try_key::<PushdownParityEntity>())
        .collect::<Result<Vec<_>, _>>()
        .expect("resolved index keys should decode to entity ids")
}

type PushdownSeedRow = (u128, u32, u32, &'static str);

fn pushdown_entity((id, group, rank, label): PushdownSeedRow) -> PushdownParityEntity {
    PushdownParityEntity {
        id: Ulid::from_u128(id),
        group,
        rank,
        label: label.to_string(),
    }
}

fn setup_pagination_test() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();
}

fn seed_pushdown_rows(rows: &[PushdownSeedRow]) {
    let save = SaveExecutor::<PushdownParityEntity>::new(DB, false);
    for row in rows {
        save.insert(pushdown_entity(*row))
            .expect("seed row save should succeed");
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

fn pushdown_group_ids(rows: &[PushdownSeedRow], group: u32) -> Vec<Ulid> {
    rows.iter()
        .filter(|(_, row_group, _, _)| *row_group == group)
        .map(|(id, _, _, _)| Ulid::from_u128(*id))
        .collect()
}

fn pushdown_rows_with_group8(prefix: u128) -> [PushdownSeedRow; 5] {
    [
        (prefix + 3, 7, 30, "g7-r30"),
        (prefix + 1, 7, 10, "g7-r10-a"),
        (prefix + 2, 7, 10, "g7-r10-b"),
        (prefix + 4, 8, 5, "g8-r5"),
        (prefix + 5, 7, 20, "g7-r20"),
    ]
}

fn pushdown_rows_with_group9(prefix: u128) -> [PushdownSeedRow; 6] {
    [
        (prefix + 3, 7, 30, "g7-r30"),
        (prefix + 1, 7, 10, "g7-r10-a"),
        (prefix + 2, 7, 10, "g7-r10-b"),
        (prefix + 4, 7, 20, "g7-r20"),
        (prefix + 5, 7, 40, "g7-r40"),
        (prefix + 6, 9, 1, "g9-r1"),
    ]
}

fn pushdown_rows_window(prefix: u128) -> [PushdownSeedRow; 4] {
    [
        (prefix + 1, 7, 10, "g7-r10"),
        (prefix + 2, 7, 20, "g7-r20"),
        (prefix + 3, 7, 30, "g7-r30"),
        (prefix + 4, 9, 1, "g9-r1"),
    ]
}

fn pushdown_rows_trace(prefix: u128) -> [PushdownSeedRow; 2] {
    [(prefix + 1, 7, 10, "g7-r10"), (prefix + 2, 7, 20, "g7-r20")]
}

type IndexedMetricsSeedRow = (u128, u32, &'static str);

fn indexed_metrics_entity((id, tag, label): IndexedMetricsSeedRow) -> IndexedMetricsEntity {
    IndexedMetricsEntity {
        id: Ulid::from_u128(id),
        tag,
        label: label.to_string(),
    }
}

fn seed_indexed_metrics_rows(rows: &[IndexedMetricsSeedRow]) {
    let save = SaveExecutor::<IndexedMetricsEntity>::new(DB, false);
    for row in rows {
        save.insert(indexed_metrics_entity(*row))
            .expect("indexed-metrics seed row save should succeed");
    }
}

type UniqueIndexRangeSeedRow = (u128, u32, &'static str);

fn unique_index_range_entity((id, code, label): UniqueIndexRangeSeedRow) -> UniqueIndexRangeEntity {
    UniqueIndexRangeEntity {
        id: Ulid::from_u128(id),
        code,
        label: label.to_string(),
    }
}

fn seed_unique_index_range_rows(rows: &[UniqueIndexRangeSeedRow]) {
    let save = SaveExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    for row in rows {
        save.insert(unique_index_range_entity(*row))
            .expect("unique-index-range seed row save should succeed");
    }
}

fn strict_compare_predicate(field: &str, op: CompareOp, value: Value) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        value,
        CoercionId::Strict,
    ))
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

fn group_rank_range_predicate(group: u32, lower_inclusive: u32, upper_exclusive: u32) -> Predicate {
    Predicate::And(vec![
        strict_compare_predicate("group", CompareOp::Eq, Value::Uint(u64::from(group))),
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
        strict_compare_predicate("group", CompareOp::Eq, Value::Uint(u64::from(group))),
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

// Evaluate one scalar comparison for table-driven range matrix assertions.
fn scalar_u32_matches_compare(value: u32, op: CompareOp, bound: u32) -> bool {
    match op {
        CompareOp::Eq => value == bound,
        CompareOp::Gt => value > bound,
        CompareOp::Gte => value >= bound,
        CompareOp::Lt => value < bound,
        CompareOp::Lte => value <= bound,
        _ => panic!("range-matrix helper only supports Eq/Gt/Gte/Lt/Lte operators"),
    }
}

// Build one strict predicate from a list of range bounds for a single field.
fn predicate_from_field_bounds(field: &str, bounds: &[(CompareOp, u32)]) -> Predicate {
    let mut predicates = Vec::with_capacity(bounds.len());
    for (op, bound) in bounds {
        predicates.push(strict_compare_predicate(
            field,
            *op,
            Value::Uint(u64::from(*bound)),
        ));
    }
    if predicates.len() == 1 {
        return predicates
            .pop()
            .expect("single-bound predicate list should contain one predicate");
    }

    Predicate::And(predicates)
}

// Compute expected IDs for single-field range matrix cases.
fn indexed_metrics_ids_for_bounds(
    rows: &[IndexedMetricsSeedRow],
    bounds: &[(CompareOp, u32)],
) -> Vec<Ulid> {
    rows.iter()
        .filter(|(_, tag, _)| {
            bounds
                .iter()
                .all(|(op, bound)| scalar_u32_matches_compare(*tag, *op, *bound))
        })
        .map(|(id, _, _)| Ulid::from_u128(*id))
        .collect()
}

// Compute expected IDs for composite `(group, rank)` range matrix cases.
fn pushdown_ids_for_group_rank_bounds(
    rows: &[PushdownSeedRow],
    group: u32,
    bounds: &[(CompareOp, u32)],
) -> Vec<Ulid> {
    rows.iter()
        .filter(|(_, row_group, rank, _)| {
            *row_group == group
                && bounds
                    .iter()
                    .all(|(op, bound)| scalar_u32_matches_compare(*rank, *op, *bound))
        })
        .map(|(id, _, _, _)| Ulid::from_u128(*id))
        .collect()
}

fn indexed_metrics_ids_in_tag_range(
    rows: &[IndexedMetricsSeedRow],
    lower_inclusive: u32,
    upper_exclusive: u32,
) -> Vec<Ulid> {
    rows.iter()
        .filter(|(_, tag, _)| *tag >= lower_inclusive && *tag < upper_exclusive)
        .map(|(id, _, _)| Ulid::from_u128(*id))
        .collect()
}

fn indexed_metrics_ids_in_between_equivalent_range(
    rows: &[IndexedMetricsSeedRow],
    lower_inclusive: u32,
    upper_inclusive: u32,
) -> Vec<Ulid> {
    rows.iter()
        .filter(|(_, tag, _)| *tag >= lower_inclusive && *tag <= upper_inclusive)
        .map(|(id, _, _)| Ulid::from_u128(*id))
        .collect()
}

fn pushdown_ids_in_group_rank_range(
    rows: &[PushdownSeedRow],
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
    rows: &[PushdownSeedRow],
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

fn explain_contains_index_range(
    access: &ExplainAccessPath,
    index_name: &'static str,
    prefix_len: usize,
) -> bool {
    match access {
        ExplainAccessPath::IndexRange {
            name,
            prefix_len: actual_prefix_len,
            ..
        } => *name == index_name && *actual_prefix_len == prefix_len,
        ExplainAccessPath::Union(children) | ExplainAccessPath::Intersection(children) => children
            .iter()
            .any(|child| explain_contains_index_range(child, index_name, prefix_len)),
        _ => false,
    }
}

fn extract_access_rows(events: &[QueryTraceEvent]) -> Option<u64> {
    events.iter().find_map(|event| match event {
        QueryTraceEvent::Phase {
            phase: TracePhase::Access,
            rows,
            ..
        } => Some(*rows),
        _ => None,
    })
}

fn assert_anchor_monotonic(
    anchors: &mut Vec<RawIndexKey>,
    next_cursor: &[u8],
    decode_message: &'static str,
    missing_anchor_message: &'static str,
    monotonic_message: &'static str,
) {
    let token = ContinuationToken::decode(next_cursor).expect(decode_message);
    let anchor = token
        .index_range_anchor()
        .expect(missing_anchor_message)
        .last_raw_key()
        .clone();

    if let Some(previous_anchor) = anchors.last() {
        assert!(previous_anchor < &anchor, "{monotonic_message}");
    }
    anchors.push(anchor);
}

fn ids_from_items<E>(items: &[(Id<E>, E)]) -> Vec<Ulid>
where
    E: EntityKind<Key = Ulid>,
{
    items.iter().map(|(id, _)| id.key()).collect()
}

fn decode_boundary(cursor: &[u8], decode_message: &'static str) -> CursorBoundary {
    ContinuationToken::decode(cursor)
        .expect(decode_message)
        .boundary()
        .clone()
}

fn assert_pushdown_parity<E, I, O>(
    build_pushdown_query: impl Fn() -> Query<E>,
    fallback_ids: I,
    apply_order: O,
) where
    E: EntityKind<Key = Ulid, Canister = TestCanister> + EntityValue,
    I: IntoIterator<Item = Ulid>,
    O: Fn(Query<E>) -> Query<E>,
{
    let load = LoadExecutor::<E>::new(DB, false);

    let pushdown = load
        .execute(
            build_pushdown_query()
                .plan()
                .expect("pushdown plan should build"),
        )
        .expect("pushdown execution should succeed");

    let fallback = load
        .execute(
            apply_order(Query::<E>::new(ReadConsistency::MissingOk).by_ids(fallback_ids))
                .plan()
                .expect("fallback plan should build"),
        )
        .expect("fallback execution should succeed");

    let push_ids: Vec<Ulid> = ids_from_items(&pushdown.0);
    let fallback_ids: Vec<Ulid> = ids_from_items(&fallback.0);

    assert_eq!(push_ids, fallback_ids);
}

fn collect_all_pages<E>(
    load: &LoadExecutor<E>,
    build_query: impl Fn() -> Query<E>,
    max_pages: usize,
) -> (Vec<Ulid>, Vec<Vec<u8>>)
where
    E: EntityKind<Key = Ulid, Canister = TestCanister> + EntityValue,
{
    let mut cursor: Option<Vec<u8>> = None;
    let mut ids = Vec::new();
    let mut row_bytes = Vec::new();
    let mut pages = 0usize;

    loop {
        pages += 1;
        assert!(pages <= max_pages, "pagination must terminate");

        let plan = build_query().plan().expect("page plan should build");

        let planned_cursor = plan
            .plan_cursor(cursor.as_deref())
            .expect("page cursor should plan");

        let page = load
            .execute_paged_with_cursor(plan, planned_cursor)
            .expect("paged execution should succeed");

        ids.extend(ids_from_items(&page.items.0));
        row_bytes.extend(
            page.items
                .0
                .iter()
                .map(|(_, e)| serialize(e).expect("entity serialization should succeed")),
        );

        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    (ids, row_bytes)
}

fn assert_limit_matrix<E>(build_base_query: impl Fn() -> Query<E>, limits: &[u32], max_pages: usize)
where
    E: EntityKind<Key = Ulid, Canister = TestCanister> + EntityValue,
{
    let load = LoadExecutor::<E>::new(DB, false);

    let unbounded = load
        .execute(
            build_base_query()
                .plan()
                .expect("unbounded plan should build"),
        )
        .expect("unbounded execution should succeed");

    let unbounded_ids: Vec<Ulid> = ids_from_items(&unbounded.0);

    for &limit in limits {
        let (ids, _) = collect_all_pages(&load, || build_base_query().limit(limit), max_pages);

        if limit == 0 {
            assert!(ids.is_empty());
            continue;
        }

        assert_eq!(ids, unbounded_ids);

        let unique: BTreeSet<Ulid> = ids.iter().copied().collect();
        assert_eq!(unique.len(), ids.len());
    }
}

fn assert_resume_after_entity<E>(
    build_query: impl Fn() -> Query<E>,
    entity: &E,
    expected_ids: Vec<Ulid>,
) where
    E: EntityKind<Key = Ulid, Canister = TestCanister> + EntityValue,
{
    let load = LoadExecutor::<E>::new(DB, false);

    let boundary = build_query()
        .plan()
        .expect("boundary plan should build")
        .into_inner()
        .cursor_boundary_from_entity(entity)
        .expect("boundary should build");

    let page = load
        .execute_paged_with_cursor(
            build_query().plan().expect("resume plan should build"),
            Some(boundary),
        )
        .expect("resume execution should succeed");

    let ids: Vec<Ulid> = ids_from_items(&page.items.0);

    assert_eq!(ids, expected_ids);
}

#[test]
fn load_applies_order_and_pagination() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [3_u128, 1_u128, 2_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(1)
        .offset(1)
        .plan()
        .expect("load plan should build");

    let response = load.execute(plan).expect("load should succeed");
    assert_eq!(response.0.len(), 1, "pagination should return one row");
    assert_eq!(
        response.0[0].1.id,
        Ulid::from_u128(2),
        "pagination should run after canonical ordering by id"
    );
}

#[test]
fn load_offset_pagination_preserves_next_cursor_boundary() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [5_u128, 1_u128, 4_u128, 2_u128, 3_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let page_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("offset page plan should build");
    let page_boundary = page_plan
        .plan_cursor(None)
        .expect("offset page boundary should plan");
    let page = load
        .execute_paged_with_cursor(page_plan, page_boundary)
        .expect("offset page should execute");

    let page_ids: Vec<Ulid> = ids_from_items(&page.items.0);
    assert_eq!(
        page_ids,
        vec![Ulid::from_u128(2), Ulid::from_u128(3)],
        "offset pagination should return canonical ordered window"
    );

    let cursor_bytes = page
        .next_cursor
        .as_ref()
        .expect("offset page should emit continuation cursor");
    let token = ContinuationToken::decode(cursor_bytes.as_slice())
        .expect("continuation cursor should decode");
    let comparison_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("comparison plan should build")
        .into_inner();
    let expected_boundary = comparison_plan
        .cursor_boundary_from_entity(&page.items.0[1].1)
        .expect("expected boundary should build");
    assert_eq!(
        token.boundary(),
        &expected_boundary,
        "next cursor must encode the last returned row for offset pages"
    );
}

#[test]
fn load_cursor_pagination_pk_order_round_trips_across_pages() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [4_u128, 1_u128, 3_u128, 2_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let page1_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .plan()
        .expect("pk-order page1 plan should build");
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("pk-order page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("pk-order page1 should execute");
    let page1_ids: Vec<Ulid> = ids_from_items(&page1.items.0);
    assert_eq!(page1_ids, vec![Ulid::from_u128(1), Ulid::from_u128(2)]);

    let cursor = page1
        .next_cursor
        .as_ref()
        .expect("pk-order page1 should emit continuation cursor");
    let page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .plan()
        .expect("pk-order page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor(Some(cursor.as_slice()))
        .expect("pk-order page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("pk-order page2 should execute");
    let page2_ids: Vec<Ulid> = ids_from_items(&page2.items.0);
    assert_eq!(page2_ids, vec![Ulid::from_u128(3), Ulid::from_u128(4)]);
    assert!(
        page2.next_cursor.is_none(),
        "final pk-order page should not emit continuation cursor"
    );
}

#[test]
fn load_cursor_pagination_pk_fast_path_matches_non_fast_post_access_semantics() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    let keys = [5_u128, 1_u128, 4_u128, 2_u128, 3_u128];
    for id in keys {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    // Path A: full scan + PK ASC is fast-path eligible.
    let fast_page1_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("fast page1 plan should build");
    let fast_page1_boundary = fast_page1_plan
        .plan_cursor(None)
        .expect("fast page1 boundary should plan");
    let fast_page1 = load
        .execute_paged_with_cursor(fast_page1_plan, fast_page1_boundary)
        .expect("fast page1 should execute");

    // Path B: key-batch access forces non-fast path, but post-access semantics are identical.
    let non_fast_page1_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .by_ids(keys.into_iter().map(Ulid::from_u128))
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("non-fast page1 plan should build");
    let non_fast_page1_boundary = non_fast_page1_plan
        .plan_cursor(None)
        .expect("non-fast page1 boundary should plan");
    let non_fast_page1 = load
        .execute_paged_with_cursor(non_fast_page1_plan, non_fast_page1_boundary)
        .expect("non-fast page1 should execute");

    let fast_page1_ids: Vec<Ulid> = ids_from_items(&fast_page1.items.0);
    let non_fast_page1_ids: Vec<Ulid> = ids_from_items(&non_fast_page1.items.0);
    assert_eq!(
        fast_page1_ids, non_fast_page1_ids,
        "page1 rows should match between fast and non-fast access paths"
    );
    assert_eq!(
        fast_page1.next_cursor.is_some(),
        non_fast_page1.next_cursor.is_some(),
        "page1 cursor presence should match between paths"
    );

    let fast_cursor_page1 = fast_page1
        .next_cursor
        .as_ref()
        .expect("fast page1 should emit continuation cursor");
    let non_fast_cursor_page1 = non_fast_page1
        .next_cursor
        .as_ref()
        .expect("non-fast page1 should emit continuation cursor");
    let fast_cursor_page1_boundary =
        decode_boundary(fast_cursor_page1.as_slice(), "fast cursor should decode");
    let non_fast_cursor_page1_boundary = decode_boundary(
        non_fast_cursor_page1.as_slice(),
        "non-fast cursor should decode",
    );
    assert_eq!(
        &fast_cursor_page1_boundary, &non_fast_cursor_page1_boundary,
        "cursor boundaries should match even when signatures differ by access path"
    );

    let fast_page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("fast page2 plan should build");
    let fast_page2_boundary = fast_page2_plan
        .plan_cursor(Some(fast_cursor_page1.as_slice()))
        .expect("fast page2 boundary should plan");
    let fast_page2 = load
        .execute_paged_with_cursor(fast_page2_plan, fast_page2_boundary)
        .expect("fast page2 should execute");

    let non_fast_page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .by_ids(keys.into_iter().map(Ulid::from_u128))
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("non-fast page2 plan should build");
    let non_fast_page2_boundary = non_fast_page2_plan
        .plan_cursor(Some(non_fast_cursor_page1.as_slice()))
        .expect("non-fast page2 boundary should plan");
    let non_fast_page2 = load
        .execute_paged_with_cursor(non_fast_page2_plan, non_fast_page2_boundary)
        .expect("non-fast page2 should execute");

    let fast_page2_ids: Vec<Ulid> = ids_from_items(&fast_page2.items.0);
    let non_fast_page2_ids: Vec<Ulid> = ids_from_items(&non_fast_page2.items.0);
    assert_eq!(
        fast_page2_ids, non_fast_page2_ids,
        "page2 rows should match between fast and non-fast access paths"
    );
    assert_eq!(
        fast_page2.next_cursor.is_some(),
        non_fast_page2.next_cursor.is_some(),
        "page2 cursor presence should match between paths"
    );
}

#[test]
fn load_cursor_pagination_pk_fast_path_matches_non_fast_with_same_cursor_boundary() {
    setup_pagination_test();

    // Phase 1: seed rows with non-sorted insertion order.
    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    let keys = [7_u128, 1_u128, 6_u128, 2_u128, 5_u128, 3_u128, 4_u128];
    for id in keys {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    // Phase 2: capture one canonical cursor boundary from an initial fast-path page.
    let page1_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(3)
        .plan()
        .expect("cursor source plan should build");
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("cursor source boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("cursor source page should execute");
    let cursor_bytes = page1
        .next_cursor
        .as_ref()
        .expect("cursor source page should emit continuation cursor");
    let shared_boundary =
        decode_boundary(cursor_bytes.as_slice(), "cursor source token should decode");

    // Phase 3: execute page-2 parity checks with the same typed cursor boundary.
    let fast_page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .plan()
        .expect("fast page2 plan should build");
    let fast_page2 = load
        .execute_paged_with_cursor(fast_page2_plan, Some(shared_boundary.clone()))
        .expect("fast page2 should execute");

    let non_fast_page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .by_ids(keys.into_iter().map(Ulid::from_u128))
        .order_by("id")
        .limit(2)
        .plan()
        .expect("non-fast page2 plan should build");
    let non_fast_page2 = load
        .execute_paged_with_cursor(non_fast_page2_plan, Some(shared_boundary))
        .expect("non-fast page2 should execute");

    let fast_ids: Vec<Ulid> = ids_from_items(&fast_page2.items.0);
    let non_fast_ids: Vec<Ulid> = ids_from_items(&non_fast_page2.items.0);
    assert_eq!(
        fast_ids, non_fast_ids,
        "fast and non-fast paths must return identical rows for the same cursor boundary"
    );

    assert_eq!(
        fast_page2.next_cursor.is_some(),
        non_fast_page2.next_cursor.is_some(),
        "cursor presence must match between fast and non-fast paths"
    );

    let fast_next = fast_page2
        .next_cursor
        .as_ref()
        .expect("fast page2 should emit continuation cursor");
    let non_fast_next = non_fast_page2
        .next_cursor
        .as_ref()
        .expect("non-fast page2 should emit continuation cursor");
    let fast_next_boundary =
        decode_boundary(fast_next.as_slice(), "fast next cursor should decode");
    let non_fast_next_boundary = decode_boundary(
        non_fast_next.as_slice(),
        "non-fast next cursor should decode",
    );
    assert_eq!(
        &fast_next_boundary, &non_fast_next_boundary,
        "fast and non-fast paths must emit the same continuation boundary"
    );
}

#[test]
fn load_cursor_pagination_pk_order_key_range_respects_bounds() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1_u128, 2_u128, 3_u128, 4_u128, 5_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let mut page1_logical = LogicalPlan::<Ulid>::new(
        AccessPath::KeyRange {
            start: Ulid::from_u128(2),
            end: Ulid::from_u128(4),
        },
        ReadConsistency::MissingOk,
    );
    page1_logical.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    page1_logical.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let page1_plan = ExecutablePlan::<SimpleEntity>::new(page1_logical);
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("pk-range page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("pk-range page1 should execute");
    let page1_ids: Vec<Ulid> = ids_from_items(&page1.items.0);
    assert_eq!(page1_ids, vec![Ulid::from_u128(2), Ulid::from_u128(3)]);

    let cursor = page1
        .next_cursor
        .as_ref()
        .expect("pk-range page1 should emit continuation cursor");
    let mut page2_logical = LogicalPlan::<Ulid>::new(
        AccessPath::KeyRange {
            start: Ulid::from_u128(2),
            end: Ulid::from_u128(4),
        },
        ReadConsistency::MissingOk,
    );
    page2_logical.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    page2_logical.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let page2_plan = ExecutablePlan::<SimpleEntity>::new(page2_logical);
    let page2_boundary = page2_plan
        .plan_cursor(Some(cursor.as_slice()))
        .expect("pk-range page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("pk-range page2 should execute");
    let page2_ids: Vec<Ulid> = ids_from_items(&page2.items.0);
    assert_eq!(page2_ids, vec![Ulid::from_u128(4)]);
    assert!(
        page2.next_cursor.is_none(),
        "final pk-range page should not emit continuation cursor"
    );
}

#[test]
fn load_cursor_pagination_pk_order_key_range_cursor_past_end_returns_empty_page() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1_u128, 2_u128, 3_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let mut logical = LogicalPlan::<Ulid>::new(
        AccessPath::KeyRange {
            start: Ulid::from_u128(1),
            end: Ulid::from_u128(2),
        },
        ReadConsistency::MissingOk,
    );
    logical.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    logical.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let plan = ExecutablePlan::<SimpleEntity>::new(logical);
    let boundary = Some(CursorBoundary {
        slots: vec![CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(
            99,
        )))],
    });

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let page = load
        .execute_paged_with_cursor(plan, boundary)
        .expect("pk-range cursor past end should execute");

    assert!(
        page.items.0.is_empty(),
        "cursor beyond range end should produce an empty page"
    );
    assert!(
        page.next_cursor.is_none(),
        "empty page should not emit a continuation cursor"
    );
}

#[test]
fn load_cursor_pagination_pk_order_missing_slot_is_invariant_violation() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1_u128, 2_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(1)
        .plan()
        .expect("pk-order plan should build");

    let err = load
        .execute_paged_with_cursor(
            plan,
            Some(CursorBoundary {
                slots: vec![CursorBoundarySlot::Missing],
            }),
        )
        .expect_err("missing pk slot should be rejected by executor invariants");
    assert_eq!(
        err.class,
        ErrorClass::InvariantViolation,
        "missing pk slot should classify as invariant violation"
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Query,
        "missing pk slot should originate from query invariant checks"
    );
    assert!(
        err.message.contains("pk cursor slot must be present"),
        "missing pk slot should return a clear invariant message: {err:?}"
    );
}

#[test]
fn load_cursor_pagination_pk_order_type_mismatch_is_invariant_violation() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1_u128, 2_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(1)
        .plan()
        .expect("pk-order plan should build");

    let err = load
        .execute_paged_with_cursor(
            plan,
            Some(CursorBoundary {
                slots: vec![CursorBoundarySlot::Present(Value::Text(
                    "not-a-ulid".to_string(),
                ))],
            }),
        )
        .expect_err("pk slot type mismatch should be rejected by executor invariants");
    assert_eq!(
        err.class,
        ErrorClass::InvariantViolation,
        "pk slot mismatch should classify as invariant violation"
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Query,
        "pk slot mismatch should originate from query invariant checks"
    );
    assert!(
        err.message.contains("pk cursor slot type mismatch"),
        "pk slot mismatch should return a clear invariant message: {err:?}"
    );
}

#[test]
fn load_cursor_pagination_pk_order_arity_mismatch_is_invariant_violation() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1_u128, 2_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(1)
        .plan()
        .expect("pk-order plan should build");

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
        .expect_err("pk slot arity mismatch should be rejected by executor invariants");
    assert_eq!(
        err.class,
        ErrorClass::InvariantViolation,
        "pk slot arity mismatch should classify as invariant violation"
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Query,
        "pk slot arity mismatch should originate from query invariant checks"
    );
    assert!(
        err.message
            .contains("pk-ordered continuation boundary must contain exactly 1 slot"),
        "pk slot arity mismatch should return a clear invariant message: {err:?}"
    );
}

#[test]
fn load_cursor_pagination_skips_strictly_before_limit() {
    setup_pagination_test();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
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
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let page1_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("cursor page1 plan should build");
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("cursor page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("cursor page1 should execute");
    assert_eq!(page1.items.0.len(), 1, "page1 should return one row");
    assert_eq!(page1.items.0[0].1.id, Ulid::from_u128(1100));

    let cursor1 = page1
        .next_cursor
        .as_ref()
        .expect("page1 should emit a continuation cursor");
    let page2_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("cursor page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor(Some(cursor1.as_slice()))
        .expect("cursor page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("cursor page2 should execute");
    assert_eq!(page2.items.0.len(), 1, "page2 should return one row");
    assert_eq!(
        page2.items.0[0].1.id,
        Ulid::from_u128(1101),
        "cursor boundary must be applied before limit using strict ordering"
    );

    let cursor2 = page2
        .next_cursor
        .as_ref()
        .expect("page2 should emit a continuation cursor");
    let page3_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("cursor page3 plan should build");
    let page3_boundary = page3_plan
        .plan_cursor(Some(cursor2.as_slice()))
        .expect("cursor page3 boundary should plan");
    let page3 = load
        .execute_paged_with_cursor(page3_plan, page3_boundary)
        .expect("cursor page3 should execute");
    assert_eq!(page3.items.0.len(), 1, "page3 should return one row");
    assert_eq!(
        page3.items.0[0].1.id,
        Ulid::from_u128(1102),
        "strict cursor continuation must advance beyond the last returned row"
    );
}

#[test]
fn load_cursor_next_cursor_uses_last_returned_row_boundary() {
    setup_pagination_test();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
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
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let page1_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("cursor next-cursor plan should build");
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("cursor page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("cursor page1 should execute");
    assert_eq!(page1.items.0.len(), 2, "page1 should return two rows");
    assert_eq!(page1.items.0[0].1.id, Ulid::from_u128(1200));
    assert_eq!(
        page1.items.0[1].1.id,
        Ulid::from_u128(1201),
        "page1 second row should be the PK tie-break winner for rank=20"
    );

    let cursor_bytes = page1
        .next_cursor
        .as_ref()
        .expect("page1 should include next cursor");
    let token = ContinuationToken::decode(cursor_bytes.as_slice())
        .expect("continuation cursor should decode");
    let comparison_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("comparison plan should build")
        .into_inner();
    let expected_boundary = comparison_plan
        .cursor_boundary_from_entity(&page1.items.0[1].1)
        .expect("expected boundary should build");
    assert_eq!(
        token.boundary(),
        &expected_boundary,
        "next cursor must encode the last returned row boundary"
    );

    let page2_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("cursor page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor(Some(cursor_bytes.as_slice()))
        .expect("cursor page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("cursor page2 should execute");
    let page2_ids: Vec<Ulid> = ids_from_items(&page2.items.0);
    assert_eq!(
        page2_ids,
        vec![Ulid::from_u128(1202), Ulid::from_u128(1203)],
        "page2 should resume strictly after page1's final row"
    );
    assert!(
        page2.next_cursor.is_none(),
        "final page should not emit a continuation cursor"
    );
}

#[test]
fn load_cursor_pagination_desc_order_resumes_strictly_after_boundary() {
    setup_pagination_test();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
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
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let page1_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("descending page1 plan should build");
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("descending page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("descending page1 should execute");
    let page1_ids: Vec<Ulid> = ids_from_items(&page1.items.0);
    assert_eq!(
        page1_ids,
        vec![Ulid::from_u128(1403), Ulid::from_u128(1401)],
        "descending page1 should apply rank DESC then canonical PK tie-break"
    );

    let cursor = page1
        .next_cursor
        .as_ref()
        .expect("descending page1 should emit continuation cursor");
    let page2_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("descending page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor(Some(cursor.as_slice()))
        .expect("descending page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("descending page2 should execute");
    let page2_ids: Vec<Ulid> = ids_from_items(&page2.items.0);
    assert_eq!(
        page2_ids,
        vec![Ulid::from_u128(1402), Ulid::from_u128(1400)],
        "descending continuation must resume strictly after the boundary row"
    );
    assert!(
        page2.next_cursor.is_none(),
        "final descending page should not emit a continuation cursor"
    );
}

#[test]
fn load_desc_order_uses_primary_key_tie_break_for_equal_rank_rows() {
    setup_pagination_test();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
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
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("rank")
        .limit(4)
        .plan()
        .expect("descending tie-break plan should build");
    let page = load
        .execute_paged_with_cursor(plan, None)
        .expect("descending tie-break page should execute");
    let page_ids: Vec<Ulid> = ids_from_items(&page.items.0);

    assert_eq!(
        page_ids,
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

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
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
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let asc_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("ascending cursor plan should build");
    let asc_boundary = asc_plan
        .plan_cursor(None)
        .expect("ascending boundary should plan");
    let asc_page = load
        .execute_paged_with_cursor(asc_plan, asc_boundary)
        .expect("ascending cursor page should execute");
    let cursor = asc_page
        .next_cursor
        .expect("ascending page should emit cursor");

    let desc_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("rank")
        .limit(1)
        .plan()
        .expect("descending plan should build");
    let err = desc_plan
        .plan_cursor(Some(cursor.as_slice()))
        .expect_err("cursor from different canonical plan should be rejected");
    assert!(
        matches!(
            err,
            PlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::CursorPlanError::ContinuationCursorSignatureMismatch { .. }
                )
        ),
        "planning should reject plan-signature mismatch"
    );
}

#[test]
fn load_index_pushdown_eligible_order_matches_index_scan_order() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group8(10_000);
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .explain()
        .expect("parity explain should build");
    assert!(
        matches!(
            explain.order_pushdown,
            ExplainOrderPushdown::EligibleSecondaryIndex {
                index,
                prefix_len
            } if index == PUSHDOWN_PARITY_INDEX_MODELS[0].name && prefix_len == 1
        ),
        "query shape should be pushdown-eligible for group+rank index traversal"
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .plan()
        .expect("parity load plan should build");
    let response = load.execute(plan).expect("parity load should execute");
    let actual_ids: Vec<Ulid> = ids_from_items(&response.0);

    let expected_ids = ordered_ids_from_group_rank_index(7);
    assert_eq!(
        actual_ids, expected_ids,
        "fallback post-access ordering must match canonical index traversal order for eligible plans"
    );
}

#[test]
fn load_index_pushdown_eligible_paged_results_match_index_scan_window() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(11_000);
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let page1_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("page1 parity plan should build");
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("page1 parity boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("page1 parity should execute");
    let page1_ids: Vec<Ulid> = ids_from_items(&page1.items.0);

    let expected_all = ordered_ids_from_group_rank_index(7);
    let expected_page1: Vec<Ulid> = expected_all.iter().copied().take(2).collect();
    assert_eq!(
        page1_ids, expected_page1,
        "page1 fallback output must match the canonical index-order window"
    );

    let page2_cursor = page1
        .next_cursor
        .as_ref()
        .expect("page1 parity should emit continuation cursor")
        .clone();
    let page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("page2 parity plan should build");
    let page2_boundary = page2_plan
        .plan_cursor(Some(page2_cursor.as_slice()))
        .expect("page2 parity boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("page2 parity should execute");
    let page2_ids: Vec<Ulid> = ids_from_items(&page2.items.0);

    let expected_page2: Vec<Ulid> = expected_all.iter().copied().skip(2).take(2).collect();
    assert_eq!(
        page2_ids, expected_page2,
        "page2 fallback continuation must match the canonical index-order window"
    );
}

#[test]
fn load_index_pushdown_and_fallback_emit_equivalent_cursor_boundaries() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(12_000);
    seed_pushdown_rows(&rows);
    let group7_ids = pushdown_group_ids(&rows, 7);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let pushdown_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("pushdown plan should build");
    let pushdown_page = load
        .execute_paged_with_cursor(pushdown_plan, None)
        .expect("pushdown page should execute");

    let fallback_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .by_ids(group7_ids.iter().copied())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("fallback plan should build");
    let fallback_page = load
        .execute_paged_with_cursor(fallback_plan, None)
        .expect("fallback page should execute");

    let pushdown_ids: Vec<Ulid> = ids_from_items(&pushdown_page.items.0);
    let fallback_ids: Vec<Ulid> = ids_from_items(&fallback_page.items.0);
    assert_eq!(
        pushdown_ids, fallback_ids,
        "pushdown and fallback page windows should match"
    );

    let pushdown_cursor = pushdown_page
        .next_cursor
        .as_ref()
        .expect("pushdown page should emit continuation cursor");
    let fallback_cursor = fallback_page
        .next_cursor
        .as_ref()
        .expect("fallback page should emit continuation cursor");
    let pushdown_boundary =
        decode_boundary(pushdown_cursor.as_slice(), "pushdown cursor should decode");
    let fallback_boundary =
        decode_boundary(fallback_cursor.as_slice(), "fallback cursor should decode");
    assert_eq!(
        &pushdown_boundary, &fallback_boundary,
        "pushdown and fallback cursors should encode the same continuation boundary"
    );
}

#[test]
fn load_index_pushdown_and_fallback_resume_equivalently_from_shared_boundary() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(13_000);
    seed_pushdown_rows(&rows);
    let group7_ids = pushdown_group_ids(&rows, 7);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let seed_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("seed plan should build");
    let seed_page = load
        .execute_paged_with_cursor(seed_plan, None)
        .expect("seed page should execute");
    let seed_cursor = seed_page
        .next_cursor
        .as_ref()
        .expect("seed page should emit continuation cursor");
    let shared_boundary = decode_boundary(seed_cursor.as_slice(), "seed cursor should decode");

    let pushdown_page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("pushdown page2 plan should build");
    let pushdown_page2 = load
        .execute_paged_with_cursor(pushdown_page2_plan, Some(shared_boundary.clone()))
        .expect("pushdown page2 should execute");

    let fallback_page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .by_ids(group7_ids.iter().copied())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("fallback page2 plan should build");
    let fallback_page2 = load
        .execute_paged_with_cursor(fallback_page2_plan, Some(shared_boundary))
        .expect("fallback page2 should execute");

    let pushdown_page2_ids: Vec<Ulid> = ids_from_items(&pushdown_page2.items.0);
    let fallback_page2_ids: Vec<Ulid> = ids_from_items(&fallback_page2.items.0);
    assert_eq!(
        pushdown_page2_ids, fallback_page2_ids,
        "pushdown and fallback should return the same rows for a shared continuation boundary"
    );

    let pushdown_next = pushdown_page2
        .next_cursor
        .as_ref()
        .expect("pushdown page2 should emit continuation cursor");
    let fallback_next = fallback_page2
        .next_cursor
        .as_ref()
        .expect("fallback page2 should emit continuation cursor");
    let pushdown_next_boundary =
        decode_boundary(pushdown_next.as_slice(), "pushdown next should decode");
    let fallback_next_boundary =
        decode_boundary(fallback_next.as_slice(), "fallback next should decode");
    assert_eq!(
        &pushdown_next_boundary, &fallback_next_boundary,
        "pushdown and fallback page2 cursors should encode identical boundaries"
    );
}

#[test]
fn load_index_desc_order_with_ties_matches_for_index_and_by_ids_paths() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(14_000);
    seed_pushdown_rows(&rows);
    let group7_ids = pushdown_group_ids(&rows, 7);

    let predicate = pushdown_group_predicate(7);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("rank")
        .explain()
        .expect("desc explain should build");
    assert!(
        matches!(
            explain.order_pushdown,
            ExplainOrderPushdown::Matrix(
                SecondaryOrderPushdownRejection::NonAscendingDirection { field }
            ) if field == "rank"
        ),
        "descending rank order should be ineligible and use fallback execution"
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let index_path_page1_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("index-path desc page1 plan should build");
    let index_path_page1 = load
        .execute_paged_with_cursor(index_path_page1_plan, None)
        .expect("index-path desc page1 should execute");

    let by_ids_page1_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .by_ids(group7_ids.iter().copied())
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("by-ids desc page1 plan should build");
    let by_ids_page1 = load
        .execute_paged_with_cursor(by_ids_page1_plan, None)
        .expect("by-ids desc page1 should execute");

    let index_path_page1_ids: Vec<Ulid> = ids_from_items(&index_path_page1.items.0);
    let by_ids_page1_ids: Vec<Ulid> = ids_from_items(&by_ids_page1.items.0);
    assert_eq!(
        index_path_page1_ids, by_ids_page1_ids,
        "descending page1 should match across index-prefix and by-ids paths"
    );

    let shared_boundary = decode_boundary(
        index_path_page1
            .next_cursor
            .as_ref()
            .expect("index-path desc page1 should emit cursor")
            .as_slice(),
        "index-path desc cursor should decode",
    );
    let index_path_page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("index-path desc page2 plan should build");
    let index_path_page2 = load
        .execute_paged_with_cursor(index_path_page2_plan, Some(shared_boundary.clone()))
        .expect("index-path desc page2 should execute");

    let by_ids_page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .by_ids(group7_ids.iter().copied())
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("by-ids desc page2 plan should build");
    let by_ids_page2 = load
        .execute_paged_with_cursor(by_ids_page2_plan, Some(shared_boundary))
        .expect("by-ids desc page2 should execute");

    let index_path_page2_ids: Vec<Ulid> = ids_from_items(&index_path_page2.items.0);
    let by_ids_page2_ids: Vec<Ulid> = ids_from_items(&by_ids_page2.items.0);
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

    let page1_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("prefix window page1 plan should build");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, None)
        .expect("prefix window page1 should execute");

    let page1_cursor = page1
        .next_cursor
        .as_ref()
        .expect("prefix window page1 should emit continuation cursor");
    let page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("prefix window page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor(Some(page1_cursor.as_slice()))
        .expect("prefix window page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("prefix window page2 should execute");
    assert_eq!(page2.items.0.len(), 1, "page2 should return final row only");
    assert!(
        page2.next_cursor.is_none(),
        "final prefix window page should not emit continuation cursor"
    );

    let plan_for_boundary = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("prefix window boundary plan should build");
    let explicit_boundary = plan_for_boundary
        .into_inner()
        .cursor_boundary_from_entity(&page2.items.0[0].1)
        .expect("explicit boundary from terminal row should build");
    let past_end_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(pushdown_group_predicate(7))
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("past-end plan should build");
    let past_end = load
        .execute_paged_with_cursor(past_end_plan, Some(explicit_boundary))
        .expect("past-end execution should succeed");
    assert!(
        past_end.items.0.is_empty(),
        "cursor boundary at final prefix row should yield an empty continuation page"
    );
    assert!(
        past_end.next_cursor.is_none(),
        "empty continuation page should not emit a cursor"
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
    let explain = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("tag")
        .explain()
        .expect("single-field range explain should build");
    assert!(
        explain_contains_index_range(&explain.access, INDEXED_METRICS_INDEX_MODELS[0].name, 0),
        "single-field range should plan an IndexRange access path"
    );

    let fallback_ids = indexed_metrics_ids_in_tag_range(&rows, 10, 30);
    assert_pushdown_parity(
        || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
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
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .explain()
        .expect("composite range explain should build");
    assert!(
        explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
        "composite prefix+range should plan an IndexRange access path"
    );

    let fallback_ids = pushdown_ids_in_group_rank_range(&rows, 7, 10, 30);
    assert_pushdown_parity(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
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

    // Phase 1: seed unique range values so ASC and DESC are strict inverses.
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

    // Phase 2: verify the surface still plans as IndexRange.
    let explain = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("tag")
        .explain()
        .expect("single-field asc explain should build");
    assert!(
        explain_contains_index_range(&explain.access, INDEXED_METRICS_INDEX_MODELS[0].name, 0),
        "single-field asc query should plan an IndexRange access path"
    );

    // Phase 3: assert full-result directional symmetry.
    let asc = load
        .execute(
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("tag")
                .plan()
                .expect("single-field asc plan should build"),
        )
        .expect("single-field asc execution should succeed");
    let desc = load
        .execute(
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate)
                .order_by_desc("tag")
                .plan()
                .expect("single-field desc plan should build"),
        )
        .expect("single-field desc execution should succeed");

    let mut asc_ids = ids_from_items(&asc.0);
    asc_ids.reverse();

    assert_eq!(
        asc_ids,
        ids_from_items(&desc.0),
        "full DESC result stream should match reversed full ASC result stream"
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
    let explain = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("tag")
        .limit(2)
        .explain()
        .expect("single-field limit matrix explain should build");
    assert!(
        explain_contains_index_range(&explain.access, INDEXED_METRICS_INDEX_MODELS[0].name, 0),
        "single-field limit matrix should plan an IndexRange access path"
    );

    assert_limit_matrix(
        || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
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
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .explain()
        .expect("composite limit matrix explain should build");
    assert!(
        explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
        "composite limit matrix should plan an IndexRange access path"
    );

    assert_limit_matrix(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
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
    let page_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("tag")
        .limit(4)
        .plan()
        .expect("single-field exact-size page plan should build");
    let planned_cursor = page_plan
        .plan_cursor(None)
        .expect("single-field exact-size cursor should plan");
    let page = load
        .execute_paged_with_cursor(page_plan, planned_cursor)
        .expect("single-field exact-size page should execute");

    let page_ids: Vec<Ulid> = ids_from_items(&page.items.0);
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

        let page_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(predicate.clone())
            .order_by("rank")
            .limit(3)
            .plan()
            .expect("composite terminal-page plan should build");
        let planned_cursor = page_plan
            .plan_cursor(cursor.as_deref())
            .expect("composite terminal-page cursor should plan");
        let page = load
            .execute_paged_with_cursor(page_plan, planned_cursor)
            .expect("composite terminal-page execution should succeed");

        page_sizes.push(page.items.0.len());

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
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

    let mut logical = LogicalPlan::new(
        AccessPath::IndexRange {
            index: INDEXED_METRICS_INDEX_MODELS[0],
            prefix: Vec::new(),
            lower: Bound::Included(Value::Uint(10)),
            upper: Bound::Excluded(Value::Uint(30)),
        },
        ReadConsistency::MissingOk,
    );
    logical.order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    logical.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let page_plan = ExecutablePlan::<IndexedMetricsEntity>::new(logical);

    let _ = take_trace_events();
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false).with_trace(&TEST_TRACE_SINK);
    let _page = load
        .execute_paged_with_cursor(page_plan, None)
        .expect("trace limit-pushdown execution should succeed");
    let events = take_trace_events();

    let access_rows = extract_access_rows(&events);

    assert_eq!(
        access_rows,
        Some(3),
        "limit=2 index-range pushdown should scan only offset+limit+1 rows in access phase"
    );
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

    let mut logical = LogicalPlan::new(
        AccessPath::IndexRange {
            index: INDEXED_METRICS_INDEX_MODELS[0],
            prefix: Vec::new(),
            lower: Bound::Included(Value::Uint(10)),
            upper: Bound::Excluded(Value::Uint(30)),
        },
        ReadConsistency::MissingOk,
    );
    logical.order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    logical.page = Some(PageSpec {
        limit: Some(0),
        offset: 0,
    });
    let page_plan = ExecutablePlan::<IndexedMetricsEntity>::new(logical);

    let _ = take_trace_events();
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false).with_trace(&TEST_TRACE_SINK);
    let page = load
        .execute_paged_with_cursor(page_plan, None)
        .expect("limit=0 trace execution should succeed");
    let events = take_trace_events();

    let access_rows = extract_access_rows(&events);

    assert_eq!(
        access_rows,
        Some(0),
        "limit=0 index-range pushdown should not scan access rows"
    );
    assert!(
        page.items.0.is_empty(),
        "limit=0 should return an empty page for eligible index-range plans"
    );
    assert!(
        page.next_cursor.is_none(),
        "limit=0 should not emit a continuation cursor"
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

    let mut logical = LogicalPlan::new(
        AccessPath::IndexRange {
            index: INDEXED_METRICS_INDEX_MODELS[0],
            prefix: Vec::new(),
            lower: Bound::Included(Value::Uint(10)),
            upper: Bound::Excluded(Value::Uint(30)),
        },
        ReadConsistency::MissingOk,
    );
    logical.order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    logical.page = Some(PageSpec {
        limit: Some(0),
        offset: 2,
    });
    let page_plan = ExecutablePlan::<IndexedMetricsEntity>::new(logical);

    let _ = take_trace_events();
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false).with_trace(&TEST_TRACE_SINK);
    let page = load
        .execute_paged_with_cursor(page_plan, None)
        .expect("limit=0 with offset trace execution should succeed");
    let events = take_trace_events();

    let access_rows = extract_access_rows(&events);

    assert_eq!(
        access_rows,
        Some(0),
        "limit=0 should short-circuit access scanning even when offset is non-zero"
    );
    assert!(
        page.items.0.is_empty(),
        "limit=0 with offset should return an empty page for eligible index-range plans"
    );
    assert!(
        page.next_cursor.is_none(),
        "limit=0 with offset should not emit a continuation cursor"
    );
}

#[test]
fn load_single_field_between_equivalent_pushdown_matches_by_ids_fallback() {
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

    let predicate = tag_between_equivalent_predicate(10, 30);
    let explain = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("tag")
        .explain()
        .expect("single-field between-equivalent explain should build");
    assert!(
        explain_contains_index_range(&explain.access, INDEXED_METRICS_INDEX_MODELS[0].name, 0),
        "single-field between-equivalent predicate should plan an IndexRange access path"
    );

    let fallback_ids = indexed_metrics_ids_in_between_equivalent_range(&rows, 10, 30);
    assert_pushdown_parity(
        || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("tag")
        },
        fallback_ids,
        |query| query.order_by("tag"),
    );
}

#[test]
fn load_composite_between_equivalent_pushdown_matches_by_ids_fallback() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(19_200);
    seed_pushdown_rows(&rows);

    let predicate = group_rank_between_equivalent_predicate(7, 10, 30);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .explain()
        .expect("composite between-equivalent explain should build");
    assert!(
        explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
        "composite between-equivalent predicate should plan an IndexRange access path"
    );

    let fallback_ids = pushdown_ids_in_group_rank_between_equivalent_range(&rows, 7, 10, 30);
    assert_pushdown_parity(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("rank")
        },
        fallback_ids,
        |query| query.order_by("rank"),
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

    // Phase 1: exclusive upper bound should exclude the max-value group.
    let exclusive_predicate = tag_range_predicate(0, MAX_TAG);
    let explain = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(exclusive_predicate.clone())
        .order_by("tag")
        .explain()
        .expect("single-field extreme-edge explain should build");
    assert!(
        explain_contains_index_range(&explain.access, INDEXED_METRICS_INDEX_MODELS[0].name, 0),
        "single-field extreme-edge range should plan an IndexRange access path"
    );

    let fallback_exclusive_ids = indexed_metrics_ids_in_tag_range(&rows, 0, MAX_TAG);
    assert_pushdown_parity(
        || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(exclusive_predicate.clone())
                .order_by("tag")
        },
        fallback_exclusive_ids,
        |query| query.order_by("tag"),
    );

    // Phase 2: inclusive upper bound should include the max-value group.
    let inclusive_predicate = tag_between_equivalent_predicate(0, MAX_TAG);
    let fallback_inclusive_ids = indexed_metrics_ids_in_between_equivalent_range(&rows, 0, MAX_TAG);
    assert_pushdown_parity(
        || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(inclusive_predicate.clone())
                .order_by("tag")
        },
        fallback_inclusive_ids.iter().copied(),
        |query| query.order_by("tag"),
    );

    let pushdown_inclusive_has_max = load
        .execute(
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(inclusive_predicate)
                .order_by("tag")
                .plan()
                .expect("single-field extreme-edge inclusive plan should build"),
        )
        .expect("single-field extreme-edge inclusive pushdown should execute")
        .0
        .iter()
        .any(|(_, entity)| entity.id == Ulid::from_u128(19_305));
    assert!(
        pushdown_inclusive_has_max,
        "inclusive upper-bound range must include rows at the max field value"
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

    // Phase 1: exclusive upper bound should exclude the max-value rank group.
    let exclusive_predicate = group_rank_range_predicate(7, 0, MAX_RANK);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(exclusive_predicate.clone())
        .order_by("rank")
        .explain()
        .expect("composite extreme-edge explain should build");
    assert!(
        explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
        "composite extreme-edge range should plan an IndexRange access path"
    );

    let fallback_exclusive_ids = pushdown_ids_in_group_rank_range(&rows, 7, 0, MAX_RANK);
    assert_pushdown_parity(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(exclusive_predicate.clone())
                .order_by("rank")
        },
        fallback_exclusive_ids,
        |query| query.order_by("rank"),
    );

    // Phase 2: inclusive upper bound should include the max-value rank group.
    let inclusive_predicate = group_rank_between_equivalent_predicate(7, 0, MAX_RANK);
    let fallback_inclusive_ids =
        pushdown_ids_in_group_rank_between_equivalent_range(&rows, 7, 0, MAX_RANK);
    assert_pushdown_parity(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(inclusive_predicate.clone())
                .order_by("rank")
        },
        fallback_inclusive_ids.iter().copied(),
        |query| query.order_by("rank"),
    );

    let pushdown_inclusive_has_max = load
        .execute(
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(inclusive_predicate)
                .order_by("rank")
                .plan()
                .expect("composite extreme-edge inclusive plan should build"),
        )
        .expect("composite extreme-edge inclusive pushdown should execute")
        .0
        .iter()
        .any(|(_, entity)| entity.id == Ulid::from_u128(19_405));
    assert!(
        pushdown_inclusive_has_max,
        "inclusive upper-bound range must include rows at the max field value"
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
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .explain()
        .expect("composite range pagination explain should build");
    assert!(
        explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
        "composite range pagination should plan an IndexRange access path"
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let (pushdown_ids, _) = collect_all_pages(
        &load,
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("rank")
                .limit(2)
        },
        8,
    );

    let fallback_seed_ids = pushdown_ids_in_group_rank_range(&rows, 7, 10, 40);
    let (fallback_ids, _) = collect_all_pages(
        &load,
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .by_ids(fallback_seed_ids.iter().copied())
                .order_by("rank")
                .limit(2)
        },
        8,
    );

    assert_eq!(
        pushdown_ids, fallback_ids,
        "composite range cursor pagination should match fallback across all pages"
    );
    let unique_pushdown_ids: BTreeSet<Ulid> = pushdown_ids.iter().copied().collect();
    assert_eq!(
        unique_pushdown_ids.len(),
        pushdown_ids.len(),
        "composite range cursor pagination must not emit duplicate rows"
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
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(3)
        .explain()
        .expect("composite range monotonicity explain should build");
    assert!(
        explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
        "composite range monotonicity test should plan an IndexRange access path"
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    // Baseline: one unbounded execution for the exact same predicate + order.
    let unbounded_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .plan()
        .expect("unbounded plan should build");
    let unbounded = load
        .execute(unbounded_plan)
        .expect("unbounded execution should succeed");
    let unbounded_ids: Vec<Ulid> = ids_from_items(&unbounded.0);
    let unbounded_row_bytes: Vec<Vec<u8>> = unbounded
        .0
        .iter()
        .map(|(_, entity)| serialize(entity).expect("entity serialization should succeed"))
        .collect();

    let (paged_ids, paged_row_bytes) = collect_all_pages(
        &load,
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("rank")
                .limit(3)
        },
        8,
    );

    // Anchor monotonicity: preserve explicit cursor-shape validation across pages.
    let mut cursor: Option<Vec<u8>> = None;
    let mut page_anchors = Vec::new();
    let mut pages = 0usize;

    loop {
        pages = pages.saturating_add(1);
        assert!(
            pages <= 8,
            "composite range pagination should terminate in bounded pages"
        );

        let page_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(predicate.clone())
            .order_by("rank")
            .limit(3)
            .plan()
            .expect("page plan should build");
        let planned_cursor = page_plan
            .plan_cursor(cursor.as_deref())
            .expect("page cursor should plan");
        let page = load
            .execute_paged_with_cursor(page_plan, planned_cursor)
            .expect("paged execution should succeed");

        let Some(next_cursor) = page.next_cursor else {
            break;
        };

        assert_anchor_monotonic(
            &mut page_anchors,
            next_cursor.as_slice(),
            "continuation cursor should decode",
            "index-range cursor should include a raw-key anchor",
            "index-range continuation anchors must progress strictly monotonically",
        );
        cursor = Some(next_cursor);
    }

    assert!(
        page_anchors.len() >= 2,
        "fixture should produce at least two continuation anchors"
    );
    assert_eq!(
        paged_ids, unbounded_ids,
        "concatenated paginated ids must match unbounded ids in the same order"
    );
    assert_eq!(
        paged_row_bytes, unbounded_row_bytes,
        "concatenated paginated rows must be byte-for-byte identical to the unbounded result set"
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

    let predicate = unique_code_range_predicate(10, 60);
    let explain = Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("code")
        .limit(2)
        .explain()
        .expect("unique index-range explain should build");
    assert!(
        explain_contains_index_range(&explain.access, UNIQUE_INDEX_RANGE_INDEX_MODELS[0].name, 0),
        "unique index-range continuation should plan an IndexRange access path"
    );

    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    let unbounded_plan = Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("code")
        .plan()
        .expect("unique unbounded plan should build");
    let unbounded = load
        .execute(unbounded_plan)
        .expect("unique unbounded execution should succeed");
    let unbounded_ids: Vec<Ulid> = ids_from_items(&unbounded.0);
    let unbounded_row_bytes: Vec<Vec<u8>> = unbounded
        .0
        .iter()
        .map(|(_, entity)| serialize(entity).expect("entity serialization should succeed"))
        .collect();

    let (paged_ids, paged_row_bytes) = collect_all_pages(
        &load,
        || {
            Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("code")
                .limit(2)
        },
        8,
    );

    let mut cursor: Option<Vec<u8>> = None;
    let mut anchors = Vec::new();
    let mut pages = 0usize;

    loop {
        pages = pages.saturating_add(1);
        assert!(pages <= 8, "unique index-range pagination should terminate");

        let page_plan = Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
            .filter(predicate.clone())
            .order_by("code")
            .limit(2)
            .plan()
            .expect("unique page plan should build");
        let planned_cursor = page_plan
            .plan_cursor(cursor.as_deref())
            .expect("unique page cursor should plan");
        let page = load
            .execute_paged_with_cursor(page_plan, planned_cursor)
            .expect("unique paged execution should succeed");

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        assert_anchor_monotonic(
            &mut anchors,
            next_cursor.as_slice(),
            "unique continuation cursor should decode",
            "unique index-range cursor should include a raw-key anchor",
            "unique index-range continuation anchors must advance strictly",
        );
        cursor = Some(next_cursor);
    }

    let unique_paged_ids: BTreeSet<Ulid> = paged_ids.iter().copied().collect();
    assert_eq!(
        unique_paged_ids.len(),
        paged_ids.len(),
        "unique index-range pagination must not emit duplicate rows"
    );
    assert_eq!(
        paged_ids, unbounded_ids,
        "unique index-range paginated ids must match unbounded ids in order"
    );
    assert_eq!(
        paged_row_bytes, unbounded_row_bytes,
        "unique index-range paginated rows must match unbounded rows byte-for-byte"
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

    let predicate = tag_range_predicate(10, 30);
    let explain = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("tag")
        .limit(10)
        .explain()
        .expect("single-field range boundary explain should build");
    assert!(
        explain_contains_index_range(&explain.access, INDEXED_METRICS_INDEX_MODELS[0].name, 0),
        "single-field range boundary test should plan an IndexRange access path"
    );

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);
    let base_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("tag")
        .limit(10)
        .plan()
        .expect("single-field base plan should build");
    let base_page = load
        .execute_paged_with_cursor(base_plan, None)
        .expect("single-field base page should execute");
    let all_ids: Vec<Ulid> = ids_from_items(&base_page.items.0);
    assert_eq!(
        all_ids.len(),
        4,
        "single-field range should include only rows in [10, 30)"
    );

    let first_entity = &base_page.items.0[0].1;
    assert_resume_after_entity(
        || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("tag")
                .limit(10)
        },
        first_entity,
        all_ids[1..].to_vec(),
    );

    let terminal_entity = &base_page.items.0[base_page.items.0.len() - 1].1;
    let terminal_boundary_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("tag")
        .limit(10)
        .plan()
        .expect("single-field terminal-boundary plan should build");
    let terminal_boundary = terminal_boundary_plan
        .into_inner()
        .cursor_boundary_from_entity(terminal_entity)
        .expect("single-field terminal boundary should build");
    let past_end_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(tag_range_predicate(10, 30))
        .order_by("tag")
        .limit(10)
        .plan()
        .expect("single-field past-end plan should build");
    let past_end = load
        .execute_paged_with_cursor(past_end_plan, Some(terminal_boundary))
        .expect("single-field past-end page should execute");
    assert!(
        past_end.items.0.is_empty(),
        "cursor boundary at the upper edge row should return an empty continuation page"
    );
    assert!(
        past_end.next_cursor.is_none(),
        "single-field empty continuation page should not emit a cursor"
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

    let predicate = tag_between_equivalent_predicate(10, 50);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);

    let page1_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("tag")
        .limit(1)
        .plan()
        .expect("single-field desc upper-anchor page1 plan should build");
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("single-field desc upper-anchor page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("single-field desc upper-anchor page1 should execute");
    assert_eq!(
        ids_from_items(&page1.items.0),
        vec![Ulid::from_u128(21_105)],
        "descending first page should start at the upper envelope row"
    );

    let cursor = page1
        .next_cursor
        .as_ref()
        .expect("single-field desc upper-anchor page1 should emit continuation cursor");
    let resume_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by_desc("tag")
        .limit(10)
        .plan()
        .expect("single-field desc upper-anchor resume plan should build");
    let resume_boundary = resume_plan
        .plan_cursor(Some(cursor.as_slice()))
        .expect("single-field desc upper-anchor resume boundary should plan");
    let resume = load
        .execute_paged_with_cursor(resume_plan, resume_boundary)
        .expect("single-field desc upper-anchor resume should execute");

    assert_eq!(
        ids_from_items(&resume.items.0),
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
fn load_single_field_desc_range_resume_from_lower_boundary_returns_empty() {
    setup_pagination_test();

    let rows = [
        (21_201, 10, "t10"),
        (21_202, 20, "t20"),
        (21_203, 30, "t30"),
    ];
    seed_indexed_metrics_rows(&rows);

    let predicate = tag_between_equivalent_predicate(10, 30);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);
    let base_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("tag")
        .limit(10)
        .plan()
        .expect("single-field desc lower-boundary base plan should build");
    let base_page = load
        .execute_paged_with_cursor(base_plan, None)
        .expect("single-field desc lower-boundary base page should execute");

    let terminal_entity = &base_page.items.0[base_page.items.0.len() - 1].1;
    let terminal_boundary_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by_desc("tag")
        .limit(10)
        .plan()
        .expect("single-field desc lower-boundary plan should build");
    let terminal_boundary = terminal_boundary_plan
        .into_inner()
        .cursor_boundary_from_entity(terminal_entity)
        .expect("single-field desc lower-boundary should build");

    let resume_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(tag_between_equivalent_predicate(10, 30))
        .order_by_desc("tag")
        .limit(10)
        .plan()
        .expect("single-field desc lower-boundary resume plan should build");
    let resume = load
        .execute_paged_with_cursor(resume_plan, Some(terminal_boundary))
        .expect("single-field desc lower-boundary resume should execute");

    assert!(
        resume.items.0.is_empty(),
        "descending resume from the lower boundary row must return an empty page"
    );
    assert!(
        resume.next_cursor.is_none(),
        "empty descending continuation page must not emit a cursor"
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

    let predicate = tag_between_equivalent_predicate(30, 30);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);
    let page1_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("tag")
        .limit(1)
        .plan()
        .expect("single-element desc page1 plan should build");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, None)
        .expect("single-element desc page1 should execute");
    assert_eq!(
        ids_from_items(&page1.items.0),
        vec![Ulid::from_u128(21_302)],
        "single-element descending range should return the only row"
    );
    assert!(
        page1.next_cursor.is_none(),
        "single-element descending first page should not emit a cursor"
    );

    let boundary_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by_desc("tag")
        .limit(1)
        .plan()
        .expect("single-element desc boundary plan should build");
    let boundary = boundary_plan
        .into_inner()
        .cursor_boundary_from_entity(&page1.items.0[0].1)
        .expect("single-element desc boundary should build");
    let resume_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(tag_between_equivalent_predicate(30, 30))
        .order_by_desc("tag")
        .limit(1)
        .plan()
        .expect("single-element desc resume plan should build");
    let resume = load
        .execute_paged_with_cursor(resume_plan, Some(boundary))
        .expect("single-element desc resume should execute");

    assert!(
        resume.items.0.is_empty(),
        "resuming a single-element descending range must return an empty page"
    );
    assert!(
        resume.next_cursor.is_none(),
        "single-element empty resume must not emit a cursor"
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

    let predicate = tag_between_equivalent_predicate(10, 50);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);

    let page1_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("tag")
        .limit(2)
        .plan()
        .expect("multi-page desc page1 plan should build");
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("multi-page desc page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("multi-page desc page1 should execute");
    assert_eq!(
        ids_from_items(&page1.items.0),
        vec![Ulid::from_u128(21_405), Ulid::from_u128(21_404)],
        "descending page1 should return E, D"
    );

    let page1_cursor = page1
        .next_cursor
        .as_ref()
        .expect("multi-page desc page1 should emit continuation cursor");
    let page2_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("tag")
        .limit(2)
        .plan()
        .expect("multi-page desc page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor(Some(page1_cursor.as_slice()))
        .expect("multi-page desc page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("multi-page desc page2 should execute");
    assert_eq!(
        ids_from_items(&page2.items.0),
        vec![Ulid::from_u128(21_403), Ulid::from_u128(21_402)],
        "descending page2 should return C, B"
    );

    let page2_cursor = page2
        .next_cursor
        .as_ref()
        .expect("multi-page desc page2 should emit continuation cursor");
    let page3_plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by_desc("tag")
        .limit(2)
        .plan()
        .expect("multi-page desc page3 plan should build");
    let page3_boundary = page3_plan
        .plan_cursor(Some(page2_cursor.as_slice()))
        .expect("multi-page desc page3 boundary should plan");
    let page3 = load
        .execute_paged_with_cursor(page3_plan, page3_boundary)
        .expect("multi-page desc page3 should execute");
    assert_eq!(
        ids_from_items(&page3.items.0),
        vec![Ulid::from_u128(21_401)],
        "descending page3 should return A"
    );
    assert!(
        page3.next_cursor.is_none(),
        "final descending page should not emit a continuation cursor"
    );

    let mut all_ids = ids_from_items(&page1.items.0);
    all_ids.extend(ids_from_items(&page2.items.0));
    all_ids.extend(ids_from_items(&page3.items.0));

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
    let unique_ids: BTreeSet<Ulid> = all_ids.iter().copied().collect();
    assert_eq!(
        unique_ids.len(),
        all_ids.len(),
        "descending pagination must not duplicate rows"
    );
}

#[test]
fn load_single_field_range_pushdown_parity_matrix_is_table_driven() {
    #[derive(Clone, Copy)]
    struct Case {
        name: &'static str,
        bounds: &'static [(CompareOp, u32)],
        descending: bool,
    }

    const GT_10: &[(CompareOp, u32)] = &[(CompareOp::Gt, 10)];
    const GTE_10: &[(CompareOp, u32)] = &[(CompareOp::Gte, 10)];
    const LT_30: &[(CompareOp, u32)] = &[(CompareOp::Lt, 30)];
    const LTE_30: &[(CompareOp, u32)] = &[(CompareOp::Lte, 30)];
    const GTE_10_LT_30: &[(CompareOp, u32)] = &[(CompareOp::Gte, 10), (CompareOp::Lt, 30)];
    const GT_10_LTE_30: &[(CompareOp, u32)] = &[(CompareOp::Gt, 10), (CompareOp::Lte, 30)];
    const BETWEEN_10_30: &[(CompareOp, u32)] = &[(CompareOp::Gte, 10), (CompareOp::Lte, 30)];
    const GT_40_NO_MATCH: &[(CompareOp, u32)] = &[(CompareOp::Gt, 40)];
    const LTE_40_ALL: &[(CompareOp, u32)] = &[(CompareOp::Lte, 40)];

    let cases = [
        Case {
            name: "gt_only",
            bounds: GT_10,
            descending: false,
        },
        Case {
            name: "gte_only",
            bounds: GTE_10,
            descending: false,
        },
        Case {
            name: "lt_only_desc",
            bounds: LT_30,
            descending: true,
        },
        Case {
            name: "lte_only",
            bounds: LTE_30,
            descending: false,
        },
        Case {
            name: "gte_lt_window",
            bounds: GTE_10_LT_30,
            descending: false,
        },
        Case {
            name: "gt_lte_window_desc",
            bounds: GT_10_LTE_30,
            descending: true,
        },
        Case {
            name: "between_equivalent",
            bounds: BETWEEN_10_30,
            descending: false,
        },
        Case {
            name: "no_match",
            bounds: GT_40_NO_MATCH,
            descending: false,
        },
        Case {
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

    for case in cases {
        // Phase 1: seed deterministic rows and verify range planning shape.
        reset_store();
        seed_indexed_metrics_rows(&rows);

        let predicate = predicate_from_field_bounds("tag", case.bounds);
        let mut explain_query = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
            .filter(predicate.clone());
        explain_query = if case.descending {
            explain_query.order_by_desc("tag")
        } else {
            explain_query.order_by("tag")
        };
        let explain = explain_query
            .explain()
            .expect("single-field matrix explain should build");
        assert!(
            explain_contains_index_range(&explain.access, INDEXED_METRICS_INDEX_MODELS[0].name, 0),
            "single-field case '{}' should plan an IndexRange access path",
            case.name
        );

        // Phase 2: execute pushdown and fallback plans under identical ordering.
        let fallback_seed_ids = indexed_metrics_ids_for_bounds(&rows, case.bounds);
        assert_pushdown_parity(
            || {
                let query = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                    .filter(predicate.clone());
                if case.descending {
                    query.order_by_desc("tag")
                } else {
                    query.order_by("tag")
                }
            },
            fallback_seed_ids,
            |query| {
                if case.descending {
                    query.order_by_desc("tag")
                } else {
                    query.order_by("tag")
                }
            },
        );
    }
}

#[test]
fn load_composite_range_pushdown_parity_matrix_is_table_driven() {
    #[derive(Clone, Copy)]
    struct Case {
        name: &'static str,
        bounds: &'static [(CompareOp, u32)],
        descending: bool,
    }

    const GT_10: &[(CompareOp, u32)] = &[(CompareOp::Gt, 10)];
    const GTE_10: &[(CompareOp, u32)] = &[(CompareOp::Gte, 10)];
    const LT_30: &[(CompareOp, u32)] = &[(CompareOp::Lt, 30)];
    const LTE_30: &[(CompareOp, u32)] = &[(CompareOp::Lte, 30)];
    const GTE_10_LT_40: &[(CompareOp, u32)] = &[(CompareOp::Gte, 10), (CompareOp::Lt, 40)];
    const GT_10_LTE_40: &[(CompareOp, u32)] = &[(CompareOp::Gt, 10), (CompareOp::Lte, 40)];
    const BETWEEN_10_30: &[(CompareOp, u32)] = &[(CompareOp::Gte, 10), (CompareOp::Lte, 30)];
    const GT_50_NO_MATCH: &[(CompareOp, u32)] = &[(CompareOp::Gt, 50)];
    const LTE_50_ALL: &[(CompareOp, u32)] = &[(CompareOp::Lte, 50)];

    let cases = [
        Case {
            name: "gt_only",
            bounds: GT_10,
            descending: false,
        },
        Case {
            name: "gte_only",
            bounds: GTE_10,
            descending: false,
        },
        Case {
            name: "lt_only_desc",
            bounds: LT_30,
            descending: true,
        },
        Case {
            name: "lte_only",
            bounds: LTE_30,
            descending: false,
        },
        Case {
            name: "gte_lt_window",
            bounds: GTE_10_LT_40,
            descending: false,
        },
        Case {
            name: "gt_lte_window_desc",
            bounds: GT_10_LTE_40,
            descending: true,
        },
        Case {
            name: "between_equivalent",
            bounds: BETWEEN_10_30,
            descending: false,
        },
        Case {
            name: "no_match",
            bounds: GT_50_NO_MATCH,
            descending: false,
        },
        Case {
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
    for case in cases {
        // Phase 1: seed deterministic rows and verify prefix+range planning shape.
        reset_store();
        seed_pushdown_rows(&rows);

        let mut compare_bounds = Vec::with_capacity(case.bounds.len() + 1);
        compare_bounds.push(strict_compare_predicate(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
        ));
        for (op, bound) in case.bounds {
            compare_bounds.push(strict_compare_predicate(
                "rank",
                *op,
                Value::Uint(u64::from(*bound)),
            ));
        }
        let predicate = Predicate::And(compare_bounds);

        let mut explain_query = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(predicate.clone());
        explain_query = if case.descending {
            explain_query.order_by_desc("rank")
        } else {
            explain_query.order_by("rank")
        };
        let explain = explain_query
            .explain()
            .expect("composite matrix explain should build");
        assert!(
            explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
            "composite case '{}' should plan an IndexRange access path",
            case.name
        );

        // Phase 2: execute pushdown and fallback plans under identical ordering.
        let fallback_seed_ids = pushdown_ids_for_group_rank_bounds(&rows, 7, case.bounds);
        assert_pushdown_parity(
            || {
                let query = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(predicate.clone());
                if case.descending {
                    query.order_by_desc("rank")
                } else {
                    query.order_by("rank")
                }
            },
            fallback_seed_ids,
            |query| {
                if case.descending {
                    query.order_by_desc("rank")
                } else {
                    query.order_by("rank")
                }
            },
        );
    }
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

    let predicate = group_rank_between_equivalent_predicate(7, 10, 30);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(10)
        .explain()
        .expect("composite duplicate-edge explain should build");
    assert!(
        explain_contains_index_range(&explain.access, PUSHDOWN_PARITY_INDEX_MODELS[0].name, 1),
        "composite duplicate-edge boundary test should plan an IndexRange access path"
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    // Phase 1: collect the full ranged row set and verify expected window size.
    let base_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(10)
        .plan()
        .expect("composite duplicate-edge base plan should build");
    let base_page = load
        .execute_paged_with_cursor(base_plan, None)
        .expect("composite duplicate-edge base page should execute");
    let all_ids: Vec<Ulid> = ids_from_items(&base_page.items.0);
    assert_eq!(
        all_ids.len(),
        5,
        "composite between range should include duplicate lower and upper edge rows"
    );

    // Phase 2: boundary at the first lower-edge row should skip only that row.
    let lower_entity = &base_page.items.0[0].1;
    assert_resume_after_entity(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("rank")
                .limit(10)
        },
        lower_entity,
        all_ids[1..].to_vec(),
    );

    // Phase 3: mid-window boundary should resume at the next strict row.
    let mid_entity = &base_page.items.0[2].1;
    assert_resume_after_entity(
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by("rank")
                .limit(10)
        },
        mid_entity,
        all_ids[3..].to_vec(),
    );

    // Phase 4: boundary at the terminal upper-edge row should produce an empty continuation page.
    let terminal_entity = &base_page.items.0[base_page.items.0.len() - 1].1;
    let terminal_boundary_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(10)
        .plan()
        .expect("composite duplicate-edge terminal-boundary plan should build");
    let terminal_boundary = terminal_boundary_plan
        .into_inner()
        .cursor_boundary_from_entity(terminal_entity)
        .expect("composite duplicate-edge terminal boundary should build");
    let past_end_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(group_rank_between_equivalent_predicate(7, 10, 30))
        .order_by("rank")
        .limit(10)
        .plan()
        .expect("composite duplicate-edge past-end plan should build");
    let past_end = load
        .execute_paged_with_cursor(past_end_plan, Some(terminal_boundary))
        .expect("composite duplicate-edge past-end page should execute");
    assert!(
        past_end.items.0.is_empty(),
        "boundary at upper-edge terminal row should return an empty continuation page"
    );
    assert!(
        past_end.next_cursor.is_none(),
        "composite empty continuation page should not emit a cursor"
    );
}

#[test]
fn load_trace_marks_secondary_order_pushdown_outcomes() {
    #[derive(Clone, Copy)]
    enum ExpectedDecision {
        Accepted,
        RejectedNonAscending,
    }

    #[derive(Clone, Copy)]
    struct Case {
        name: &'static str,
        prefix: u128,
        descending: bool,
        expected: ExpectedDecision,
    }

    let cases = [
        Case {
            name: "accepted_ascending",
            prefix: 16_000,
            descending: false,
            expected: ExpectedDecision::Accepted,
        },
        Case {
            name: "rejected_descending",
            prefix: 17_000,
            descending: true,
            expected: ExpectedDecision::RejectedNonAscending,
        },
    ];

    setup_pagination_test();

    for case in cases {
        reset_store();
        seed_pushdown_rows(&pushdown_rows_trace(case.prefix));

        let predicate = pushdown_group_predicate(7);
        let mut query = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(predicate)
            .limit(1);
        query = if case.descending {
            query.order_by_desc("rank")
        } else {
            query.order_by("rank")
        };

        let plan = query
            .plan()
            .expect("trace outcome test plan should build for case");

        let _ = take_trace_events();
        let load =
            LoadExecutor::<PushdownParityEntity>::new(DB, false).with_trace(&TEST_TRACE_SINK);
        let _page = load
            .execute_paged_with_cursor(plan, None)
            .expect("trace outcome execution should succeed for case");
        let events = take_trace_events();

        let matched = events.iter().any(|event| match case.expected {
            ExpectedDecision::Accepted => matches!(
                event,
                QueryTraceEvent::Pushdown {
                    decision: TracePushdownDecision::AcceptedSecondaryIndexOrder,
                    ..
                }
            ),
            ExpectedDecision::RejectedNonAscending => matches!(
                event,
                QueryTraceEvent::Pushdown {
                    decision: TracePushdownDecision::RejectedSecondaryIndexOrder {
                        reason: TracePushdownRejectionReason::NonAscendingDirection
                    },
                    ..
                }
            ),
        });

        assert!(
            matched,
            "trace should emit expected secondary-order pushdown marker for case '{}'",
            case.name
        );
    }
}

#[test]
fn load_trace_marks_composite_index_range_pushdown_rejection_outcome() {
    setup_pagination_test();
    seed_pushdown_rows(&pushdown_rows_trace(22_000));

    let logical = LogicalPlan {
        mode: QueryMode::Load(LoadSpec::new()),
        access: AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::IndexRange {
                index: PUSHDOWN_PARITY_INDEX_MODELS[0],
                prefix: vec![Value::Uint(7)],
                lower: std::ops::Bound::Included(Value::Uint(10)),
                upper: std::ops::Bound::Excluded(Value::Uint(20)),
            }),
            AccessPlan::path(AccessPath::FullScan),
        ]),
        predicate: None,
        order: Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        }),
        delete_limit: None,
        page: Some(PageSpec {
            limit: Some(1),
            offset: 0,
        }),
        consistency: ReadConsistency::MissingOk,
    };
    let plan = ExecutablePlan::<PushdownParityEntity>::new(logical);

    let _ = take_trace_events();
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false).with_trace(&TEST_TRACE_SINK);
    let _page = load
        .execute_paged_with_cursor(plan, None)
        .expect("composite-index-range trace test execution should succeed");
    let events = take_trace_events();

    let matched = events.iter().any(|event| {
        matches!(
            event,
            QueryTraceEvent::Pushdown {
                decision: TracePushdownDecision::RejectedSecondaryIndexOrder {
                    reason: TracePushdownRejectionReason::AccessPathIndexRangeUnsupported
                },
                ..
            }
        )
    });
    assert!(
        matched,
        "composite access with index-range child should emit explicit pushdown rejection trace"
    );
}
