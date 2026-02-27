#![expect(clippy::too_many_lines)]
use super::*;
use crate::{
    db::{
        access::{AccessPath, AccessPlan, SecondaryOrderPushdownRejection},
        cursor::ContinuationToken,
        direction::Direction,
        executor::ExecutablePlan,
        index::{EncodedValue, RawIndexKey, raw_keys_for_encoded_prefix},
        query::{
            explain::{ExplainAccessPath, ExplainOrderPushdown},
            intent::{LoadSpec, QueryMode},
            plan::{AccessPlannedQuery, LogicalPlan, OrderDirection, OrderSpec, PageSpec},
        },
    },
    error::{ErrorClass, ErrorOrigin},
    serialize::serialize,
    traits::Storable,
    types::Id,
};
use std::{borrow::Cow, collections::BTreeSet, ops::Bound};

macro_rules! assert_exhausted_continuation_page {
    ($page:expr, $empty_message:expr, $cursor_message:expr $(,)?) => {{
        assert!($page.items.0.is_empty(), "{}", $empty_message);
        assert!($page.next_cursor.is_none(), "{}", $cursor_message);
    }};
}

type RangeBounds = &'static [(CompareOp, u32)];

/// RangeMatrixCase
///
/// Shared table row for strict range-predicate pushdown parity matrices.
#[derive(Clone, Copy)]
struct RangeMatrixCase {
    name: &'static str,
    bounds: RangeBounds,
    descending: bool,
}

// Extract the canonical resume id from the trailing cursor boundary slot.
fn boundary_last_ulid(boundary: &CursorBoundary) -> Ulid {
    match boundary.slots.last() {
        Some(CursorBoundarySlot::Present(Value::Ulid(id))) => *id,
        slots => panic!("expected trailing Ulid boundary slot, found {slots:?}"),
    }
}

// Compute the strict suffix expected when resuming after one boundary id.
fn expected_resume_suffix_after_id(expected_ids: &[Ulid], boundary_id: Ulid) -> Vec<Ulid> {
    expected_ids
        .iter()
        .copied()
        .skip_while(|id| id != &boundary_id)
        .skip(1)
        .collect::<Vec<_>>()
}

// Verify boundary-complete resume semantics for a paged execution plan.
fn assert_resume_suffixes_from_boundaries<E, F>(
    load: &LoadExecutor<E>,
    build_plan: &F,
    boundaries: &[CursorBoundary],
    expected_ids: &[Ulid],
    max_pages: usize,
    context: &str,
) where
    E: EntityKind<Key = Ulid, Canister = TestCanister> + EntityValue,
    F: Fn() -> ExecutablePlan<E>,
{
    for boundary in boundaries {
        let boundary_id = boundary_last_ulid(boundary);
        let expected_suffix = expected_resume_suffix_after_id(expected_ids, boundary_id);
        let resumed_ids = collect_all_pages_from_executable_plan_with_start(
            load,
            build_plan,
            Some(boundary.clone()),
            max_pages,
        );
        assert_eq!(
            resumed_ids, expected_suffix,
            "{context}: resume from boundary should return strict suffix",
        );
    }
}

// Verify token-complete resume semantics for offset-aware paged execution plans.
fn assert_resume_suffixes_from_tokens<E, F>(
    load: &LoadExecutor<E>,
    build_plan: &F,
    tokens: &[Vec<u8>],
    expected_ids: &[Ulid],
    max_pages: usize,
    context: &str,
) where
    E: EntityKind<Key = Ulid, Canister = TestCanister> + EntityValue,
    F: Fn() -> ExecutablePlan<E>,
{
    for token in tokens {
        let boundary = decode_boundary(
            token.as_slice(),
            "continuation cursor should decode for token resume checks",
        );
        let boundary_id = boundary_last_ulid(&boundary);
        let expected_suffix = expected_resume_suffix_after_id(expected_ids, boundary_id);
        let resumed_ids = collect_all_pages_from_executable_plan_with_token_start(
            load,
            build_plan,
            Some(token.clone()),
            max_pages,
        );
        assert_eq!(
            resumed_ids, expected_suffix,
            "{context}: resume from token should return strict suffix",
        );
    }
}

// Resolve ids directly from the `(group, rank)` index prefix in raw index-key order.
fn ordered_ids_from_group_rank_index(group: u32) -> Vec<Ulid> {
    let encoded_prefix = [EncodedValue::try_new(Value::Uint(u64::from(group)))
        .expect("group literal should be canonically index-encodable")];
    let (lower, upper) = raw_keys_for_encoded_prefix::<PushdownParityEntity>(
        &PUSHDOWN_PARITY_INDEX_MODELS[0],
        encoded_prefix.as_slice(),
    );
    let (lower, upper) = (Bound::Included(lower), Bound::Included(upper));

    // Phase 1: read candidate keys from canonical index traversal order.
    let data_keys = DB
        .with_store_registry(|reg| {
            reg.try_get_store(TestDataStore::PATH).and_then(|store| {
                store.with_index(|index_store| {
                    index_store.resolve_data_values_in_raw_range_limited::<PushdownParityEntity>(
                        &PUSHDOWN_PARITY_INDEX_MODELS[0],
                        (&lower, &upper),
                        None,
                        Direction::Asc,
                        usize::MAX,
                        None,
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

// Build one strict composite `(group, rank)` predicate from range matrix bounds.
fn predicate_from_group_rank_bounds(group: u32, bounds: &[(CompareOp, u32)]) -> Predicate {
    let mut predicates = Vec::with_capacity(bounds.len() + 1);
    predicates.push(strict_compare_predicate(
        "group",
        CompareOp::Eq,
        Value::Uint(u64::from(group)),
    ));
    for (op, bound) in bounds {
        predicates.push(strict_compare_predicate(
            "rank",
            *op,
            Value::Uint(u64::from(*bound)),
        ));
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

fn assert_anchor_monotonic(
    anchors: &mut Vec<RawIndexKey>,
    next_cursor: &[u8],
    decode_message: &'static str,
    missing_anchor_message: &'static str,
    monotonic_message: &'static str,
) {
    let token = ContinuationToken::decode(next_cursor).expect(decode_message);
    let anchor = <RawIndexKey as Storable>::from_bytes(Cow::Borrowed(
        token
            .index_range_anchor()
            .expect(missing_anchor_message)
            .last_raw_key(),
    ));

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

fn encode_token(token: &ContinuationToken, encode_message: &'static str) -> Vec<u8> {
    token.encode().expect(encode_message)
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
            .prepare_cursor(cursor.as_deref())
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
            Some(next) => {
                cursor = Some(encode_token(
                    &next,
                    "continuation cursor should serialize for loop resume",
                ));
            }
            None => break,
        }
    }

    (ids, row_bytes)
}

// Collect all pages for a fixed executable-plan shape while preserving the
// emitted continuation cursor bytes for boundary-equivalence assertions.
fn collect_all_pages_from_executable_plan<E>(
    load: &LoadExecutor<E>,
    build_plan: impl Fn() -> ExecutablePlan<E>,
    max_pages: usize,
) -> (Vec<Ulid>, Vec<CursorBoundary>)
where
    E: EntityKind<Key = Ulid, Canister = TestCanister> + EntityValue,
{
    let mut cursor: Option<CursorBoundary> = None;
    let mut ids = Vec::new();
    let mut boundaries = Vec::new();
    let mut pages = 0usize;

    loop {
        pages += 1;
        assert!(pages <= max_pages, "pagination must terminate");

        let page = load
            .execute_paged_with_cursor(build_plan(), cursor.clone())
            .expect("paged execution should succeed");

        ids.extend(ids_from_items(&page.items.0));

        let Some(next_cursor) = page.next_cursor else {
            break;
        };

        let next_boundary = next_cursor.boundary().clone();
        cursor = Some(next_boundary.clone());
        boundaries.push(next_boundary);
    }

    (ids, boundaries)
}

// Collect all pages while preserving raw continuation cursor bytes.
fn collect_all_pages_from_executable_plan_with_tokens<E>(
    load: &LoadExecutor<E>,
    build_plan: impl Fn() -> ExecutablePlan<E>,
    max_pages: usize,
) -> (Vec<Ulid>, Vec<CursorBoundary>, Vec<Vec<u8>>)
where
    E: EntityKind<Key = Ulid, Canister = TestCanister> + EntityValue,
{
    let mut cursor_bytes: Option<Vec<u8>> = None;
    let mut ids = Vec::new();
    let mut boundaries = Vec::new();
    let mut tokens = Vec::new();
    let mut pages = 0usize;

    loop {
        pages += 1;
        assert!(pages <= max_pages, "pagination must terminate");

        let plan = build_plan();
        let planned_cursor = plan
            .prepare_cursor(cursor_bytes.as_deref())
            .expect("page cursor should plan");
        let page = load
            .execute_paged_with_cursor(plan, planned_cursor)
            .expect("paged execution should succeed");

        ids.extend(ids_from_items(&page.items.0));

        let Some(next_cursor) = page.next_cursor else {
            break;
        };

        let next_cursor_bytes = encode_token(
            &next_cursor,
            "continuation cursor should serialize for token collection",
        );
        boundaries.push(decode_boundary(
            next_cursor_bytes.as_slice(),
            "continuation cursor should decode",
        ));
        tokens.push(next_cursor_bytes.clone());
        cursor_bytes = Some(next_cursor_bytes);
    }

    (ids, boundaries, tokens)
}

// Collect all pages from a fixed executable plan starting from one cursor boundary.
fn collect_all_pages_from_executable_plan_with_start<E>(
    load: &LoadExecutor<E>,
    build_plan: impl Fn() -> ExecutablePlan<E>,
    initial_cursor: Option<CursorBoundary>,
    max_pages: usize,
) -> Vec<Ulid>
where
    E: EntityKind<Key = Ulid, Canister = TestCanister> + EntityValue,
{
    let mut cursor = initial_cursor;
    let mut ids = Vec::new();
    let mut pages = 0usize;

    loop {
        pages += 1;
        assert!(pages <= max_pages, "pagination must terminate");

        let page = load
            .execute_paged_with_cursor(build_plan(), cursor.clone())
            .expect("paged execution should succeed");
        ids.extend(ids_from_items(&page.items.0));

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor.boundary().clone());
    }

    ids
}

// Collect all pages from a fixed executable plan starting from raw cursor bytes.
fn collect_all_pages_from_executable_plan_with_token_start<E>(
    load: &LoadExecutor<E>,
    build_plan: impl Fn() -> ExecutablePlan<E>,
    initial_cursor: Option<Vec<u8>>,
    max_pages: usize,
) -> Vec<Ulid>
where
    E: EntityKind<Key = Ulid, Canister = TestCanister> + EntityValue,
{
    let mut cursor = initial_cursor;
    let mut ids = Vec::new();
    let mut pages = 0usize;

    loop {
        pages += 1;
        assert!(pages <= max_pages, "pagination must terminate");

        let plan = build_plan();
        let planned_cursor = plan
            .prepare_cursor(cursor.as_deref())
            .expect("page cursor should plan");
        let page = load
            .execute_paged_with_cursor(plan, planned_cursor)
            .expect("paged execution should succeed");
        ids.extend(ids_from_items(&page.items.0));

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(encode_token(
            &next_cursor,
            "continuation cursor should serialize for token-start resume",
        ));
    }

    ids
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

fn assert_resume_from_terminal_entity_exhausts_range<E>(
    build_query: impl Fn() -> Query<E>,
    terminal_entity: &E,
    empty_message: &'static str,
    cursor_message: &'static str,
) where
    E: EntityKind<Key = Ulid, Canister = TestCanister> + EntityValue,
{
    let load = LoadExecutor::<E>::new(DB, false);

    let terminal_boundary = build_query()
        .plan()
        .expect("terminal-boundary plan should build")
        .into_inner()
        .cursor_boundary_from_entity(terminal_entity)
        .expect("terminal boundary should build");
    let resume = load
        .execute_paged_with_cursor(
            build_query()
                .plan()
                .expect("terminal resume plan should build"),
            Some(terminal_boundary),
        )
        .expect("terminal resume execution should succeed");

    assert_exhausted_continuation_page!(resume, empty_message, cursor_message);
}

fn apply_order_field<E>(query: Query<E>, field: &str, descending: bool) -> Query<E>
where
    E: EntityKind,
{
    if descending {
        query.order_by_desc(field)
    } else {
        query.order_by(field)
    }
}

#[expect(clippy::too_many_arguments)]
fn run_range_pushdown_parity_matrix<E, Row>(
    rows: &[Row],
    cases: &[RangeMatrixCase],
    seed_rows: fn(&[Row]),
    build_predicate: impl Fn(RangeBounds) -> Predicate + Copy,
    fallback_ids_for_bounds: impl Fn(&[Row], RangeBounds) -> Vec<Ulid> + Copy,
    order_field: &str,
    index_name: &'static str,
    index_range_slots: usize,
    matrix_name: &'static str,
) where
    E: EntityKind<Key = Ulid, Canister = TestCanister> + EntityValue,
{
    for case in cases {
        // Phase 1: seed deterministic rows and verify range planning shape.
        reset_store();
        seed_rows(rows);

        let predicate = build_predicate(case.bounds);
        let explain = apply_order_field(
            Query::<E>::new(ReadConsistency::MissingOk).filter(predicate.clone()),
            order_field,
            case.descending,
        )
        .explain()
        .expect("range matrix explain should build");
        assert!(
            explain_contains_index_range(&explain.access, index_name, index_range_slots),
            "{} case '{}' should plan an IndexRange access path",
            matrix_name,
            case.name
        );

        // Phase 2: execute pushdown and fallback plans under identical ordering.
        let fallback_seed_ids = fallback_ids_for_bounds(rows, case.bounds);
        assert_pushdown_parity(
            || {
                apply_order_field(
                    Query::<E>::new(ReadConsistency::MissingOk).filter(predicate.clone()),
                    order_field,
                    case.descending,
                )
            },
            fallback_seed_ids,
            |query| apply_order_field(query, order_field, case.descending),
        );
    }
}

mod composite_budget;
mod cursor_pk;
mod distinct;
mod index_range;
mod ordering_permutations;
mod range_edges_trace;
