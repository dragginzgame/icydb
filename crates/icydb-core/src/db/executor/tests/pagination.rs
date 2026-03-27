//! Module: db::executor::tests::pagination
//! Responsibility: executor-owned pagination contracts for the revived live test harness.
//! Does not own: old matrix wrappers or query-intent paging policy tests.
//! Boundary: covers small runtime pagination behaviors that are easiest to validate end-to-end.

use super::*;
use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        executor::ExecutablePlan,
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::plan::{
            AccessPlannedQuery, LoadSpec, LogicalPlan, OrderDirection, OrderSpec, PageSpec,
            QueryMode, ScalarPlan,
        },
    },
    metrics::sink::{MetricsEvent, MetricsSink, with_metrics_sink},
    types::Ulid,
    value::Value,
};
use std::cell::RefCell;

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

fn pushdown_group_ids(rows: &[(u128, u32, u32, &str)], group: u32) -> Vec<Ulid> {
    rows.iter()
        .filter(|(_, row_group, _, _)| *row_group == group)
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
                .then_with(|| right_id.cmp(left_id))
        } else {
            left_rank
                .cmp(right_rank)
                .then_with(|| left_id.cmp(right_id))
        }
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
