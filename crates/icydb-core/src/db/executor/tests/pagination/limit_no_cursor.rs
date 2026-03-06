use super::*;
use crate::obs::sink::{MetricsEvent, MetricsSink, with_metrics_sink};
use std::cell::RefCell;

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
        .expect("simple limit seed save should succeed");
    }
}

fn build_scalar_limit_plan(
    access: AccessPlan<Ulid>,
    limit: u32,
    offset: u32,
) -> ExecutablePlan<SimpleEntity> {
    ExecutablePlan::new(AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
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
        access,
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
        .map(|trace| trace.keys_scanned)
        .expect("traced execution should emit keys_scanned");
    let keys_scanned =
        usize::try_from(keys_scanned).expect("keys_scanned should fit within usize in test scope");

    (ids_from_items(&page.items), keys_scanned)
}

#[test]
fn load_limit_without_cursor_applies_scan_budget_across_primary_access_shapes() {
    setup_pagination_test();
    seed_simple_rows(&[41_001, 41_002, 41_003, 41_004, 41_005, 41_006]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, true);

    // Phase 1: ByKey shape still respects page limiting without cursor state.
    let single_lookup_id = Ulid::from_u128(41_003);
    let single_lookup_plan =
        build_scalar_limit_plan(AccessPlan::path(AccessPath::ByKey(single_lookup_id)), 1, 0);
    let (single_lookup_ids, single_lookup_scanned) =
        execute_page_ids_and_keys_scanned(&load, single_lookup_plan);
    assert_eq!(
        single_lookup_ids,
        vec![single_lookup_id],
        "ByKey should return the selected id"
    );
    assert!(
        single_lookup_scanned <= 2,
        "ByKey limit window should keep scan budget bounded (keys_scanned={single_lookup_scanned})",
    );

    // Phase 2: ByKeys shape should stop after offset+limit(+1) candidate keys.
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

    // Phase 3: KeyRange shape should apply the same no-cursor limit budget.
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

    // Phase 4: FullScan shape should also honor the no-cursor limit budget.
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

    assert_eq!(
        ids_from_items(&response),
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

    assert_eq!(
        ids_from_items(&response),
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
