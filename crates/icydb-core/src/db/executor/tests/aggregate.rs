use super::*;
use crate::{
    db::{data::DataKey, query::plan::ExplainAccessPath},
    obs::sink::{MetricsEvent, MetricsSink, with_metrics_sink},
};
use std::cell::RefCell;

///
/// AggregateCaptureSink
///

#[derive(Default)]
struct AggregateCaptureSink {
    events: RefCell<Vec<MetricsEvent>>,
}

impl AggregateCaptureSink {
    fn into_events(self) -> Vec<MetricsEvent> {
        self.events.into_inner()
    }
}

impl MetricsSink for AggregateCaptureSink {
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
    let sink = AggregateCaptureSink::default();
    let output = with_metrics_sink(&sink, run);
    let rows_scanned = rows_scanned_for_entity(&sink.into_events(), entity_path);

    (output, rows_scanned)
}

fn seed_simple_entities(ids: &[u128]) {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in ids {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(*id),
        })
        .expect("seed row save should succeed");
    }
}

fn seed_pushdown_entities(rows: &[(u128, u32, u32)]) {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PushdownParityEntity>::new(DB, false);
    for (id, group, rank) in rows {
        save.insert(PushdownParityEntity {
            id: Ulid::from_u128(*id),
            group: *group,
            rank: *rank,
            label: format!("g{group}-r{rank}"),
        })
        .expect("seed pushdown row save should succeed");
    }
}

fn seed_unique_index_range_entities(rows: &[(u128, u32)]) {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    for (id, code) in rows {
        save.insert(UniqueIndexRangeEntity {
            id: Ulid::from_u128(*id),
            code: *code,
            label: format!("code-{code}"),
        })
        .expect("seed unique-index row save should succeed");
    }
}

fn seed_phase_entities(rows: &[(u128, u32)]) {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for (id, rank) in rows {
        save.insert(PhaseEntity {
            id: Ulid::from_u128(*id),
            opt_rank: Some(*rank),
            rank: *rank,
            tags: vec![*rank],
            label: format!("phase-{rank}"),
        })
        .expect("seed phase row save should succeed");
    }
}

fn remove_pushdown_row_data(id: u128) {
    let raw_key = DataKey::try_new::<PushdownParityEntity>(Ulid::from_u128(id))
        .expect("pushdown data key should build")
        .to_raw()
        .expect("pushdown data key should encode");

    DATA_STORE.with(|store| {
        let removed = store.borrow_mut().remove(&raw_key);
        assert!(
            removed.is_some(),
            "expected row to exist before data-only removal"
        );
    });
}

fn assert_aggregate_parity_for_query<E>(
    load: &LoadExecutor<E>,
    make_query: impl Fn() -> Query<E>,
    context: &str,
) where
    E: EntityKind<Canister = TestCanister> + EntityValue,
{
    // Execute canonical materialized baseline once per query shape.
    let expected_response = load
        .execute(
            make_query()
                .plan()
                .expect("baseline materialized plan should build"),
        )
        .expect("baseline materialized execution should succeed");
    let expected_count = expected_response.count();
    let expected_exists = !expected_response.is_empty();
    let expected_min = expected_response.ids().into_iter().min();
    let expected_max = expected_response.ids().into_iter().max();

    // Execute aggregate terminals against the same logical query shape.
    let actual_count = load
        .aggregate_count(
            make_query()
                .plan()
                .expect("aggregate COUNT plan should build"),
        )
        .expect("aggregate COUNT should succeed");
    let actual_exists = load
        .aggregate_exists(
            make_query()
                .plan()
                .expect("aggregate EXISTS plan should build"),
        )
        .expect("aggregate EXISTS should succeed");
    let actual_min = load
        .aggregate_min(
            make_query()
                .plan()
                .expect("aggregate MIN plan should build"),
        )
        .expect("aggregate MIN should succeed");
    let actual_max = load
        .aggregate_max(
            make_query()
                .plan()
                .expect("aggregate MAX plan should build"),
        )
        .expect("aggregate MAX should succeed");

    assert_eq!(
        actual_count, expected_count,
        "{context}: count parity failed"
    );
    assert_eq!(
        actual_exists, expected_exists,
        "{context}: exists parity failed"
    );
    assert_eq!(actual_min, expected_min, "{context}: min parity failed");
    assert_eq!(actual_max, expected_max, "{context}: max parity failed");
}

fn assert_count_parity_for_query<E>(
    load: &LoadExecutor<E>,
    make_query: impl Fn() -> Query<E>,
    context: &str,
) where
    E: EntityKind<Canister = TestCanister> + EntityValue,
{
    let expected_count = load
        .execute(
            make_query()
                .plan()
                .expect("baseline materialized plan should build"),
        )
        .expect("baseline materialized execution should succeed")
        .count();

    let actual_count = load
        .aggregate_count(
            make_query()
                .plan()
                .expect("aggregate COUNT plan should build"),
        )
        .expect("aggregate COUNT should succeed");

    assert_eq!(
        actual_count, expected_count,
        "{context}: count parity failed"
    );
}

fn id_in_predicate(ids: &[u128]) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::In,
        Value::List(
            ids.iter()
                .copied()
                .map(|id| Value::Ulid(Ulid::from_u128(id)))
                .collect(),
        ),
        CoercionId::Strict,
    ))
}

fn explain_access_supports_count_pushdown(access: &ExplainAccessPath) -> bool {
    match access {
        ExplainAccessPath::FullScan | ExplainAccessPath::KeyRange { .. } => true,
        ExplainAccessPath::Union(children) | ExplainAccessPath::Intersection(children) => {
            children.iter().all(explain_access_supports_count_pushdown)
        }
        ExplainAccessPath::ByKey { .. }
        | ExplainAccessPath::ByKeys { .. }
        | ExplainAccessPath::IndexPrefix { .. }
        | ExplainAccessPath::IndexRange { .. } => false,
    }
}

fn count_pushdown_contract_eligible<E>(plan: &crate::db::query::plan::ExecutablePlan<E>) -> bool
where
    E: EntityKind<Canister = TestCanister> + EntityValue,
{
    plan.as_inner().is_streaming_access_shape_safe::<E>()
        && explain_access_supports_count_pushdown(&plan.explain().access)
}

fn u32_eq_predicate(field: &str, value: u32) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::Eq,
        Value::Uint(u64::from(value)),
        CoercionId::NumericWiden,
    ))
}

fn u32_range_predicate(field: &str, lower_inclusive: u32, upper_exclusive: u32) -> Predicate {
    Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            field,
            CompareOp::Gte,
            Value::Uint(u64::from(lower_inclusive)),
            CoercionId::NumericWiden,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            field,
            CompareOp::Lt,
            Value::Uint(u64::from(upper_exclusive)),
            CoercionId::NumericWiden,
        )),
    ])
}

#[test]
fn aggregate_parity_ordered_page_window_asc() {
    seed_simple_entities(&[8101, 8102, 8103, 8104, 8105, 8106, 8107, 8108]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .offset(2)
                .limit(3)
        },
        "ordered ASC page window",
    );
}

#[test]
fn aggregate_parity_ordered_page_window_desc() {
    seed_simple_entities(&[8201, 8202, 8203, 8204, 8205, 8206, 8207, 8208]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
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
        || Query::<SimpleEntity>::new(ReadConsistency::MissingOk).by_id(Ulid::from_u128(8602)),
        "by_id path",
    );

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk).by_ids([
                Ulid::from_u128(8604),
                Ulid::from_u128(8601),
                Ulid::from_u128(8604),
            ])
        },
        "by_ids path",
    );
}

#[test]
fn aggregate_parity_distinct_asc() {
    seed_simple_entities(&[8301, 8302, 8303, 8304, 8305, 8306]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let predicate = Predicate::Or(vec![
        id_in_predicate(&[8301, 8302, 8303, 8304]),
        id_in_predicate(&[8303, 8304, 8305, 8306]),
    ]);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .distinct()
                .order_by("id")
                .offset(1)
                .limit(3)
        },
        "distinct ASC",
    );
}

#[test]
fn aggregate_parity_distinct_desc() {
    seed_simple_entities(&[8401, 8402, 8403, 8404, 8405, 8406]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let predicate = Predicate::Or(vec![
        id_in_predicate(&[8401, 8402, 8403, 8404]),
        id_in_predicate(&[8403, 8404, 8405, 8406]),
    ]);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .distinct()
                .order_by_desc("id")
                .offset(1)
                .limit(3)
        },
        "distinct DESC",
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
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
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
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .filter(intersection_predicate.clone())
                .order_by_desc("id")
                .offset(0)
                .limit(2)
        },
        "intersection path",
    );
}

#[test]
fn aggregate_parity_secondary_index_order_shape() {
    seed_pushdown_entities(&[
        (8801, 7, 40),
        (8802, 7, 10),
        (8803, 7, 30),
        (8804, 7, 20),
        (8805, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let group_seven = u32_eq_predicate("group", 7);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(group_seven.clone())
                .order_by("rank")
                .offset(1)
                .limit(2)
        },
        "secondary-index order shape",
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
            Query::<UniqueIndexRangeEntity>::new(ReadConsistency::MissingOk)
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
            Query::<SimpleEntity>::new(ReadConsistency::Strict)
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
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .offset(2)
                .limit(0)
        },
        "limit zero window",
    );
}

#[test]
fn session_load_aggregate_terminals_match_execute() {
    seed_simple_entities(&[8501, 8502, 8503, 8504, 8505]);
    let session = DbSession::new(DB);

    let expected = session
        .load::<SimpleEntity>()
        .order_by("id")
        .offset(1)
        .limit(3)
        .execute()
        .expect("baseline session execute should succeed");
    let expected_count = expected.count();
    let expected_exists = !expected.is_empty();
    let expected_min = expected.ids().into_iter().min();
    let expected_max = expected.ids().into_iter().max();

    let actual_count = session
        .load::<SimpleEntity>()
        .order_by("id")
        .offset(1)
        .limit(3)
        .count()
        .expect("session count should succeed");
    let actual_exists = session
        .load::<SimpleEntity>()
        .order_by("id")
        .offset(1)
        .limit(3)
        .exists()
        .expect("session exists should succeed");
    let actual_min = session
        .load::<SimpleEntity>()
        .order_by("id")
        .offset(1)
        .limit(3)
        .min()
        .expect("session min should succeed");
    let actual_max = session
        .load::<SimpleEntity>()
        .order_by("id")
        .offset(1)
        .limit(3)
        .max()
        .expect("session max should succeed");

    assert_eq!(actual_count, expected_count, "session count parity failed");
    assert_eq!(
        actual_exists, expected_exists,
        "session exists parity failed"
    );
    assert_eq!(actual_min, expected_min, "session min parity failed");
    assert_eq!(actual_max, expected_max, "session max parity failed");
}

#[test]
fn aggregate_exists_desc_early_stop_matches_asc_scan_budget() {
    seed_simple_entities(&[9201, 9202, 9203, 9204, 9205, 9206]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (exists_asc, scanned_asc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_exists(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .plan()
                .expect("exists ASC plan should build"),
        )
        .expect("exists ASC should succeed")
    });
    let (exists_desc, scanned_desc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_exists(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by_desc("id")
                .plan()
                .expect("exists DESC plan should build"),
        )
        .expect("exists DESC should succeed")
    });

    assert!(exists_asc, "exists ASC should find at least one row");
    assert!(exists_desc, "exists DESC should find at least one row");
    assert_eq!(
        scanned_asc, 1,
        "exists ASC should early-stop after first key"
    );
    assert_eq!(
        scanned_desc, 1,
        "exists DESC should early-stop after first key"
    );
}

#[test]
fn aggregate_extrema_first_row_short_circuit_is_direction_symmetric() {
    seed_simple_entities(&[9301, 9302, 9303, 9304, 9305, 9306]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (min_asc, scanned_min_asc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_min(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .plan()
                .expect("min ASC plan should build"),
        )
        .expect("min ASC should succeed")
    });
    let (max_desc, scanned_max_desc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_max(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by_desc("id")
                .plan()
                .expect("max DESC plan should build"),
        )
        .expect("max DESC should succeed")
    });

    assert_eq!(min_asc.map(|id| id.key()), Some(Ulid::from_u128(9301)));
    assert_eq!(max_desc.map(|id| id.key()), Some(Ulid::from_u128(9306)));
    assert_eq!(
        scanned_min_asc, 1,
        "min ASC should early-stop after first in-window key"
    );
    assert_eq!(
        scanned_max_desc, 1,
        "max DESC should early-stop after first in-window key"
    );
}

#[test]
fn aggregate_extrema_offset_short_circuit_scans_offset_plus_one() {
    seed_simple_entities(&[9401, 9402, 9403, 9404, 9405, 9406, 9407]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (min_asc, scanned_min_asc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_min(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by("id")
                .offset(3)
                .plan()
                .expect("min ASC with offset plan should build"),
        )
        .expect("min ASC with offset should succeed")
    });
    let (max_desc, scanned_max_desc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_max(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by_desc("id")
                .offset(3)
                .plan()
                .expect("max DESC with offset plan should build"),
        )
        .expect("max DESC with offset should succeed")
    });

    assert_eq!(min_asc.map(|id| id.key()), Some(Ulid::from_u128(9404)));
    assert_eq!(max_desc.map(|id| id.key()), Some(Ulid::from_u128(9404)));
    assert_eq!(
        scanned_min_asc, 4,
        "min ASC should scan exactly offset + 1 keys"
    );
    assert_eq!(
        scanned_max_desc, 4,
        "max DESC should scan exactly offset + 1 keys"
    );
}

#[test]
fn aggregate_distinct_offset_probe_hint_suppression_preserves_parity() {
    seed_simple_entities(&[9501, 9502]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let duplicate_front_predicate = Predicate::Or(vec![
        id_in_predicate(&[9501]),
        id_in_predicate(&[9501, 9502]),
    ]);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .filter(duplicate_front_predicate.clone())
                .distinct()
                .order_by("id")
                .offset(1)
        },
        "distinct + offset probe-hint suppression",
    );
}

#[test]
fn aggregate_count_distinct_offset_window_disables_bounded_probe_hint() {
    seed_simple_entities(&[9511, 9512, 9513, 9514, 9515, 9516, 9517]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count_asc, scanned_asc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_count(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .distinct()
                .order_by("id")
                .offset(2)
                .limit(2)
                .plan()
                .expect("count distinct+offset ASC plan should build"),
        )
        .expect("count distinct+offset ASC should succeed")
    });
    let (count_desc, scanned_desc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_count(
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .distinct()
                .order_by_desc("id")
                .offset(2)
                .limit(2)
                .plan()
                .expect("count distinct+offset DESC plan should build"),
        )
        .expect("count distinct+offset DESC should succeed")
    });

    assert_eq!(
        count_asc, 2,
        "ASC distinct+offset count should respect window"
    );
    assert_eq!(
        count_desc, 2,
        "DESC distinct+offset count should respect window"
    );
    assert_eq!(
        scanned_asc, 7,
        "ASC distinct+offset count should stay unbounded at access phase"
    );
    assert_eq!(
        scanned_desc, 7,
        "DESC distinct+offset count should stay unbounded at access phase"
    );
}

#[test]
fn aggregate_missing_ok_skips_leading_stale_secondary_keys_for_exists_min_max() {
    seed_pushdown_entities(&[
        (9601, 7, 10),
        (9602, 7, 20),
        (9603, 7, 30),
        (9604, 7, 40),
        (9605, 8, 50),
    ]);
    // Remove edge rows from primary data only, preserving index entries to
    // simulate stale leading secondary keys.
    remove_pushdown_row_data(9601);
    remove_pushdown_row_data(9604);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let group_seven = u32_eq_predicate("group", 7);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(group_seven.clone())
                .order_by("rank")
        },
        "MissingOk stale-leading ASC secondary path",
    );
    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(group_seven.clone())
                .order_by_desc("rank")
        },
        "MissingOk stale-leading DESC secondary path",
    );

    let (exists_asc, scanned_asc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_exists(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(group_seven.clone())
                    .order_by("rank")
                    .plan()
                    .expect("exists ASC stale-leading plan should build"),
            )
            .expect("exists ASC stale-leading should succeed")
        });
    let (exists_desc, scanned_desc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_exists(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(group_seven.clone())
                    .order_by_desc("rank")
                    .plan()
                    .expect("exists DESC stale-leading plan should build"),
            )
            .expect("exists DESC stale-leading should succeed")
        });

    assert!(
        exists_asc,
        "exists ASC should continue past stale leading key and find a row"
    );
    assert!(
        exists_desc,
        "exists DESC should continue past stale leading key and find a row"
    );
    assert!(
        scanned_asc >= 2,
        "exists ASC should scan beyond the first stale key"
    );
    assert!(
        scanned_desc >= 2,
        "exists DESC should scan beyond the first stale key"
    );
}

#[test]
fn aggregate_count_pushdown_contract_matrix_preserves_parity() {
    // Case A: full-scan ordered shape should be count-pushdown eligible.
    seed_simple_entities(&[9701, 9702, 9703, 9704, 9705]);
    let simple_load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let full_scan_query = || {
        Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
            .order_by("id")
            .offset(1)
            .limit(2)
    };
    let full_scan_plan = full_scan_query()
        .plan()
        .expect("full-scan count matrix plan should build");
    assert!(
        full_scan_plan
            .as_inner()
            .is_streaming_access_shape_safe::<SimpleEntity>(),
        "full-scan matrix shape should be streaming-safe"
    );
    assert!(
        count_pushdown_contract_eligible(&full_scan_plan),
        "full-scan matrix shape should be count-pushdown eligible by contract"
    );
    assert_count_parity_for_query(&simple_load, full_scan_query, "count matrix full-scan");

    // Case B: residual-filter full-scan is access-supported but not streaming-safe.
    seed_phase_entities(&[(9801, 1), (9802, 2), (9803, 2), (9804, 3)]);
    let phase_load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let residual_filter_query = || {
        Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("rank", 2))
            .order_by("id")
    };
    let residual_filter_plan = residual_filter_query()
        .plan()
        .expect("residual-filter count matrix plan should build");
    assert!(
        !residual_filter_plan
            .as_inner()
            .is_streaming_access_shape_safe::<PhaseEntity>(),
        "residual-filter matrix shape should be streaming-unsafe"
    );
    assert!(
        explain_access_supports_count_pushdown(&residual_filter_plan.explain().access),
        "residual-filter matrix shape should still be access-supported for pushdown paths"
    );
    assert!(
        !count_pushdown_contract_eligible(&residual_filter_plan),
        "residual-filter matrix shape must not be count-pushdown eligible"
    );
    assert_count_parity_for_query(
        &phase_load,
        residual_filter_query,
        "count matrix residual-filter full-scan",
    );

    // Case C: secondary-order query with stale leading keys must remain ineligible
    // for count pushdown and preserve materialized count parity.
    seed_pushdown_entities(&[(9901, 7, 10), (9902, 7, 20), (9903, 7, 30), (9904, 7, 40)]);
    remove_pushdown_row_data(9901);
    let pushdown_load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let secondary_index_query = || {
        Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(u32_eq_predicate("group", 7))
            .order_by("rank")
    };
    let secondary_index_plan = secondary_index_query()
        .plan()
        .expect("secondary-index count matrix plan should build");
    assert!(
        !count_pushdown_contract_eligible(&secondary_index_plan),
        "secondary-index matrix shape must not be count-pushdown eligible"
    );
    assert_count_parity_for_query(
        &pushdown_load,
        secondary_index_query,
        "count matrix secondary-index",
    );
}
