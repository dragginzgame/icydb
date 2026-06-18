//! Module: db::executor::tests::aggregate_optimizations
//! Responsibility: live aggregate optimization-adjacent contracts on the
//! current executor surface.
//! Does not own: historical hit-counter seams or broad aggregate matrix
//! coverage.
//! Boundary: keeps observable aggregate optimization behavior near executor
//! support fixtures.

use super::support::*;
use crate::{
    db::{
        data::DecodedDataStoreKey,
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::builder::aggregate,
    },
    error::ErrorClass,
    traits::EntityKind,
    types::Ulid,
    value::Value,
};

fn field_slot_for_test<E>(field: &str) -> crate::db::query::plan::FieldSlot
where
    E: EntityKind,
{
    crate::db::query::plan::FieldSlot::resolve(E::MODEL, field).unwrap_or_else(|| {
        crate::db::query::plan::FieldSlot::from_test_slot(usize::MAX, field.to_string())
    })
}

fn remove_pushdown_row_data(id: u128) {
    let raw_key = DecodedDataStoreKey::try_new::<PushdownParityEntity>(Ulid::from_u128(id))
        .expect("pushdown data key should build")
        .to_raw()
        .expect("pushdown data key should encode");

    TEST_DATA_STORE.with(|store| {
        let removed = store.borrow_mut().remove(&raw_key);
        assert!(
            removed.is_some(),
            "expected pushdown row to exist before data-only removal",
        );
    });
}

fn seed_simple_entities(rows: &[u128]) {
    reset_store();
    let save = SaveExecutor::<SimpleEntity>::new(DB, false);

    for id in rows {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(*id),
        })
        .expect("aggregate optimization simple seed save should succeed");
    }
}

fn seed_indexed_metrics_rows(rows: &[(u128, u32, &str)]) {
    reset_store();
    let save = SaveExecutor::<IndexedMetricsEntity>::new(DB, false);

    for (id, tag, label) in rows {
        save.insert(IndexedMetricsEntity {
            id: Ulid::from_u128(*id),
            tag: *tag,
            label: (*label).to_string(),
        })
        .expect("aggregate optimization indexed-metrics seed save should succeed");
    }
}

fn u32_eq_predicate_strict(field: &str, value: u32) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::Eq,
        Value::Nat64(u64::from(value)),
        CoercionId::Strict,
    ))
}

fn u32_in_predicate_strict(field: &str, values: &[u32]) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::In,
        Value::List(
            values
                .iter()
                .map(|value| Value::Nat64(u64::from(*value)))
                .collect(),
        ),
        CoercionId::Strict,
    ))
}

#[test]
fn aggregate_optimizations_bytes_by_strict_mode_surfaces_missing_row_corruption() {
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
    let err = load
        .bytes_by_slot(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Error)
                .filter_predicate(u32_eq_predicate_strict("group", 7))
                .order_term(crate::db::asc("rank"))
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("strict bytes_by plan should build"),
            field_slot_for_test::<PushdownParityEntity>("rank"),
        )
        .expect_err("strict bytes_by should fail on missing primary rows");

    assert_eq!(
        err.class,
        ErrorClass::Corruption,
        "strict bytes_by must preserve missing-row corruption classification",
    );
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
        "strict bytes_by must preserve missing-row corruption diagnostics",
    );
}

#[test]
fn aggregate_optimizations_index_multi_lookup_count_uses_prefix_cardinality() {
    seed_indexed_metrics_rows(&[
        (8_711, 10, "a"),
        (8_712, 10, "b"),
        (8_713, 20, "c"),
        (8_714, 30, "d"),
    ]);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);

    let (count, scanned) = capture_rows_scanned_for_entity(IndexedMetricsEntity::PATH, || {
        execute_count_terminal(
            &load,
            Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
                .filter_predicate(u32_in_predicate_strict("tag", &[10, 30]))
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("indexed IN COUNT plan should build"),
        )
        .expect("indexed IN COUNT should succeed")
    });

    assert_eq!(
        count, 3,
        "indexed IN COUNT should sum exact prefix cardinalities",
    );
    assert_eq!(
        scanned, 0,
        "indexed IN COUNT should not scan rows when prefix cardinality is synchronized",
    );
}

#[test]
fn aggregate_optimizations_index_multi_lookup_exists_uses_prefix_cardinality() {
    seed_indexed_metrics_rows(&[
        (8_716, 10, "a"),
        (8_717, 10, "b"),
        (8_718, 20, "c"),
        (8_719, 30, "d"),
    ]);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);

    let plan = Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(u32_in_predicate_strict("tag", &[10, 30]))
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("indexed IN EXISTS plan should build");
    let range_scans_before = IndexStore::current_range_scan_call_count();
    let (exists, scanned) = capture_rows_scanned_for_entity(IndexedMetricsEntity::PATH, || {
        execute_exists_terminal(&load, plan).expect("indexed IN EXISTS should succeed")
    });
    let range_scans =
        IndexStore::current_range_scan_call_count().saturating_sub(range_scans_before);

    assert!(
        exists,
        "indexed IN EXISTS should answer true from prefix cardinality",
    );
    assert_eq!(
        scanned, 0,
        "indexed IN EXISTS should not scan rows when prefix cardinality is synchronized",
    );
    assert_eq!(
        range_scans, 0,
        "indexed IN EXISTS should not open an index range when prefix cardinality is synchronized",
    );
}

#[test]
fn aggregate_optimizations_empty_index_prefix_exists_uses_prefix_cardinality() {
    seed_indexed_metrics_rows(&[(8_721, 10, "a"), (8_722, 10, "b"), (8_723, 20, "c")]);
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);

    let plan = Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(u32_eq_predicate_strict("tag", 250))
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("empty indexed EXISTS plan should build");
    let range_scans_before = IndexStore::current_range_scan_call_count();
    let (exists, scanned) = capture_rows_scanned_for_entity(IndexedMetricsEntity::PATH, || {
        execute_exists_terminal(&load, plan).expect("empty indexed EXISTS should succeed")
    });
    let range_scans =
        IndexStore::current_range_scan_call_count().saturating_sub(range_scans_before);

    assert!(
        !exists,
        "empty indexed EXISTS should return false from prefix cardinality",
    );
    assert_eq!(
        scanned, 0,
        "empty indexed EXISTS should not scan rows when prefix cardinality is synchronized",
    );
    assert_eq!(
        range_scans, 0,
        "empty indexed EXISTS should not open an index range when prefix cardinality is synchronized",
    );
}

#[test]
fn aggregate_optimizations_by_ids_count_dedups_before_windowing() {
    seed_simple_entities(&[8_651, 8_652, 8_653, 8_654, 8_655]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        execute_count_terminal(
            &load,
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_ids([
                    Ulid::from_u128(8_654),
                    Ulid::from_u128(8_652),
                    Ulid::from_u128(8_652),
                    Ulid::from_u128(8_651),
                ])
                .order_term(crate::db::asc("id"))
                .offset(1)
                .limit(1)
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("by_ids dedup COUNT plan should build"),
        )
        .expect("by_ids dedup COUNT should succeed")
    });

    assert_eq!(count, 1, "by_ids dedup COUNT should keep one in-window row");
    assert_eq!(
        scanned, 2,
        "ordered by_ids dedup COUNT should scan only offset + limit rows",
    );
}

#[test]
fn aggregate_optimizations_by_ids_count_desc_window_preserves_scan_budget() {
    seed_simple_entities(&[8_656, 8_657, 8_658, 8_659, 8_660]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        execute_count_terminal(
            &load,
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .by_ids([
                    Ulid::from_u128(8_659),
                    Ulid::from_u128(8_657),
                    Ulid::from_u128(8_657),
                    Ulid::from_u128(8_656),
                ])
                .order_term(crate::db::desc("id"))
                .offset(1)
                .limit(1)
                .plan()
                .map(crate::db::executor::PreparedExecutionPlan::from)
                .expect("ordered by_ids DESC COUNT plan should build"),
        )
        .expect("ordered by_ids DESC COUNT should succeed")
    });

    assert_eq!(
        count, 1,
        "ordered by_ids DESC COUNT should keep one in-window row",
    );
    assert_eq!(
        scanned, 2,
        "ordered by_ids DESC COUNT should scan only offset + limit rows",
    );
}

#[test]
fn aggregate_optimizations_unordered_by_ids_count_preserves_canonical_dedup() {
    seed_simple_entities(&[8_701, 8_702, 8_703, 8_704]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let count = execute_count_terminal(
        &load,
        Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
            .by_ids([
                Ulid::from_u128(8_704),
                Ulid::from_u128(8_702),
                Ulid::from_u128(8_702),
                Ulid::from_u128(8_701),
            ])
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("unordered by-ids COUNT plan should build"),
    )
    .expect("unordered by-ids COUNT should succeed");

    assert_eq!(
        count, 3,
        "unordered by-ids COUNT should preserve canonical dedup semantics",
    );
}

#[test]
fn aggregate_optimizations_secondary_aggregate_explain_tracks_covering_projection() {
    let covering_exists = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(u32_eq_predicate_strict("group", 7))
        .explain_aggregate_terminal(aggregate::exists())
        .expect("strict-compatible EXISTS explain should build");
    let ordered_exists = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(u32_eq_predicate_strict("group", 7))
        .order_term(crate::db::asc("rank"))
        .explain_aggregate_terminal(aggregate::exists())
        .expect("ordered EXISTS explain should build");
    let uncertain_exists = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::And(vec![
            u32_eq_predicate_strict("group", 7),
            Predicate::TextContains {
                field: "label".to_string(),
                value: Value::Text("keep".to_string()),
            },
        ]))
        .explain_aggregate_terminal(aggregate::exists())
        .expect("strict-uncertain EXISTS explain should build");
    let covering_count = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(u32_eq_predicate_strict("group", 7))
        .explain_aggregate_terminal(aggregate::count())
        .expect("strict-compatible COUNT explain should build");
    let ordered_count = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(u32_eq_predicate_strict("group", 7))
        .order_term(crate::db::asc("rank"))
        .explain_aggregate_terminal(aggregate::count())
        .expect("ordered COUNT explain should build");

    assert!(
        covering_exists.execution().covering_projection(),
        "strict secondary EXISTS explain should mark covering projection",
    );
    assert!(
        !ordered_exists.execution().covering_projection(),
        "ordered EXISTS explain should fall back from covering projection",
    );
    assert!(
        !uncertain_exists.execution().covering_projection(),
        "strict-uncertain EXISTS explain should fall back from covering projection",
    );
    assert!(
        covering_count.execution().covering_projection(),
        "strict secondary COUNT explain should mark covering projection",
    );
    assert!(
        !ordered_count.execution().covering_projection(),
        "ordered COUNT explain should fall back from covering projection",
    );
}
