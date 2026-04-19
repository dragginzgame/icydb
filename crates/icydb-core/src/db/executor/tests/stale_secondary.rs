//! Module: db::executor::tests::stale_secondary
//! Covers stale secondary-index handling and recovery behavior.
//! Does not own: unrelated executor behavior outside missing-row reconciliation.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::support::*;
use crate::{
    db::{
        data::DataKey,
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::explain::ExplainAccessPath,
    },
    value::Value,
};

// Remove one pushdown row from the primary store while keeping index entries.
fn remove_pushdown_row_data(id: u128) {
    let raw_key = DataKey::try_new::<PushdownParityEntity>(Ulid::from_u128(id))
        .expect("pushdown data key should build")
        .to_raw()
        .expect("pushdown data key should encode");

    DATA_STORE.with(|store| {
        let removed = store.borrow_mut().remove(&raw_key);
        assert!(
            removed.is_some(),
            "expected pushdown row to exist before data-only removal"
        );
    });
}

#[test]
fn load_secondary_index_missing_ok_skips_stale_keys_by_reading_primary_rows() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PushdownParityEntity>::new(DB, false);
    for (id, group, rank) in [(7101_u128, 7_u32, 10_u32), (7102, 7, 20), (7103, 7, 30)] {
        save.insert(PushdownParityEntity {
            id: Ulid::from_u128(id),
            group,
            rank,
            label: format!("g{group}-r{rank}"),
        })
        .expect("seed pushdown row save should succeed");
    }

    remove_pushdown_row_data(7101);

    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "group",
        CompareOp::Eq,
        Value::Uint(7),
        CoercionId::Strict,
    ));
    let explain = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(predicate.clone())
        .order_term(crate::db::asc("rank"))
        .explain()
        .expect("missing-ok stale-secondary explain should build");
    assert!(
        matches!(explain.access(), ExplainAccessPath::IndexPrefix { .. }),
        "group equality with rank order should plan as secondary index-prefix access",
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(predicate)
        .order_term(crate::db::asc("rank"))
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("missing-ok stale-secondary load plan should build");
    let response = load
        .execute(plan)
        .expect("missing-ok stale-secondary load should succeed");
    let ids: Vec<Ulid> = response
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();

    assert_eq!(
        ids,
        vec![Ulid::from_u128(7102), Ulid::from_u128(7103)],
        "Ignore must filter stale secondary keys instead of materializing missing rows",
    );
}

#[test]
fn load_secondary_index_strict_missing_row_surfaces_corruption() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PushdownParityEntity>::new(DB, false);
    for (id, group, rank) in [(7201_u128, 7_u32, 10_u32), (7202, 7, 20), (7203, 7, 30)] {
        save.insert(PushdownParityEntity {
            id: Ulid::from_u128(id),
            group,
            rank,
            label: format!("g{group}-r{rank}"),
        })
        .expect("seed pushdown row save should succeed");
    }

    remove_pushdown_row_data(7201);

    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "group",
        CompareOp::Eq,
        Value::Uint(7),
        CoercionId::Strict,
    ));
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Error)
        .filter_predicate(predicate)
        .order_term(crate::db::asc("rank"))
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("strict stale-secondary load plan should build");
    let err = load
        .execute(plan)
        .expect_err("strict stale-secondary load should fail on missing primary row");

    assert_eq!(
        err.class,
        crate::error::ErrorClass::Corruption,
        "strict stale-secondary load must classify missing primary rows as corruption",
    );
    assert!(
        err.message.contains("missing row"),
        "strict stale-secondary failure should report missing-row corruption",
    );
}
