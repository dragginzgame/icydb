mod ordered_hashing;

use super::{GroupedAggregateBundle, GroupedAggregateBundleSpec, OrderedGroupedAggregateFold};
use crate::{
    db::{
        data::{DecodedDataStoreKey, PrimaryKeyComponent},
        direction::Direction,
        executor::{
            aggregate::{
                AggregateKind, CompiledExpr, ExecutionConfig, ExecutionContext, GroupError,
                contracts::GroupedDistinctExecutionMode,
                runtime::grouped_fold::utils::GroupIndexBucket,
            },
            group::GroupKey,
            pipeline::runtime::RowView,
        },
        query::plan::FieldSlot,
        resource::budget::runtime_value_work,
    },
    error::InternalError,
    types::{Decimal, EntityTag},
    value::Value,
};
use std::rc::Rc;

fn owned_count_bundle() -> GroupedAggregateBundle {
    GroupedAggregateBundle::new(
        (0..2)
            .map(|_| {
                GroupedAggregateBundleSpec::new(
                    AggregateKind::Count,
                    GroupedDistinctExecutionMode::new(false, false),
                    None,
                    None,
                    None,
                    u64::MAX,
                )
                .unwrap()
            })
            .collect(),
    )
}

#[test]
fn owned_groups_reuse_unit_collection_and_composite_value_keys_within_group_limits() {
    for value in [
        Value::Unit,
        Value::List(vec![Value::Nat64(7)]),
        Value::Map(vec![(
            Value::Text("member".into()),
            Value::List(vec![Value::Nat64(7)]),
        )]),
    ] {
        let mut bundle = owned_count_bundle();
        let mut context = ExecutionContext::new(ExecutionConfig::with_hard_limits(1, 64 * 1024));
        let fields = [FieldSlot::from_test_slot(0, "group")];
        for id in 1..=3 {
            bundle
                .ingest_row_with_owned_group_key(
                    &mut context,
                    &data_key(id),
                    &RowView::new(vec![Some(value.clone())]),
                    &fields,
                )
                .unwrap();
        }
        assert_eq!(context.budget().groups(), 1);
        assert_eq!(context.budget().aggregate_states(), 2);
        let finalized = bundle
            .into_groups()
            .into_iter()
            .map(|group| group.finalize(2).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(finalized.len(), 1);
        assert_eq!(finalized[0].1, [Value::Nat64(3), Value::Nat64(3)]);
    }
}

#[test]
fn owned_groups_use_canonical_nested_numeric_equality_for_multi_field_keys() {
    let mut bundle = owned_count_bundle();
    let mut context = ExecutionContext::new(ExecutionConfig::with_hard_limits(2, 64 * 1024));
    let fields = [
        FieldSlot::from_test_slot(0, "nested"),
        FieldSlot::from_test_slot(1, "other"),
    ];
    for (id, nested, other) in [
        (1, Value::Decimal(Decimal::new(700, 2)), Value::Unit),
        (2, Value::Decimal(Decimal::new(70, 1)), Value::Nat64(9)),
        (3, Value::Decimal(Decimal::new(70, 1)), Value::Unit),
        (4, Value::Decimal(Decimal::new(700, 2)), Value::Nat64(9)),
    ] {
        bundle
            .ingest_row_with_owned_group_key(
                &mut context,
                &data_key(id),
                &RowView::new(vec![Some(Value::List(vec![nested])), Some(other)]),
                &fields,
            )
            .unwrap();
    }
    assert_eq!(context.budget().groups(), 2);
    assert_eq!(context.budget().aggregate_states(), 4);
    for group in bundle.into_groups() {
        assert_eq!(
            group.finalize(2).unwrap().1,
            [Value::Nat64(2), Value::Nat64(2)]
        );
    }
}

#[test]
fn owned_groups_probe_collisions_and_preserve_typed_invalid_bucket_errors() {
    let mut bundle = owned_count_bundle();
    let mut context = ExecutionContext::new(ExecutionConfig::with_hard_limits(2, 64 * 1024));
    let fields = [FieldSlot::from_test_slot(0, "group")];
    let row = RowView::new(vec![Some(Value::Unit)]);
    let target = GroupKey::from_group_values(vec![Value::Unit]).unwrap();
    let hash = target.hash();
    // Force the same cached hash on unequal canonical values so lookup
    // must compare every candidate, rather than equating hashes.
    let collision =
        GroupKey::from_group_values_with_hash(vec![Value::List(vec![Value::Nat64(9)])], hash)
            .unwrap();
    assert_eq!(bundle.insert_new_group(collision, &mut context).unwrap(), 0);
    assert_eq!(
        bundle
            .resolve_owned_group_index(&mut context, &row, &fields)
            .unwrap(),
        1
    );
    assert_eq!(
        bundle
            .resolve_owned_group_index(&mut context, &row, &fields)
            .unwrap(),
        1
    );
    assert_eq!(context.budget().groups(), 2);
    bundle
        .bucket_index
        .insert(hash, GroupIndexBucket::single(usize::MAX));
    let error = bundle
        .resolve_owned_group_index(&mut context, &row, &fields)
        .unwrap_err()
        .into_internal_error();
    assert_eq!(
        error.diagnostic_code(),
        InternalError::query_executor_invariant().diagnostic_code()
    );
}

#[test]
fn grouped_bundle_extrema_and_key_terminals_preserve_results() {
    for (kind, input_expr, expected) in [
        (AggregateKind::Min, Some(CompiledExpr::Slot { slot: 0 }), 1),
        (AggregateKind::Max, Some(CompiledExpr::Slot { slot: 0 }), 9),
        (AggregateKind::Min, None, 1),
        (AggregateKind::Max, None, 9),
        (AggregateKind::First, None, 1),
        (AggregateKind::Last, None, 9),
    ] {
        let needs_row = input_expr.is_some();
        let spec = GroupedAggregateBundleSpec::new(
            kind,
            GroupedDistinctExecutionMode::new(false, false),
            None,
            input_expr,
            None,
            u64::MAX,
        )
        .unwrap();
        let mut state = spec.build_state();
        let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
        for value in [4, 9, 1, 6] {
            let row = RowView::new(vec![Some(Value::Nat64(value))]);
            let control = state
                .apply_with_row_view(&data_key(value), needs_row.then_some(&row), &mut context)
                .unwrap();
            assert!(matches!(
                control,
                crate::db::executor::aggregate::FoldControl::Continue
            ));
        }
        assert_eq!(state.finalize().unwrap(), Value::Nat64(expected));
    }
}

#[test]
fn shared_expressions_keep_group_reducers_and_distinct_sets_independent() {
    let spec = GroupedAggregateBundleSpec::new(
        AggregateKind::Count,
        GroupedDistinctExecutionMode::new(true, true),
        None,
        Some(CompiledExpr::Slot { slot: 0 }),
        Some(CompiledExpr::Slot { slot: 1 }),
        u64::MAX,
    )
    .unwrap();
    let mut groups = [spec.build_state(), spec.build_state()];
    assert_eq!(
        Rc::strong_count(spec.grouped_input_expr.as_ref().unwrap()),
        3
    );
    assert_eq!(
        Rc::strong_count(spec.grouped_filter_expr.as_ref().unwrap()),
        3
    );
    // Finalization may outlive the blueprint; sharing must not borrow it.
    drop(spec);

    let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
    for (group, value, admit) in [
        (0, Value::Nat64(10), true),
        (1, Value::Nat64(10), true),
        (0, Value::Nat64(10), true),
        (0, Value::Nat64(20), false),
        (0, Value::Nat64(20), true),
        (1, Value::Null, true),
        (1, Value::Nat64(30), false),
    ] {
        let row = RowView::new(vec![Some(value), Some(Value::Bool(admit))]);
        groups[group]
            .apply_with_row_view(&data_key(1), Some(&row), &mut context)
            .unwrap();
    }
    let [first, second] = groups;
    assert_eq!(first.finalize().unwrap(), Value::Nat64(2));
    assert_eq!(second.finalize().unwrap(), Value::Nat64(1));
}

fn data_key(value: u64) -> DecodedDataStoreKey {
    let raw =
        DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(value).into())
            .to_raw()
            .expect("ordered grouped test key should encode");

    DecodedDataStoreKey::try_from_raw(&raw).expect("ordered grouped test key should decode")
}

fn count_rows_fold() -> OrderedGroupedAggregateFold {
    let spec = GroupedAggregateBundleSpec::new(
        AggregateKind::Count,
        GroupedDistinctExecutionMode::new(false, false),
        None,
        None,
        None,
        u64::MAX,
    )
    .expect("ordered grouped COUNT state should build");

    OrderedGroupedAggregateFold::new(vec![spec])
}

fn row(group: u64) -> RowView {
    RowView::new(vec![Some(Value::Nat64(group))])
}

#[test]
fn ordered_grouped_fold_finalizes_transitions_and_releases_live_state() {
    let mut fold = count_rows_fold();
    let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
    let group_fields = [FieldSlot::from_test_slot(0, "group")];
    let mut finalized = Vec::new();

    for (key, group) in [(1, 10), (2, 10), (3, 20), (4, 20)] {
        let stopped = fold
            .ingest_row(
                &mut context,
                &data_key(key),
                &row(group),
                &group_fields,
                Direction::Asc,
                |closed| {
                    finalized.push(closed.finalize_single()?);
                    Ok(false)
                },
            )
            .expect("monotonic ordered grouped row should fold");
        assert!(!stopped);
    }
    fold.finish(&mut context, |closed| {
        finalized.push(closed.finalize_single()?);
        Ok(false)
    })
    .expect("final active ordered group should close");

    assert_eq!(
        finalized,
        vec![
            (
                crate::db::executor::group::GroupKey::from_group_values(vec![Value::Nat64(10)])
                    .expect("group key"),
                Value::Nat64(2)
            ),
            (
                crate::db::executor::group::GroupKey::from_group_values(vec![Value::Nat64(20)])
                    .expect("group key"),
                Value::Nat64(2)
            ),
        ],
    );
    assert_eq!(context.budget().groups(), 2);
    assert_eq!(context.budget().aggregate_states(), 2);
    assert_eq!(context.budget().estimated_bytes(), 0);
    assert!(context.budget().peak_estimated_bytes() > 0);
    let runtime_stats = context.successful_runtime_stats(false);
    assert_eq!(runtime_stats.groups_observed(), 2);
    assert_eq!(runtime_stats.groups_finalized(), 2);
    assert_eq!(runtime_stats.peak_live_groups(), 1);
    assert_eq!(runtime_stats.peak_live_aggregate_states(), 1);
    assert_eq!(runtime_stats.peak_live_distinct_values(), 0);
    assert!(!runtime_stats.early_scan_stop());
}

#[test]
fn ordered_grouped_fold_fails_closed_on_non_monotonic_group_keys() {
    let mut fold = count_rows_fold();
    let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
    let group_fields = [FieldSlot::from_test_slot(0, "group")];

    fold.ingest_row(
        &mut context,
        &data_key(1),
        &row(20),
        &group_fields,
        Direction::Asc,
        |_| Ok(false),
    )
    .expect("first ordered group should open");
    let err = fold
        .ingest_row(
            &mut context,
            &data_key(2),
            &row(10),
            &group_fields,
            Direction::Asc,
            |_| Ok(false),
        )
        .expect_err("descending key on ascending route must fail closed");

    assert!(matches!(err, GroupError::Internal(_)));
}

#[test]
fn ordered_grouped_fold_does_not_open_next_group_after_selection_stop() {
    let mut fold = count_rows_fold();
    let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
    let group_fields = [FieldSlot::from_test_slot(0, "group")];

    fold.ingest_row(
        &mut context,
        &data_key(1),
        &row(10),
        &group_fields,
        Direction::Asc,
        |_| Ok(false),
    )
    .expect("first ordered group should open");
    let stopped = fold
        .ingest_row(
            &mut context,
            &data_key(2),
            &row(20),
            &group_fields,
            Direction::Asc,
            |_| Ok(true),
        )
        .expect("selection stop should close the active group");

    assert!(stopped);
    assert_eq!(context.budget().groups(), 1);
    assert_eq!(context.budget().estimated_bytes(), 0);
    let runtime_stats = context.successful_runtime_stats(true);
    assert_eq!(runtime_stats.groups_observed(), 1);
    assert_eq!(runtime_stats.groups_finalized(), 1);
    assert_eq!(runtime_stats.peak_live_groups(), 1);
    assert!(runtime_stats.early_scan_stop());
}

#[test]
fn ordered_grouped_fold_accounts_owned_group_key_backing_and_releases_it() {
    let mut fold = count_rows_fold();
    let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
    let group_fields = [FieldSlot::from_test_slot(0, "group")];
    let group_value = Value::Text("large-group-key".repeat(512));
    let expected_key_bytes = runtime_value_work(&Value::List(vec![group_value.clone()])).0;

    fold.ingest_row(
        &mut context,
        &data_key(1),
        &RowView::new(vec![Some(group_value)]),
        &group_fields,
        Direction::Asc,
        |_| Ok(false),
    )
    .expect("large ordered group key should be admitted");
    assert!(context.budget().estimated_bytes() >= expected_key_bytes);

    fold.finish(&mut context, |_| Ok(false))
        .expect("large ordered group should close");
    assert_eq!(context.budget().estimated_bytes(), 0);
    assert!(
        context
            .successful_runtime_stats(false)
            .peak_estimated_state_bytes()
            >= expected_key_bytes
    );
}

#[test]
fn ordered_grouped_fold_rejects_a_key_larger_than_the_group_state_budget() {
    let mut fold = count_rows_fold();
    let mut context = ExecutionContext::new(ExecutionConfig::with_hard_limits(8, 1_024));
    let group_fields = [FieldSlot::from_test_slot(0, "group")];
    let err = fold
        .ingest_row(
            &mut context,
            &data_key(1),
            &RowView::new(vec![Some(Value::Text("oversize".repeat(512)))]),
            &group_fields,
            Direction::Asc,
            |_| Ok(false),
        )
        .expect_err("oversize retained group key must fail before insertion");

    assert!(matches!(err, GroupError::MemoryLimitExceeded { .. }));
}
