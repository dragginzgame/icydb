//! Ordered repeats use canonical equality; only newly retained keys need hashes.

use super::*;
use crate::{
    db::{
        executor::aggregate::runtime::grouped_fold::bundle::OrderedGroupFoldState,
        query::plan::GroupField, schema::AcceptedFieldKind,
    },
    value::{test_hash_budget_error, with_test_hash_override},
};
use std::{cell::Cell, panic::AssertUnwindSafe};

#[test]
fn ordered_direct_repeats_do_not_hash_their_canonical_values() {
    let cases = [
        vec![Value::Nat64(7)],
        vec![Value::Null, Value::Decimal(Decimal::new(100, 2))],
        vec![Value::Text("payload".repeat(1024))],
        vec![Value::List(vec![Value::Map(vec![
            (Value::Text("z".into()), Value::Blob(vec![7; 4096])),
            (
                Value::Text("a".into()),
                Value::Decimal(Decimal::new(100, 2)),
            ),
        ])])],
    ];
    for values in cases {
        for direction in [Direction::Asc, Direction::Desc] {
            let mut fold = count_rows_fold();
            let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
            let fields = (0..values.len())
                .map(|slot| FieldSlot::from_test_slot(slot, format!("group_{slot}")))
                .collect::<Vec<_>>();
            let row = RowView::new(values.iter().cloned().map(Some).collect());
            fold.ingest_row(&mut context, &data_key(1), &row, &fields, direction, |_| {
                panic!("first row cannot close a group")
            })
            .unwrap();
            let Value::List(canonical_values) = GroupKey::from_group_values(values.clone())
                .unwrap()
                .into_canonical_value()
            else {
                panic!("group tuples must remain lists");
            };
            let canonical_row = RowView::new(canonical_values.into_iter().map(Some).collect());
            with_test_hash_override(
                Err(test_hash_budget_error),
                AssertUnwindSafe(|| {
                    for repeat in [&row, &canonical_row] {
                        assert!(
                            !fold
                                .ingest_row(
                                    &mut context,
                                    &data_key(2),
                                    repeat,
                                    &fields,
                                    direction,
                                    |_| { panic!("equal key cannot close a group") }
                                )
                                .unwrap()
                        );
                    }
                }),
            );
            fold.finish(&mut context, |group| {
                let (key, count) = group.finalize_single()?;
                assert_eq!(key, GroupKey::from_group_values(values.clone())?);
                assert_eq!(count, Value::Nat64(3));
                Ok(false)
            })
            .unwrap();
            assert_eq!(context.budget().estimated_bytes(), 0);
        }
    }
}

#[test]
fn ordered_path_repeats_do_not_hash_single_or_tuple_keys() {
    for tuple in [false, true] {
        for direction in [Direction::Asc, Direction::Desc] {
            let path = GroupField::scalar_path_for_test(
                "profile.amount",
                "profile",
                vec!["amount".into()],
                0,
                AcceptedFieldKind::Decimal { scale: 2 },
            );
            let fields = if tuple {
                vec![
                    path,
                    GroupField::Direct(FieldSlot::from_test_slot(1, "other")),
                ]
            } else {
                vec![path]
            };
            let row = |amount| {
                RowView::new(vec![
                    Some(Value::Map(vec![(
                        Value::Text("amount".into()),
                        Value::Decimal(amount),
                    )])),
                    Some(Value::Null),
                ])
            };
            let mut fold = count_rows_fold();
            let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
            fold.ingest_path_row(
                &mut context,
                &data_key(1),
                &row(Decimal::new(100, 2)),
                &fields,
                direction,
                |_| panic!("first row cannot close a group"),
            )
            .unwrap();
            with_test_hash_override(
                Err(test_hash_budget_error),
                AssertUnwindSafe(|| {
                    assert!(
                        !fold
                            .ingest_path_row(
                                &mut context,
                                &data_key(2),
                                &row(Decimal::new(1, 0)),
                                &fields,
                                direction,
                                |_| { panic!("equivalent decimal cannot close a group") }
                            )
                            .unwrap()
                    );
                }),
            );
            fold.finish(&mut context, |group| {
                assert_eq!(group.finalize_single()?.1, Value::Nat64(2));
                Ok(false)
            })
            .unwrap();
            assert_eq!(context.budget().estimated_bytes(), 0);
        }
    }
}

#[test]
fn ordered_transition_hashes_only_a_new_group_that_will_be_retained() {
    for stop in [false, true] {
        let mut fold = count_rows_fold();
        let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
        let fields = [FieldSlot::from_test_slot(0, "group")];
        fold.ingest_row(
            &mut context,
            &data_key(1),
            &row(10),
            &fields,
            Direction::Asc,
            |_| panic!("first row cannot close a group"),
        )
        .unwrap();
        let result = with_test_hash_override(
            Err(test_hash_budget_error),
            AssertUnwindSafe(|| {
                fold.ingest_row(
                    &mut context,
                    &data_key(2),
                    &row(20),
                    &fields,
                    Direction::Asc,
                    |group| {
                        assert_eq!(group.finalize_single()?.1, Value::Nat64(1));
                        Ok(stop)
                    },
                )
            }),
        );
        if stop {
            assert!(result.unwrap(), "unused lookahead must not require hashing");
            assert_eq!(context.budget().groups(), 1);
        } else {
            let error = result.unwrap_err().into_internal_error();
            let expected = test_hash_budget_error();
            assert_eq!(error.diagnostic(), expected.diagnostic());
            assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
        }
        assert_eq!(context.budget().estimated_bytes(), 0);
    }
}

#[test]
fn ordered_transition_materializes_only_retained_groups() {
    for direction in [Direction::Asc, Direction::Desc] {
        for stop in [false, true] {
            let mut transitions = OrderedGroupFoldState::new(1);
            let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
            let values = match direction {
                Direction::Asc => [10, 10, 20],
                Direction::Desc => [20, 20, 10],
            };
            let mut closed = Vec::new();
            for (index, value) in values.into_iter().enumerate() {
                let value = Value::Nat64(value);
                let input_passes = Cell::new(0);
                let stopped = transitions
                    .apply_row(
                        &mut context,
                        || {
                            input_passes.set(input_passes.get() + 1);
                            std::iter::once(Ok(&value))
                        },
                        direction,
                        || 0_u32,
                        |count, _| {
                            *count += 1;
                            Ok(())
                        },
                        |key, count| {
                            closed.push((key, count));
                            Ok(stop)
                        },
                    )
                    .unwrap();
                assert_eq!(stopped, stop && index == 2);
                // Only an opened group requests an owned key from the input.
                assert_eq!(
                    input_passes.get(),
                    if index == 0 || (index == 2 && !stop) {
                        2
                    } else {
                        1
                    }
                );
            }
            transitions
                .finish(&mut context, |key, count| {
                    closed.push((key, count));
                    Ok(false)
                })
                .unwrap();
            let expected_counts = if stop { vec![2] } else { vec![2, 1] };
            assert_eq!(
                closed.iter().map(|(_, count)| *count).collect::<Vec<_>>(),
                expected_counts
            );
            assert_eq!(
                closed[0].0.canonical_value(),
                &Value::List(vec![Value::Nat64(values[0])])
            );
            assert_eq!(context.budget().estimated_bytes(), 0);
        }
    }
}

#[test]
fn ordered_borrowed_rows_validate_later_components_before_transition() {
    let invalid_maps = [
        Value::Map(vec![(Value::Null, Value::Unit)]),
        Value::Map(vec![(Value::List(Vec::new()), Value::Unit)]),
        Value::Map(vec![
            (Value::Decimal(Decimal::new(100, 2)), Value::Unit),
            (Value::Decimal(Decimal::new(1, 0)), Value::Unit),
        ]),
        Value::List(vec![Value::Map(vec![(Value::Null, Value::Unit)])]),
    ];
    for invalid in invalid_maps {
        let expected = GroupKey::from_group_values(vec![Value::Nat64(10), invalid.clone()])
            .unwrap_err()
            .diagnostic();
        for direction in [Direction::Asc, Direction::Desc] {
            for first in [5, 10, 20] {
                let mut fold = count_rows_fold();
                let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
                let fields = [
                    FieldSlot::from_test_slot(0, "first"),
                    FieldSlot::from_test_slot(1, "second"),
                ];
                fold.ingest_row(
                    &mut context,
                    &data_key(1),
                    &RowView::new(vec![Some(Value::Nat64(10)), Some(Value::Unit)]),
                    &fields,
                    direction,
                    |_| panic!("first row cannot close a group"),
                )
                .unwrap();
                let error = fold
                    .ingest_row(
                        &mut context,
                        &data_key(2),
                        &RowView::new(vec![Some(Value::Nat64(first)), Some(invalid.clone())]),
                        &fields,
                        direction,
                        |_| panic!("invalid key cannot close a group"),
                    )
                    .unwrap_err()
                    .into_internal_error();
                assert_eq!(error.diagnostic(), expected);
                fold.finish(&mut context, |group| {
                    assert_eq!(group.finalize_single()?.1, Value::Nat64(1));
                    Ok(false)
                })
                .unwrap();
                assert_eq!(context.budget().estimated_bytes(), 0);
            }
        }
    }
}
