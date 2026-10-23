//! Ordered repeats use canonical equality; only newly retained keys need hashes.

use super::*;
use crate::{
    db::{query::plan::GroupField, schema::AcceptedFieldKind},
    value::{test_hash_budget_error, with_test_hash_override},
};
use std::panic::AssertUnwindSafe;

#[test]
fn ordered_direct_repeats_do_not_hash_their_canonical_values() {
    let cases = [
        vec![Value::Nat64(7)],
        vec![Value::Null, Value::Decimal(Decimal::new(100, 2))],
        vec![Value::List(vec![Value::Map(vec![(
            Value::Text("key".into()),
            Value::Decimal(Decimal::new(100, 2)),
        )])])],
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
            with_test_hash_override(
                Err(test_hash_budget_error),
                AssertUnwindSafe(|| {
                    assert!(
                        !fold
                            .ingest_row(
                                &mut context,
                                &data_key(2),
                                &row,
                                &fields,
                                direction,
                                |_| { panic!("equal key cannot close a group") }
                            )
                            .unwrap()
                    );
                }),
            );
            fold.finish(&mut context, |group| {
                let (key, count) = group.finalize_single()?;
                assert_eq!(key, GroupKey::from_group_values(values.clone())?);
                assert_eq!(count, Value::Nat64(2));
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
