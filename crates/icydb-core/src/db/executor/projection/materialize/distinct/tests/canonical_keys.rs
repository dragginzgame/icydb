//! Canonical DISTINCT equality does not require a hash; global buckets still do.

use super::*;
use crate::{
    types::Decimal,
    value::{test_hash_budget_error, with_test_hash_override},
};

#[test]
fn adjacent_distinct_preserves_canonical_keys_without_hashing() {
    let first = vec![
        Value::Null,
        Value::Decimal(Decimal::new(100, 2)),
        Value::List(vec![Value::Map(vec![
            (Value::Text("z".into()), Value::Nat64(9)),
            (Value::Text("a".into()), Value::Decimal(Decimal::new(10, 1))),
        ])]),
    ];
    let second = vec![
        Value::Null,
        Value::Decimal(Decimal::new(1, 0)),
        Value::List(vec![Value::Map(vec![
            (Value::Text("a".into()), Value::Decimal(Decimal::new(1, 0))),
            (Value::Text("z".into()), Value::Nat64(9)),
        ])]),
    ];
    with_test_hash_override(Err(test_hash_budget_error), || {
        let page = collect(
            ProjectionDistinctStrategy::OrderedAdjacent,
            ProjectionDistinctWindow::new(0, None),
            vec![
                DistinctProjectedRow::new(RowView::owned(first.clone()), Some(boundary(1))),
                DistinctProjectedRow::new(RowView::owned(second), Some(boundary(2))),
            ],
        )
        .expect("adjacent equality must not need a hash");
        let (rows, last_emitted, has_more) = page.into_parts();
        assert_eq!(text_rows(rows), vec![first]);
        assert_eq!(last_emitted, Some(boundary(2)));
        assert!(!has_more);
    });
}

#[test]
fn distinct_canonical_values_match_group_keys_and_reject_invalid_maps() {
    let cases = [
        vec![],
        vec![Value::Null, Value::Nat64(7), Value::Int64(-7)],
        vec![Value::Decimal(Decimal::new(100, 2))],
        vec![Value::List(vec![Value::Decimal(Decimal::new(10, 1))])],
        vec![Value::Map(vec![
            (Value::Text("z".into()), Value::Nat64(9)),
            (Value::Text("a".into()), Value::Decimal(Decimal::new(10, 1))),
        ])],
    ];
    for values in cases {
        let expected = GroupKey::from_group_values(values.clone())
            .unwrap()
            .into_canonical_value();
        with_test_hash_override(Err(test_hash_budget_error), || {
            let canonical = canonical_projected_row(&RowView::owned(values)).unwrap();
            assert_eq!(canonical, expected);
        });
    }
    let row = RowView::owned(vec![Value::Map(vec![
        (Value::Decimal(Decimal::new(10, 1)), Value::Nat64(1)),
        (Value::Decimal(Decimal::new(1, 0)), Value::Nat64(2)),
    ])]);
    let expected = GroupKey::from_group_values(row.values().to_vec()).unwrap_err();
    let error = canonical_projected_row(&row).unwrap_err();
    assert_eq!(error.diagnostic(), expected.diagnostic());
    assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
}

#[test]
fn global_distinct_preserves_equality_under_hash_collisions() {
    with_test_hash_override(Ok([0; 16]), || {
        let mut rows = DistinctProjectionRowSet::new();
        for value in [Value::Nat64(1), Value::Nat64(2), Value::Null] {
            let row = RowView::owned(vec![value]);
            assert!(rows.insert_row(&row).unwrap());
            assert!(!rows.insert_row(&row).unwrap());
        }
        for value in [1, 2] {
            let row = RowView::owned(vec![Value::Map(vec![(
                Value::Text("key".into()),
                Value::Nat64(value),
            )])]);
            assert!(rows.insert_row(&row).unwrap());
            assert!(!rows.insert_row(&row).unwrap());
        }
        assert_eq!(rows.buckets.len(), 1);
        assert_eq!(rows.entries, 5);
    });
}
