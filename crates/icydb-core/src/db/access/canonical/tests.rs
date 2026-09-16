//! Canonical text order and owned normalization preserve access identity.

use super::*;
use crate::db::{
    access::{SemanticIndexExpression, path::SemanticIndexAccessContractInner},
    schema::PersistedIndexExpressionOp,
};
use std::sync::Arc;

fn index(
    name: &str,
    ordinal: u16,
    fields: Vec<SemanticIndexKeyItem>,
) -> SemanticIndexAccessContract {
    SemanticIndexAccessContract {
        inner: Arc::new(SemanticIndexAccessContractInner {
            ordinal,
            physical_generation: 1,
            name: name.into(),
            store_path: "test".into(),
            key_items: fields,
            unique: false,
            predicate_semantics: None,
        }),
    }
}

#[test]
fn borrowed_key_order_matches_rendered_labels_and_sequence_boundaries() {
    use PersistedIndexExpressionOp::{Date, Day, Lower, LowerTrim, Month, Trim, Upper, Year};

    let mut items = Vec::new();
    for field in ["", "a", "a(", "a)", "a\0", "账户.名", "LOWER(a)"] {
        items.push(SemanticIndexKeyItem::Field(field.into()));
        for op in [Lower, Upper, Trim, LowerTrim, Date, Year, Month, Day] {
            let expression = SemanticIndexExpression::new(op, field.into());
            // A field whose text equals an expression label compares equal;
            // canonical identity is lexical, not the key-item discriminant.
            items.push(SemanticIndexKeyItem::Field(
                expression.canonical_order_text(),
            ));
            items.push(SemanticIndexKeyItem::Expression(expression));
        }
    }
    let labels: Vec<_> = items
        .iter()
        .map(|item| item.as_ref().canonical_text())
        .collect();
    for (left, left_label) in items.iter().zip(&labels) {
        for (right, right_label) in items.iter().zip(&labels) {
            assert_eq!(
                canonical_cmp_index_key_items(
                    std::slice::from_ref(left),
                    std::slice::from_ref(right)
                ),
                left_label.cmp(right_label),
            );
        }
    }
    let lists = [
        vec![],
        vec![SemanticIndexKeyItem::Field("a".into())],
        vec![SemanticIndexKeyItem::Field("ab".into())],
        vec![
            SemanticIndexKeyItem::Field("a".into()),
            SemanticIndexKeyItem::Field("b".into()),
        ],
        vec![
            SemanticIndexKeyItem::Field("a".into()),
            SemanticIndexKeyItem::Field(String::new()),
        ],
    ];
    for left in &lists {
        for right in &lists {
            let render = |items: &[SemanticIndexKeyItem]| {
                items
                    .iter()
                    .map(|item| item.as_ref().canonical_text())
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                canonical_cmp_index_key_items(left, right),
                render(left).cmp(&render(right))
            );
        }
    }
}

#[test]
fn index_identity_keeps_name_then_ordinal_then_label_precedence() {
    let a = index("a", 9, vec![SemanticIndexKeyItem::Field("z".into())]);
    let b = index("b", 0, vec![SemanticIndexKeyItem::Field("a".into())]);
    assert_eq!(canonical_cmp_index_identity(&a, &b), Ordering::Less);
    let lower_ordinal = index("a", 8, vec![SemanticIndexKeyItem::Field("zz".into())]);
    assert_eq!(
        canonical_cmp_index_identity(&lower_ordinal, &a),
        Ordering::Less
    );
    let lower_label = index("a", 9, vec![SemanticIndexKeyItem::Field("y".into())]);
    assert_eq!(
        canonical_cmp_index_identity(&lower_label, &a),
        Ordering::Less
    );
}

#[test]
fn singleton_normalization_moves_payloads_and_reuses_lookup_backing() {
    let contract = index(
        "by_name",
        0,
        vec![SemanticIndexKeyItem::Field("name".into())],
    );
    for kind in 0..3 {
        let value = Value::Text("账户".repeat(512));
        let Value::Text(text) = &value else {
            unreachable!()
        };
        let payload = text.as_ptr();
        let values = vec![value];
        let backing = values.as_ptr();
        let path = match kind {
            0 => AccessPath::ByKeys(values),
            1 => AccessPath::IndexMultiLookup {
                index: contract.clone(),
                values,
            },
            _ => AccessPath::IndexBranchSet {
                spec: IndexBranchSetSpec::from_primary_key_asc_contract(
                    contract.clone(),
                    vec![Value::Nat64(7)],
                    values,
                ),
            },
        };
        let normalized = path.normalize_for_access();
        let value = match &normalized {
            AccessPath::ByKey(value) if kind == 0 => value,
            AccessPath::IndexPrefix { values, .. } => {
                if kind == 1 {
                    assert_eq!(values.as_ptr(), backing);
                    assert_eq!(values.len(), 1);
                } else {
                    assert_eq!(values[0], Value::Nat64(7));
                    assert_eq!(values.len(), 2);
                }
                values.last().unwrap()
            }
            other => panic!("unexpected normalized path: {other:?}"),
        };
        let Value::Text(text) = value else {
            unreachable!()
        };
        assert_eq!(text.as_ptr(), payload);
        assert_eq!(text, &"账户".repeat(512));
        assert_eq!(normalized.clone().normalize_for_access(), normalized);
    }
}

#[test]
fn normalization_preserves_empty_duplicate_and_composite_shapes() {
    let contract = index(
        "by_name",
        0,
        vec![SemanticIndexKeyItem::Field("name".into())],
    );
    for values in [
        vec![],
        vec![Value::Text("a".into()); 2],
        vec![Value::Nat64(2), Value::Nat64(1), Value::Nat64(2)],
    ] {
        let mut expected = values.clone();
        canonicalize_value_set(&mut expected);
        let multi = AccessPath::IndexMultiLookup {
            index: contract.clone(),
            values: values.clone(),
        }
        .normalize_for_access();
        let expected_path = if expected.len() == 1 {
            AccessPath::IndexPrefix {
                index: contract.clone(),
                values: expected.clone(),
            }
        } else {
            AccessPath::IndexMultiLookup {
                index: contract.clone(),
                values: expected.clone(),
            }
        };
        assert_eq!(multi, expected_path);
        let key_plan = normalize_access_plan_value(AccessPlan::by_keys(values));
        let expected_plan = if expected.len() == 1 {
            AccessPlan::by_key(expected.pop().unwrap())
        } else {
            AccessPlan::by_keys(expected)
        };
        assert_eq!(key_plan, expected_plan);
        let union = AccessPlan::Union(vec![key_plan.clone(), key_plan.clone()]);
        assert_eq!(normalize_access_plan_value(union), key_plan);
    }
}

#[test]
fn singleton_lookup_does_not_retain_spare_operand_slots() {
    let contract = index(
        "by_name",
        0,
        vec![SemanticIndexKeyItem::Field("name".into())],
    );
    let mut values = Vec::with_capacity(4096);
    values.push(Value::Text("kept".repeat(128)));
    let Value::Text(text) = &values[0] else {
        unreachable!()
    };
    let payload = text.as_ptr();
    let path = AccessPath::IndexMultiLookup {
        index: contract,
        values,
    }
    .normalize_for_access();
    let AccessPath::IndexPrefix { values, .. } = path else {
        unreachable!()
    };
    assert_eq!(values.len(), 1);
    assert_eq!(values.capacity(), 1);
    let Value::Text(text) = &values[0] else {
        unreachable!()
    };
    assert_eq!(text.as_ptr(), payload);
}
