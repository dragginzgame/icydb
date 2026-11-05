//! Projection allocates only retained collection backing and rejects scalar arity drift.

use super::{K, P, catalog, snapshot};
use crate::{
    db::{
        predicate::{CoercionId, CompareOp, Predicate},
        schema::{AcceptedCheckLiteralV1, FieldId, check::bind_index_predicate_literal},
    },
    error::InternalError,
    value::{InputValue, Value},
};

fn literals(count: usize) -> Vec<Option<AcceptedCheckLiteralV1>> {
    let catalog = catalog();
    (0..count)
        .map(|value| {
            Some(
                bind_index_predicate_literal(
                    InputValue::nat64(u64::try_from(value).unwrap()),
                    K::Nat64,
                    catalog.enum_catalog(),
                    catalog.composite_catalog(),
                )
                .unwrap(),
            )
        })
        .collect()
}

fn comparison(op: CompareOp, values: Vec<Option<AcceptedCheckLiteralV1>>) -> P {
    P::Compare {
        field: FieldId::new(2),
        op,
        coercion: CoercionId::Strict,
        values,
    }
}

#[test]
fn projected_scalar_comparisons_require_exactly_one_literal() {
    let base = snapshot(K::Nat64);
    for op in [
        CompareOp::Eq,
        CompareOp::Ne,
        CompareOp::Lt,
        CompareOp::Lte,
        CompareOp::Gt,
        CompareOp::Gte,
    ] {
        for values in [vec![], vec![None; 2], literals(2)] {
            let error = comparison(op, values)
                .to_predicate(base.fields(), &catalog(), None)
                .unwrap_err();
            let expected = InternalError::store_corruption();
            assert_eq!(error.class(), expected.class());
            assert_eq!(error.origin(), expected.origin());
            assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
        }
        for (values, expected) in [(vec![None], Value::Null), (literals(1), Value::Nat64(0))] {
            let Predicate::Compare(projected) = comparison(op, values)
                .to_predicate(base.fields(), &catalog(), None)
                .unwrap()
            else {
                panic!("scalar comparison")
            };
            assert_eq!(projected.op(), op);
            assert_eq!(projected.value(), &expected);
            assert_eq!(projected.field(), "left");
        }
    }
}

#[test]
fn projected_literals_keep_the_existing_corruption_boundary() {
    let base = snapshot(K::Nat64);
    let valid = literals(1).pop().unwrap().unwrap();
    let corrupt = AcceptedCheckLiteralV1::from_accepted_parts(
        valid.kind().clone(),
        valid.storage_decode(),
        valid.leaf_codec(),
        vec![],
    );
    for (op, values) in [
        (CompareOp::Eq, vec![Some(corrupt.clone())]),
        (
            CompareOp::In,
            vec![Some(valid.clone()), Some(corrupt.clone())],
        ),
        (CompareOp::NotIn, vec![Some(corrupt), Some(valid)]),
    ] {
        let error = comparison(op, values)
            .to_predicate(base.fields(), &catalog(), None)
            .unwrap_err();
        let expected = InternalError::store_corruption();
        assert_eq!(error.class(), expected.class());
        assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
    }
}

#[test]
fn projected_membership_backing_matches_the_known_literal_count() {
    let base = snapshot(K::Nat64);
    for op in [CompareOp::In, CompareOp::NotIn] {
        for count in [0, 1, 2, 5, 17] {
            let mut members = literals(count);
            if let Some(last) = members.last_mut() {
                *last = None;
            }
            let Predicate::Compare(projected) = comparison(op, members)
                .to_predicate(base.fields(), &catalog(), None)
                .unwrap()
            else {
                panic!("membership comparison")
            };
            let Value::List(values) = projected.value() else {
                panic!("membership list")
            };
            assert_eq!(values.capacity(), count);
            assert_eq!(values.len(), count);
            for (index, value) in values.iter().enumerate() {
                assert_eq!(
                    *value,
                    if index + 1 == count {
                        Value::Null
                    } else {
                        Value::Nat64(u64::try_from(index).unwrap())
                    }
                );
            }
        }
    }
}

#[test]
fn projected_junction_backing_matches_the_known_child_count() {
    let base = snapshot(K::Nat64);
    for count in [2, 5, 17] {
        let children = literals(count)
            .into_iter()
            .map(|literal| comparison(CompareOp::Ne, vec![literal]))
            .collect::<Vec<_>>();
        for bound in [P::And(children.clone()), P::Or(children)] {
            let projected = bound.to_predicate(base.fields(), &catalog(), None).unwrap();
            let (Predicate::And(children) | Predicate::Or(children)) = projected else {
                panic!("junction")
            };
            assert_eq!(children.capacity(), count);
            assert_eq!(children.len(), count);
            for (index, child) in children.iter().enumerate() {
                let Predicate::Compare(compare) = child else {
                    panic!("comparison child")
                };
                assert_eq!(compare.op(), CompareOp::Ne);
                assert_eq!(
                    compare.value(),
                    &Value::Nat64(u64::try_from(index).unwrap())
                );
            }
        }
    }
}
