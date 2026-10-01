//! Exact signed lookup semantics and proof-admission boundaries.

use crate::{
    db::{
        predicate::{
            CoercionId, CoercionSpec, CompareOp, compare_eq,
            normalize::normalize_signed_lookup_coercion,
        },
        query::preparation::PreparationWork,
        schema::AcceptedFieldKind,
        test_support::request_with_limit,
    },
    types::EntityTag,
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

#[test]
fn signed_lookup_equality_agrees_with_widening_at_extrema_and_null() {
    let strict = CoercionSpec::new(CoercionId::Strict);
    let widened = CoercionSpec::new(CoercionId::NumericWiden);
    let atoms = [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX];
    for left in atoms.into_iter().map(Value::Int64).chain([Value::Null]) {
        for right in atoms.into_iter().map(Value::Int64) {
            assert_eq!(
                compare_eq(&left, &right, &strict),
                compare_eq(&left, &right, &widened),
                "left={left:?} right={right:?}",
            );
        }
    }
}

#[test]
fn signed_lookup_admission_requires_exact_positive_scalar_operands() {
    let relation = AcceptedFieldKind::Relation {
        target_path: "test::Player".into(),
        target_entity_name: "Player".into(),
        target_entity_tag: EntityTag::new(1),
        target_store_path: "test::Store".into(),
        key_kind: Box::new(AcceptedFieldKind::Int64),
    };
    PreparationWork::run(
        &request_with_limit(Resource::TemporaryBytes, 0).scope(),
        Lane::PublicRead,
        |work| {
            for kind in [
                AcceptedFieldKind::Int8,
                AcceptedFieldKind::Int16,
                AcceptedFieldKind::Int32,
                AcceptedFieldKind::Int64,
                relation,
            ] {
                for (op, value) in [
                    (CompareOp::Eq, Value::Int64(-1)),
                    (
                        CompareOp::In,
                        Value::List(vec![Value::Int64(-1), Value::Int64(1)]),
                    ),
                ] {
                    assert_eq!(
                        normalize_signed_lookup_coercion(
                            &kind,
                            op,
                            &value,
                            CoercionId::NumericWiden,
                            work
                        )?,
                        CoercionId::Strict,
                    );
                }
                for (op, value) in [
                    (CompareOp::Eq, Value::Null),
                    (CompareOp::Eq, Value::Nat64(1)),
                    (CompareOp::Eq, Value::Int128(1)),
                    (
                        CompareOp::In,
                        Value::List(vec![Value::Int64(1), Value::Null]),
                    ),
                    (
                        CompareOp::In,
                        Value::List(vec![Value::Int64(1), Value::Text("x".into())]),
                    ),
                    (CompareOp::Ne, Value::Int64(1)),
                    (CompareOp::NotIn, Value::List(vec![Value::Int64(1)])),
                    (CompareOp::Gt, Value::Int64(1)),
                    (CompareOp::Gte, Value::Int64(1)),
                    (CompareOp::Lt, Value::Int64(1)),
                    (CompareOp::Lte, Value::Int64(1)),
                ] {
                    assert_eq!(
                        normalize_signed_lookup_coercion(
                            &kind,
                            op,
                            &value,
                            CoercionId::NumericWiden,
                            work
                        )?,
                        CoercionId::NumericWiden,
                    );
                }
            }
            for kind in [
                AcceptedFieldKind::Int128,
                AcceptedFieldKind::IntBig { max_bytes: 128 },
                AcceptedFieldKind::Nat64,
                AcceptedFieldKind::Float64,
                AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Int64)),
            ] {
                assert_eq!(
                    normalize_signed_lookup_coercion(
                        &kind,
                        CompareOp::Eq,
                        &Value::Int64(1),
                        CoercionId::NumericWiden,
                        work
                    )?,
                    CoercionId::NumericWiden,
                );
            }
            Ok(())
        },
    )
    .unwrap();
}

#[test]
fn signed_membership_proof_charges_every_inspected_member_before_admission() {
    let value = Value::List(vec![Value::Int64(-1), Value::Int64(0), Value::Int64(1)]);
    for limit in [0, 2, 3] {
        let root = request_with_limit(Resource::NestedValueSteps, limit);
        let result = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
            normalize_signed_lookup_coercion(
                &AcceptedFieldKind::Int64,
                CompareOp::In,
                &value,
                CoercionId::NumericWiden,
                work,
            )
        });
        if limit == 3 {
            assert_eq!(result.unwrap(), CoercionId::Strict);
            assert_eq!(root.observed(Resource::NestedValueSteps), 3);
        } else {
            assert!(result.unwrap_err().diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::NestedValueSteps.raw(),
            )));
        }
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    }
}
