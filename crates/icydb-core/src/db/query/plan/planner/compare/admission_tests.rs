//! Primary-key operand construction preserves admission and literal semantics.

use super::plan_pk_compare;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        access::AccessPath,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::CompareOp,
        query::preparation::PreparationWork,
        schema::FieldType,
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use icydb_schema::ScalarKind;

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

#[test]
fn primary_key_copies_admit_payload_list_and_path_before_publication() {
    let field_type = FieldType::Scalar(ScalarKind::Nat);
    let cases =
        std::iter::once((CompareOp::Eq, Value::Nat64(7), 0, 1)).chain([0, 1, 2, 128].map(|len| {
            let values = (0..len)
                .map(|index| Value::Nat64(7 - index as u64 % 3))
                .collect();
            (
                CompareOp::In,
                Value::List(values),
                len * size_of::<Value>(),
                len,
            )
        }));
    for (op, value, payload_bytes, visits) in cases {
        let bytes = (payload_bytes + size_of::<AccessPath<Value>>()) as u64;
        let before = value.clone();
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for (resource, exact) in [
                (Resource::TemporaryBytes, bytes),
                (Resource::NestedValueSteps, visits as u64),
            ] {
                for limit in [0, exact.saturating_sub(1), exact * 2] {
                    let root = request(resource, limit);
                    PreparationWork::run(&root.scope(), lane, |work| {
                        for attempt in 0..3 {
                            let result = plan_pk_compare(&field_type, &value, op, work);
                            if exact == 0 || limit >= exact && attempt < 2 {
                                let plan = result.unwrap().unwrap();
                                match (plan.as_path().unwrap(), &value) {
                                    (AccessPath::ByKey(key), value) => assert_eq!(key, value),
                                    (AccessPath::ByKeys(keys), Value::List(values)) => {
                                        assert_eq!(keys, values);
                                    }
                                    _ => panic!(
                                        "primary-key route must preserve its source operands"
                                    ),
                                }
                            } else {
                                let error = QueryError::execute(result.unwrap_err());
                                assert!(error.diagnostic_facts().contains(&(
                                    DiagnosticFactTag::BudgetResource,
                                    resource.raw(),
                                )));
                                break;
                            }
                        }
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(value, before);
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                    if resource == Resource::TemporaryBytes && limit == 0 {
                        // A denied IN destination never starts copying children.
                        let expected = u64::from(op == CompareOp::Eq);
                        assert_eq!(root.observed(Resource::NestedValueSteps), expected);
                    }
                }
            }
        }
    }
}

#[test]
fn unsupported_primary_key_literals_remain_absent_before_copying() {
    let key = FieldType::Scalar(ScalarKind::Nat);
    let cases = [
        (key.clone(), CompareOp::Eq, Value::Text("invalid".into())),
        (key.clone(), CompareOp::In, Value::Text("not a list".into())),
        (
            key.clone(),
            CompareOp::In,
            Value::List(vec![Value::Nat64(1), Value::Text("invalid".into())]),
        ),
        (key, CompareOp::Gt, Value::Nat64(1)),
        (
            FieldType::Composite,
            CompareOp::Eq,
            Value::Text("valid".into()),
        ),
    ];
    for (field_type, op, value) in cases {
        let root = request(Resource::TemporaryBytes, 0);
        PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
            assert!(
                plan_pk_compare(&field_type, &value, op, work)
                    .unwrap()
                    .is_none()
            );
            Ok(())
        })
        .unwrap();
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
        assert_eq!(root.observed(Resource::NestedValueSteps), 0);
    }
}
