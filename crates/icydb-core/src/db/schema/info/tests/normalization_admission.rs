//! Schema normalization uses the current request before retaining operands.

use super::{enum_newtype_query_schema, newtype_query_schema};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::{
            CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate,
            normalize_enum_literals,
        },
        query::preparation::PreparationWork,
        schema::SchemaInfo,
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn run(
    schema: &SchemaInfo,
    predicate: &Predicate,
    root: &RequestExecutionRoot,
    lane: Lane,
) -> Result<Predicate, QueryError> {
    PreparationWork::run(&root.scope(), lane, |work| {
        normalize_enum_literals(schema, predicate, work)
    })
}

#[test]
fn schema_normalization_admits_shapes_before_copying_and_shares_request_limits() {
    let schema = newtype_query_schema();
    let leaves = vec![
        Predicate::True,
        Predicate::False,
        Predicate::eq("id".into(), Value::Int64(7)),
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::In,
            Value::List(vec![Value::Int64(7), Value::Nat64(7)]),
            CoercionId::Strict,
        )),
        Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
            "id",
            CompareOp::Eq,
            "id",
            CoercionId::Strict,
        )),
        Predicate::IsNull {
            field: "name".into(),
        },
        Predicate::IsNotNull {
            field: "name".into(),
        },
        Predicate::IsMissing {
            field: "name".into(),
        },
        Predicate::IsEmpty {
            field: "aliases".into(),
        },
        Predicate::IsNotEmpty {
            field: "aliases".into(),
        },
        Predicate::TextContains {
            field: "name".into(),
            value: Value::Text("x".repeat(128)),
        },
        Predicate::TextContainsCi {
            field: "name".into(),
            value: Value::Text("İ".repeat(128)),
        },
        Predicate::eq(
            "unknown".into(),
            Value::Map(vec![(Value::Text("k".into()), Value::Blob(vec![1; 512]))]),
        ),
    ];
    let input = Predicate::And(vec![Predicate::Not(Box::new(Predicate::Or(leaves)))]);
    let mut expected = input.clone();
    let Predicate::And(and) = &mut expected else {
        panic!("AND")
    };
    let Predicate::Not(not) = &mut and[0] else {
        panic!("NOT")
    };
    let Predicate::Or(children) = not.as_mut() else {
        panic!("OR")
    };
    children[2] = Predicate::eq("id".into(), Value::Nat64(7));
    children[3] = Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::In,
        Value::List(vec![Value::Nat64(7)]),
        CoercionId::Strict,
    ));
    children[4] = Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
        "id",
        CompareOp::Eq,
        "id",
        CoercionId::NumericWiden,
    ));

    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for resource in [
            Resource::TemporaryBytes,
            Resource::NestedValueSteps,
            Resource::PredicateExpressionSteps,
        ] {
            let measured = request(resource, 16_000_000);
            assert_eq!(run(&schema, &input, &measured, lane).unwrap(), expected);
            let exact = measured.observed(resource);
            assert!(exact > 0);
            for limit in [exact - 1, exact, 2 * exact] {
                let root = request(resource, limit);
                for _ in 0..limit / exact {
                    assert_eq!(run(&schema, &input, &root, lane).unwrap(), expected);
                }
                let error = run(&schema, &input, &root, lane).unwrap_err();
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                );
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::ExecutionLane, lane.raw()))
                );
                assert_eq!(root.observed(Resource::RowsVisited), 0);
            }
        }
    }
}

#[test]
fn enum_normalization_admits_loose_canonical_and_collection_operands() {
    for collection in [false, true] {
        let schema = enum_newtype_query_schema(collection);
        let op = if collection {
            CompareOp::Contains
        } else {
            CompareOp::Eq
        };
        let loose = Predicate::Compare(ComparePredicate::with_coercion(
            "stage",
            op,
            Value::Text("Active".into()),
            CoercionId::Strict,
        ));
        let root = request(Resource::TemporaryBytes, 16_000_000);
        let canonical = run(&schema, &loose, &root, Lane::Diagnostic).unwrap();
        assert!(
            matches!(&canonical, Predicate::Compare(cmp) if matches!(cmp.value(), Value::Enum(_)))
        );
        for input in [&loose, &canonical] {
            for resource in [
                Resource::TemporaryBytes,
                Resource::NestedValueSteps,
                Resource::PredicateExpressionSteps,
            ] {
                let measured = request(resource, 16_000_000);
                assert_eq!(
                    run(&schema, input, &measured, Lane::Diagnostic).unwrap(),
                    canonical
                );
                let exact = measured.observed(resource);
                let root = request(resource, exact - 1);
                let error = run(&schema, input, &root, Lane::Diagnostic).unwrap_err();
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                );
            }
        }
    }
}

#[test]
fn discarded_boolean_branches_still_validate_and_admit() {
    let schema = enum_newtype_query_schema(false);
    for first in [Predicate::True, Predicate::False] {
        let input = Predicate::Or(vec![
            first,
            Predicate::eq("stage".into(), Value::Text("not_a_variant".into())),
        ]);
        let root = request(Resource::TemporaryBytes, 16_000_000);
        assert!(matches!(
            run(&schema, &input, &root, Lane::PublicRead),
            Err(QueryError::Validate(_))
        ));
        let root = request(Resource::TemporaryBytes, 0);
        let error = run(&schema, &input, &root, Lane::PublicRead).unwrap_err();
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw()
        )));
    }
}
