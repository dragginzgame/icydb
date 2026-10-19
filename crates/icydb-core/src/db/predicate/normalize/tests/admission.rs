use crate::{
    db::{
        predicate::normalize::admission::admit_enum_input_construction,
        query::preparation::PreparationWork, test_support::request_with_limit,
    },
    value::{Value, ValueEnum},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

#[test]
fn enum_input_preflight_covers_nested_payloads_and_preserves_canonical_detection() {
    for (value, expected_enum) in [
        (
            Value::List(vec![Value::Map(vec![(
                Value::Text("k".repeat(128)),
                Value::Blob(vec![1; 4096]),
            )])]),
            false,
        ),
        (
            Value::List(vec![Value::Enum(ValueEnum::test_payload(
                1,
                1,
                Value::Blob(vec![1; 4096]),
            ))]),
            true,
        ),
        (
            Value::Map(vec![(
                Value::Enum(ValueEnum::test_unit(1, 1)),
                Value::Text("v".into()),
            )]),
            true,
        ),
    ] {
        for resource in [
            Resource::TemporaryBytes,
            Resource::NestedValueSteps,
            Resource::PredicateExpressionSteps,
        ] {
            let root = request_with_limit(resource, 16_000_000);
            let admitted = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                admit_enum_input_construction(&value, work)
            })
            .unwrap();
            assert_eq!(admitted, expected_enum);
            let exact = root.observed(resource);
            if exact == 0 {
                continue;
            }
            let root = request_with_limit(resource, exact - 1);
            let error = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                admit_enum_input_construction(&value, work)
            })
            .unwrap_err();
            assert!(
                error
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
            );
        }
    }
}
