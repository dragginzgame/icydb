use super::{estimate_value_payload_bytes, primary_key_input_resource_from_value_list};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        query::preparation::PreparationWork,
        resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    },
    value::{Value, ValueEnum},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn request(steps: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::PredicateExpressionSteps, steps)
        .with_limit_for_tests(Resource::TemporaryBytes, 0),
    )
}

#[test]
fn nested_payload_sizing_is_cumulative_and_allocation_free() {
    let value = Value::Enum(ValueEnum::test_payload(
        1,
        1,
        Value::Map(vec![(
            Value::Text("key".into()),
            Value::List(vec![Value::Null, Value::Nat64(7)]),
        )]),
    ));
    // Enum, map, key, list and two elements: lengths do not scan text bytes.
    let steps = 6;
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for limit in [0, steps - 1, steps, 2 * steps] {
            let root = request(limit);
            for attempt in 1..=3 {
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    estimate_value_payload_bytes(&value, work).map_err(QueryError::execute)
                });
                if attempt * steps <= limit {
                    assert_eq!(result.unwrap(), 20);
                } else {
                    let facts = result.unwrap_err().diagnostic_facts();
                    assert!(facts.contains(&(
                        DiagnosticFactTag::BudgetResource,
                        Resource::PredicateExpressionSteps.raw()
                    )));
                    assert!(facts.contains(&(DiagnosticFactTag::ExecutionLane, lane.raw())));
                    break;
                }
            }
            assert_eq!(root.observed(Resource::TemporaryBytes), 0);
            assert_eq!(root.observed(Resource::RowsVisited), 0);
        }
    }
}

#[test]
fn payload_length_metadata_does_not_charge_or_copy_scalar_bytes() {
    let values = vec![
        Value::Text("x".repeat(256 * 1024)),
        Value::Blob(vec![0; 256 * 1024]),
    ];
    let root = request(2);
    PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
        let summary = primary_key_input_resource_from_value_list(&values, work)
            .unwrap()
            .unwrap();
        assert_eq!(summary.raw_term_count(), 2);
        assert_eq!(summary.estimated_payload_bytes(), 512 * 1024);
        assert!(
            primary_key_input_resource_from_value_list(&[], work)
                .unwrap()
                .is_none()
        );
        Ok(())
    })
    .unwrap();
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 2);
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
}
