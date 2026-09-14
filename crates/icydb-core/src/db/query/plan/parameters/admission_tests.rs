//! Template eligibility limits stop construction; exhaustion stays a typed error.

use super::{
    MAX_PREPARED_QUERY_LIST_PARAMETER_ITEMS, MAX_PREPARED_QUERY_PARAMETER_BYTES,
    MAX_PREPARED_QUERY_PARAMETER_SLOTS, PreparedQueryParameterContract,
};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::Predicate,
        query::preparation::PreparationWork,
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

fn contract(
    predicate: &Predicate,
    root: &RequestExecutionRoot,
    lane: Lane,
) -> Result<Option<PreparedQueryParameterContract>, QueryError> {
    PreparationWork::run(&root.scope(), lane, |work| {
        PreparedQueryParameterContract::from_normalized_predicate(predicate, work)
            .map_err(QueryError::execute)
    })
}

#[test]
fn parameter_slot_cap_stops_nested_construction_before_later_metadata() {
    let accepted = Predicate::And(vec![Predicate::Or(
        (0..MAX_PREPARED_QUERY_PARAMETER_SLOTS)
            .map(|index| Predicate::eq(format!("field_{index}"), Value::Nat64(index as u64)))
            .collect(),
    )]);
    let root = request(Resource::TemporaryBytes, 16_000_000);
    assert!(
        contract(&accepted, &root, Lane::PublicRead)
            .unwrap()
            .is_some()
    );
    let bytes = root.observed(Resource::TemporaryBytes);
    let steps = root.observed(Resource::PredicateExpressionSteps);

    for count in [1, 128] {
        let Predicate::And(mut children) = accepted.clone() else {
            unreachable!()
        };
        children.extend(
            (0..count).map(|_| Predicate::eq("oversized_metadata".repeat(1024), Value::Nat64(1))),
        );
        let oversized = Predicate::And(children);
        let root = request(Resource::TemporaryBytes, bytes);
        assert!(
            contract(&oversized, &root, Lane::PublicRead)
                .unwrap()
                .is_none()
        );
        assert_eq!(root.observed(Resource::TemporaryBytes), bytes);
        assert_eq!(root.observed(Resource::PredicateExpressionSteps), steps + 1);
    }
}

#[test]
fn parameter_payload_and_list_caps_preserve_eligibility_without_operand_copies() {
    for (predicate, eligible) in [
        (
            Predicate::eq(
                "x".into(),
                Value::Text("a".repeat(MAX_PREPARED_QUERY_PARAMETER_BYTES as usize)),
            ),
            true,
        ),
        (
            Predicate::eq(
                "x".into(),
                Value::Text("a".repeat(MAX_PREPARED_QUERY_PARAMETER_BYTES as usize + 1)),
            ),
            false,
        ),
        (
            Predicate::in_(
                "x".into(),
                (0..MAX_PREPARED_QUERY_LIST_PARAMETER_ITEMS)
                    .map(|n| Value::Nat64(n as u64))
                    .collect(),
            ),
            true,
        ),
        (
            Predicate::in_(
                "x".into(),
                (0..=MAX_PREPARED_QUERY_LIST_PARAMETER_ITEMS)
                    .map(|n| Value::Nat64(n as u64))
                    .collect(),
            ),
            false,
        ),
        (
            Predicate::in_("x".into(), vec![Value::Nat64(1), Value::Text("a".into())]),
            false,
        ),
        (Predicate::True, false),
    ] {
        // Only the field name is retained: neither large scalar nor IN payloads
        // become part of a value-independent template contract.
        let root = request(Resource::TemporaryBytes, u64::from(eligible));
        assert_eq!(
            contract(&predicate, &root, Lane::Diagnostic)
                .unwrap()
                .is_some(),
            eligible
        );
        assert_eq!(root.observed(Resource::TemporaryBytes), u64::from(eligible));
    }

    // The payload ceiling is shared across nested slots, not reset per child.
    let half = Value::Text("a".repeat(MAX_PREPARED_QUERY_PARAMETER_BYTES as usize / 2));
    let predicate = Predicate::Or(vec![
        Predicate::eq("x".into(), half.clone()),
        Predicate::And(vec![Predicate::eq("y".into(), half)]),
    ]);
    let root = request(Resource::TemporaryBytes, 16_000_000);
    assert!(
        contract(&predicate, &root, Lane::Diagnostic)
            .unwrap()
            .is_some()
    );
    let mut oversized = predicate;
    let Predicate::Or(children) = &mut oversized else {
        unreachable!()
    };
    children.push(Predicate::eq("z".into(), Value::Bool(true)));
    let root = request(
        Resource::TemporaryBytes,
        root.observed(Resource::TemporaryBytes),
    );
    assert!(
        contract(&oversized, &root, Lane::Diagnostic)
            .unwrap()
            .is_none()
    );
}

#[test]
fn parameter_coercion_metadata_is_admitted_before_copying() {
    let mut predicate = Predicate::eq("field".into(), Value::Nat64(1));
    let Predicate::Compare(compare) = &mut predicate else {
        unreachable!()
    };
    compare.coercion.params = vec![("name".into(), "value".into())];
    let bytes =
        ("field".len() + size_of::<(String, String)>() + "name".len() + "value".len()) as u64;
    let admitted = request(Resource::TemporaryBytes, bytes);
    assert!(
        contract(&predicate, &admitted, Lane::PublicRead)
            .unwrap()
            .is_some()
    );
    assert_eq!(admitted.observed(Resource::TemporaryBytes), bytes);
    let rejected = request(Resource::TemporaryBytes, bytes - 1);
    let error = contract(&predicate, &rejected, Lane::PublicRead).unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw(),
    )));
}

#[test]
fn parameter_construction_exhaustion_is_cumulative_in_every_read_lane() {
    let mut equality = Predicate::eq("account".into(), Value::Nat64(1));
    let Predicate::Compare(compare) = &mut equality else {
        unreachable!()
    };
    compare.coercion.params = vec![("name".into(), "value".repeat(32))];
    let predicate = Predicate::And(vec![
        equality,
        Predicate::in_("block".into(), vec![Value::Nat64(2), Value::Nat64(3)]),
    ]);
    let before = predicate.clone();
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        let measured = request(Resource::TemporaryBytes, 16_000_000);
        let expected = contract(&predicate, &measured, lane).unwrap().unwrap();
        for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
            let exact = measured.observed(resource);
            for limit in [0, exact - 1, exact * 2] {
                let root = request(resource, limit);
                for attempt in 1..=3 {
                    let result = contract(&predicate, &root, lane);
                    if attempt * exact <= limit {
                        assert_eq!(result.unwrap(), Some(expected.clone()));
                        assert_eq!(root.observed(resource), attempt * exact);
                    } else {
                        let facts = result.unwrap_err().diagnostic_facts();
                        assert!(
                            facts.contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                        );
                        assert!(facts.contains(&(DiagnosticFactTag::ExecutionLane, lane.raw())));
                        break;
                    }
                }
                assert_eq!(root.observed(Resource::RowsVisited), 0);
                assert_eq!(root.observed(Resource::QueryExecutions), 0);
            }
        }
    }
    assert_eq!(predicate, before);
}
