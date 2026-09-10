use super::{validate_no_duplicate_non_pk_order_fields, validate_primary_key_tie_break};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            plan::{
                OrderDirection, OrderSpec, OrderTerm,
                expr::Expr,
                validate::{OrderPlanError, PlanError},
            },
            preparation::PreparationWork,
        },
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use std::borrow::Cow;

fn root(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn order(names: &[&str]) -> OrderSpec {
    OrderSpec {
        fields: names
            .iter()
            .map(|name| OrderTerm::field(*name, OrderDirection::Asc))
            .collect(),
    }
}

fn assert_order_error(actual: QueryError, expected: OrderPlanError) {
    let expected = QueryError::from(PlanError::from(expected));
    assert_eq!(actual.diagnostic_code(), expected.diagnostic_code());
    assert_eq!(actual.diagnostic_facts(), expected.diagnostic_facts());
}

#[test]
fn primary_key_duplicates_need_no_scratch_or_label_copies() {
    let keys = ["id".into()];
    let order = order(&["id", "id"]);
    let request = root(Resource::TemporaryBytes, 0);
    PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
        validate_no_duplicate_non_pk_order_fields(&keys, &order, work)?;
        validate_primary_key_tie_break(&keys, &order, work)
    })
    .unwrap();
    assert_eq!(request.observed(Resource::TemporaryBytes), 0);
    assert_eq!(request.observed(Resource::PredicateExpressionSteps), 12);
}

#[test]
fn direct_names_borrow_payloads_and_reserve_only_seen_slots() {
    let keys = ["id".into()];
    let order = order(&["rank", "label"]);
    let bytes = 4 * size_of::<(usize, Cow<'_, str>)>() as u64;
    let request = root(Resource::TemporaryBytes, bytes);
    PreparationWork::run(&request.scope(), Lane::PublicRead, |work| {
        validate_no_duplicate_non_pk_order_fields(&keys, &order, work)
    })
    .unwrap();
    assert_eq!(request.observed(Resource::TemporaryBytes), bytes);
    assert_eq!(request.observed(Resource::PredicateExpressionSteps), 5);
    let rejected = root(Resource::TemporaryBytes, bytes - 1);
    let error = PreparationWork::run(&rejected.scope(), Lane::PublicRead, |work| {
        validate_no_duplicate_non_pk_order_fields(&keys, &order, work)
    })
    .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw()
    )));
    assert_eq!(rejected.observed(Resource::PredicateExpressionSteps), 2);
}

#[test]
fn duplicate_positions_and_label_equivalence_are_preserved() {
    let keys = ["id".into()];
    let computed = OrderSpec {
        fields: vec![
            OrderTerm::new(Expr::Literal(Value::Int64(1)), OrderDirection::Asc),
            OrderTerm::new(Expr::Literal(Value::Nat64(1)), OrderDirection::Desc),
        ],
    };
    for (order, first, duplicate) in [
        (order(&["id", "rank", "id", "rank"]), 1, 3),
        (computed, 0, 1),
    ] {
        let request = root(Resource::TemporaryBytes, 16_000_000);
        let error = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
            validate_no_duplicate_non_pk_order_fields(&keys, &order, work)
        })
        .unwrap_err();
        assert_order_error(
            error,
            OrderPlanError::duplicate_order_field(first, duplicate),
        );
    }
}

#[test]
fn missing_composite_key_keeps_its_component_index_and_empty_order_exit() {
    let keys = ["tenant".into(), "id".into()];
    let request = root(Resource::TemporaryBytes, 0);
    let error = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
        validate_primary_key_tie_break(&keys, &order(&["tenant", "ID"]), work)
    })
    .unwrap_err();
    assert_order_error(error, OrderPlanError::missing_primary_key_tie_break(1));
    let empty = root(Resource::PredicateExpressionSteps, 0);
    PreparationWork::run(&empty.scope(), Lane::Diagnostic, |work| {
        validate_primary_key_tie_break(&keys, &order(&[]), work)
    })
    .unwrap();
    assert_eq!(empty.observed(Resource::PredicateExpressionSteps), 0);
}

#[test]
fn exhausted_checks_keep_current_request_charges_in_every_lane() {
    let keys = ["id".into()];
    let order = order(&["id"]);
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        let request = root(Resource::PredicateExpressionSteps, 7);
        let validate = || {
            PreparationWork::run(&request.scope(), lane, |work| {
                validate_no_duplicate_non_pk_order_fields(&keys, &order, work)?;
                validate_primary_key_tie_break(&keys, &order, work)
            })
        };
        for observed in [8, 9] {
            let error = validate().unwrap_err();
            assert!(error.diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::PredicateExpressionSteps.raw()
            )));
            assert!(
                error
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::ExecutionLane, lane.raw()))
            );
            assert_eq!(
                request.observed(Resource::PredicateExpressionSteps),
                observed
            );
        }
    }
}

#[test]
fn computed_label_exhaustion_rejects_before_seen_storage() {
    let keys = ["id".into()];
    let order = OrderSpec {
        fields: vec![OrderTerm::new(
            Expr::Literal(Value::Text("payload'quoted".into())),
            OrderDirection::Asc,
        )],
    };
    let request = root(Resource::TemporaryBytes, 0);
    let before = order.clone();
    let error = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
        validate_no_duplicate_non_pk_order_fields(&keys, &order, work)
    })
    .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw()
    )));
    assert_eq!(request.observed(Resource::TemporaryBytes), 4);
    assert_eq!(request.observed(Resource::PredicateExpressionSteps), 2);
    assert_eq!(order, before);
}
