//! Exhaustion stays an error across shared proof and in-place pruning owners.

use super::{
    AccessBoundBranchIn, ComparisonRef, access_bound_text_prefix_range_implies_required,
    branch_in_clause_implies_required, compare_values, eligible_sorted_index_contracts,
    list_contains_all_values, predicate_implies_predicate_for_planner,
    strip_query_clauses_satisfied_by_filtered_guard,
};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::{
            construction::ConstructionBudget,
            plan::{AccessPlannedQuery, LogicalPlan, VisibleIndexes, exact_metadata_schema},
            preparation::PreparationWork,
        },
    },
    error::InternalError,
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

fn run<T>(
    root: &RequestExecutionRoot,
    proof: impl FnOnce(&dyn ConstructionBudget) -> Result<T, InternalError>,
) -> Result<T, QueryError> {
    PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
        proof(work).map_err(QueryError::execute)
    })
}

fn assert_resource(error: &QueryError, resource: Resource) {
    assert!(
        error
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
    );
}

fn assert_step_bound<T: PartialEq + std::fmt::Debug>(
    proof: impl Fn(&dyn ConstructionBudget) -> Result<T, InternalError>,
) {
    let generous = request(Resource::PredicateExpressionSteps, 16_000_000);
    let expected = run(&generous, &proof).unwrap();
    let used = generous.observed(Resource::PredicateExpressionSteps);
    assert!(used > 0);
    // Failure at any point must not turn into false, true or an absent residual.
    for limit in 0..used {
        let root = request(Resource::PredicateExpressionSteps, limit);
        assert_resource(
            &run(&root, &proof).unwrap_err(),
            Resource::PredicateExpressionSteps,
        );
    }
    let exact = request(Resource::PredicateExpressionSteps, used * 2);
    assert_eq!(run(&exact, &proof).unwrap(), expected);
    assert_eq!(run(&exact, &proof).unwrap(), expected);
    assert_resource(
        &run(&exact, &proof).unwrap_err(),
        Resource::PredicateExpressionSteps,
    );
    assert_eq!(exact.observed(Resource::RowsVisited), 0);
}

#[test]
fn implication_and_guard_pruning_preserve_every_exhaustion_boundary() {
    let guard = Predicate::is_not_null("id".into());
    let query = Predicate::Or(vec![
        Predicate::eq("id".into(), Value::Nat64(7)),
        Predicate::And(vec![
            Predicate::True,
            Predicate::eq("id".into(), Value::Int64(8)),
        ]),
    ]);
    for required in [&guard, &Predicate::False, &Predicate::True] {
        assert_step_bound(|budget| {
            predicate_implies_predicate_for_planner(&query, required, budget)
        });
    }
    let clauses = Predicate::And(vec![
        guard.clone(),
        Predicate::And(vec![Predicate::True, guard.clone()]),
        Predicate::eq("other".into(), Value::Nat64(9)),
    ]);
    assert_step_bound(|budget| {
        strip_query_clauses_satisfied_by_filtered_guard(clauses.clone(), &guard, budget)
    });
}

#[test]
fn branch_membership_preserves_exhaustion_and_structural_shortcuts() {
    let values = vec![Value::Int64(7), Value::Int64(9)];
    let branch = AccessBoundBranchIn {
        field: "id",
        values: &values,
    };
    for (op, value) in [
        (CompareOp::Eq, Value::Int64(7)),
        (CompareOp::Ne, Value::Int64(11)),
        (CompareOp::In, Value::List(values.clone())),
        (
            CompareOp::In,
            Value::List(vec![Value::Nat64(9), Value::Nat64(7)]),
        ),
        (CompareOp::NotIn, Value::List(vec![Value::Nat64(9)])),
    ] {
        let cmp = ComparePredicate::with_coercion("id", op, value, CoercionId::Strict);
        assert_step_bound(|budget| branch_in_clause_implies_required(Some(&branch), &cmp, budget));
    }
}

#[test]
fn proof_errors_reach_candidate_eligibility_and_explicit_residual_preparation() {
    let schema = exact_metadata_schema(&[("by_age", &["age"])], &["age"]);
    let visible = VisibleIndexes::accepted_schema_visible(&schema).unwrap();
    let indexes = visible.accepted_semantic_index_contracts();
    let predicate = Predicate::eq("age".into(), Value::Int64(7));
    for query in [&predicate, &Predicate::True] {
        assert_step_bound(|budget| {
            eligible_sorted_index_contracts(indexes, &schema, query, budget)
                .map(|indexes| indexes.len())
        });
    }
    let mut plan =
        AccessPlannedQuery::full_scan_for_test(crate::db::predicate::MissingRowPolicy::Error);
    plan.access = crate::db::access::AccessPlan::index_prefix_from_contract(
        indexes[0].clone(),
        vec![Value::Int64(7)],
    );
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        unreachable!();
    };
    scalar.predicate = Some(predicate);
    let before = plan.clone();
    assert_step_bound(|budget| plan.prepare_residual_filter_shape(budget));
    assert_eq!(plan, before);
    assert!(!plan.has_static_execution_planning_contract());
}

#[test]
fn prefix_successor_admission_is_cumulative_and_follows_lower_proof() {
    let lower = Value::Text("λ".into());
    let upper = Value::Text("μ".into());
    let cmp = ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        lower.clone(),
        CoercionId::Strict,
    );
    let ranges = [
        Some(ComparisonRef::strict("name", CompareOp::Gte, &lower)),
        Some(ComparisonRef::strict("name", CompareOp::Lt, &upper)),
    ];
    let proof = |budget: &dyn ConstructionBudget| {
        access_bound_text_prefix_range_implies_required(&ranges, &cmp, budget)
    };
    assert_step_bound(proof);
    let generous = request(Resource::TemporaryBytes, 16_000_000);
    assert!(run(&generous, proof).unwrap());
    let used = generous.observed(Resource::TemporaryBytes);
    assert_eq!(used, "λ".len() as u64 + 1);
    let exact = request(Resource::TemporaryBytes, used * 2);
    assert!(run(&exact, proof).unwrap());
    assert!(run(&exact, proof).unwrap());
    assert_resource(&run(&exact, proof).unwrap_err(), Resource::TemporaryBytes);
    let none = request(Resource::TemporaryBytes, 0);
    assert_resource(&run(&none, proof).unwrap_err(), Resource::TemporaryBytes);
    let unrelated = [Some(ComparisonRef::strict("other", CompareOp::Gte, &lower))];
    let none = request(Resource::TemporaryBytes, 0);
    assert!(
        !run(&none, |budget| {
            access_bound_text_prefix_range_implies_required(&unrelated, &cmp, budget)
        })
        .unwrap()
    );
    assert_eq!(none.observed(Resource::TemporaryBytes), 0);
}

#[test]
fn payload_comparison_keeps_numeric_strict_semantics_and_admits_repeated_work() {
    use crate::{
        types::{Decimal, Float64, IntBig, NatBig},
        value::ValueEnum,
    };
    let values = [
        Value::Text("λ".repeat(16)),
        Value::IntBig(i128::MIN.to_string().parse::<IntBig>().unwrap()),
        Value::NatBig(u128::MAX.to_string().parse::<NatBig>().unwrap()),
        Value::Enum(ValueEnum::test_payload(1, 2, Value::Text("nested".into()))),
        Value::Map(vec![(
            Value::List(vec![Value::Text("key".into())]),
            Value::Text("value".into()),
        )]),
        Value::Blob(vec![1, 2]),
        Value::List(vec![Value::Null]),
        Value::Int64(7),
        Value::Nat64(7),
        Value::Decimal(Decimal::new(70, 1)),
        Value::Float64(Float64::try_new(7.0).unwrap()),
        Value::Null,
    ];
    for left in &values {
        for right in &values {
            let baseline = request(Resource::PredicateExpressionSteps, 16_000_000);
            let expected = crate::db::numeric::compare_numeric_or_strict_order(left, right);
            assert_eq!(
                run(&baseline, |budget| compare_values(left, right, budget)).unwrap(),
                expected
            );
            for resource in [
                Resource::PredicateExpressionSteps,
                Resource::NestedValueSteps,
            ] {
                let used = baseline.observed(resource);
                if used == 0 {
                    continue;
                }
                let exact = request(resource, used * 2);
                for _ in 0..2 {
                    assert_eq!(
                        run(&exact, |budget| compare_values(left, right, budget)).unwrap(),
                        expected
                    );
                }
                assert_resource(
                    &run(&exact, |budget| compare_values(left, right, budget)).unwrap_err(),
                    resource,
                );
            }
        }
    }
    let mismatch = request(Resource::PredicateExpressionSteps, 1);
    assert_eq!(
        run(&mismatch, |budget| compare_values(
            &values[0],
            &Value::Null,
            budget
        ))
        .unwrap(),
        None
    );
    assert_eq!(mismatch.observed(Resource::NestedValueSteps), 0);
}

#[test]
fn structural_membership_shortcut_admits_nested_payloads_before_equality() {
    let value = Value::List(vec![Value::Blob(vec![1, 2, 3]), Value::Null]);
    let list = Value::List(vec![value.clone()]);
    let required = [value];
    let baseline = request(Resource::PredicateExpressionSteps, 16_000_000);
    assert!(
        run(&baseline, |budget| list_contains_all_values(
            &list, &required, budget
        ))
        .unwrap()
    );
    for resource in [
        Resource::PredicateExpressionSteps,
        Resource::NestedValueSteps,
    ] {
        let used = baseline.observed(resource);
        assert!(used > 0);
        let short = request(resource, used - 1);
        assert_resource(
            &run(&short, |budget| {
                list_contains_all_values(&list, &required, budget)
            })
            .unwrap_err(),
            resource,
        );
    }
    assert_eq!(baseline.observed(Resource::TemporaryBytes), 0);
}
