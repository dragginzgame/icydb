use super::*;
use crate::{
    db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            builder::scalar_projection::render_scalar_projection_expr_plan_label,
            plan::expr::{BinaryOp, FieldId},
            preparation::with_preparation_work,
        },
    },
    value::Value,
};
use icydb_diagnostic_code::{DiagnosticExecutionLane, DiagnosticFactTag};

fn comparison(value: Value) -> Expr {
    Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("amount"))),
        right: Box::new(Expr::Literal(value)),
    }
}

fn budget_root(resource: Resource, limit: u64) -> RequestExecutionRoot {
    let budget = HardExecutionBudget::uniform_for_tests(
        16_000_000,
        HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
    )
    .with_limit_for_tests(resource, limit);
    RequestExecutionRoot::new_for_tests(budget)
}

#[test]
fn fallible_ordering_preserves_label_and_typed_debug_tie_breaks() {
    with_preparation_work(|work| {
        // Cover empty/singleton, uneven merge tails, repeated keys, and equal
        // rendered labels whose typed Debug keys differ.
        for len in 0_u32..=35 {
            let mut actual = (0..len)
                .map(|index| {
                    let value = index * 17 % 11;
                    comparison(if index % 2 == 0 {
                        Value::Nat64(u64::from(value))
                    } else {
                        Value::Int64(i64::from(value))
                    })
                })
                .collect::<Vec<_>>();
            let mut expected = actual.clone();
            expected.sort_by(|left, right| {
                render_scalar_projection_expr_plan_label(left)
                    .cmp(&render_scalar_projection_expr_plan_label(right))
                    .then_with(|| format!("{left:?}").cmp(&format!("{right:?}")))
            });
            sort_bool_children(&mut actual, work).expect("finite canonical sort");
            assert_eq!(actual, expected);
        }
    });
}

#[test]
fn failed_ordering_preserves_operands_and_retains_request_charges() {
    let original = (0..8)
        .rev()
        .map(|n| comparison(Value::Nat64(n)))
        .collect::<Vec<_>>();
    for (resource, limit) in [
        (Resource::SortComparisons, 2),
        (Resource::PredicateExpressionSteps, 12),
        // A low construction allowance must reject without moving operands.
        (Resource::TemporaryBytes, (16 * size_of::<usize>()) as u64),
    ] {
        let root = budget_root(resource, limit);
        let mut observed = 0;
        for _ in 0..2 {
            let mut actual = original.clone();
            let error =
                PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                    sort_bool_children(&mut actual, work)
                })
                .expect_err("request exhaustion must abort sorting");
            assert!(
                error
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
            );
            assert_eq!(actual, original);
            assert!(root.observed(resource) > observed);
            observed = root.observed(resource);
        }
    }
}

#[test]
fn debug_tie_break_failure_never_substitutes_an_equal_key() {
    let left = comparison(Value::Int64(7));
    let right = comparison(Value::Nat64(7));
    let label = render_scalar_projection_expr_plan_label(&left);
    assert_eq!(label, render_scalar_projection_expr_plan_label(&right));
    // Two label writes plus their byte comparison fit exactly. Any Debug
    // output must then fail before it can become a partial tie-break key.
    let root = budget_root(Resource::PredicateExpressionSteps, 3 * label.len() as u64);
    let error = PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        let mut keys = [
            OrderingKey::new(&left, work)?,
            OrderingKey::new(&right, work)?,
        ];
        compare(&mut keys, 0, 1, work)
    })
    .expect_err("typed tie-break construction consumes the same request");
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::PredicateExpressionSteps.raw()
    )));
}

#[test]
fn repeated_key_comparison_charges_work_without_rebuilding_text() {
    let left = comparison(Value::Int64(7));
    let right = comparison(Value::Nat64(7));
    let root = budget_root(Resource::TemporaryBytes, 16_000_000);
    PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        let mut keys = [
            OrderingKey::new(&left, work)?,
            OrderingKey::new(&right, work)?,
        ];
        let first = compare(&mut keys, 0, 1, work)?;
        let bytes = root.observed(Resource::TemporaryBytes);
        let steps = root.observed(Resource::PredicateExpressionSteps);
        let comparisons = root.observed(Resource::SortComparisons);
        assert_ne!(
            first,
            Ordering::Equal,
            "typed Debug still resolves the label tie"
        );
        assert_eq!(compare(&mut keys, 0, 1, work)?, first);
        assert_eq!(root.observed(Resource::TemporaryBytes), bytes);
        assert!(root.observed(Resource::PredicateExpressionSteps) > steps);
        assert_eq!(root.observed(Resource::SortComparisons), comparisons + 1);
        Ok(())
    })
    .expect("repeated key comparison is bounded");
}

#[test]
fn rejected_normalization_disposes_of_owned_deep_intermediates() {
    std::thread::Builder::new()
        .stack_size(128 * 1024)
        .spawn(|| {
            let mut expr = Expr::Literal(Value::Bool(true));
            for _ in 0..20_000 {
                expr = Expr::Binary {
                    op: BinaryOp::And,
                    left: Box::new(expr),
                    right: Box::new(Expr::Literal(Value::Bool(false))),
                };
            }
            let root = budget_root(Resource::PredicateExpressionSteps, 16);
            let error =
                PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                    crate::db::query::plan::expr::normalize_bool_expr(expr, work)
                })
                .expect_err("reject before traversing the deep remainder");
            assert!(error.diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::PredicateExpressionSteps.raw()
            )));
        })
        .expect("small-stack probe thread")
        .join()
        .expect("fallible preparation cleanup");
}
