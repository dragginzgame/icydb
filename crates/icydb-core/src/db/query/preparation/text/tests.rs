use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            builder::scalar_projection::{
                render_scalar_projection_expr_plan_label, write_scalar_projection_expr_plan_label,
            },
            plan::expr::Expr,
            preparation::PreparationWork,
        },
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use std::fmt;

fn root(limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, limit),
    )
}

#[test]
fn shared_sink_preserves_label_escaping_and_charges_output_bytes() {
    let expr = Expr::Literal(Value::Text("quote's λ".into()));
    let expected = render_scalar_projection_expr_plan_label(&expr);
    let request = root(16_000_000);
    let actual = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
        work.render_text(|out| write_scalar_projection_expr_plan_label(&expr, out))
    })
    .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        request.observed(Resource::PredicateExpressionSteps),
        expected.len() as u64
    );
}

#[test]
fn swallowed_sink_errors_are_sticky_and_cannot_return_partial_text() {
    let request = root(0);
    for expected in [4, 8] {
        let error = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
            work.render_text(|out| {
                let _ = out.write_str("head");
                let _ = out.write_str("ignored tail");
                Ok(())
            })
        })
        .unwrap_err();
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw()
        )));
        assert_eq!(request.observed(Resource::TemporaryBytes), expected);
        assert_eq!(
            request.observed(Resource::PredicateExpressionSteps),
            expected
        );
    }
}

#[test]
fn formatter_failure_is_not_a_successful_partial_result() {
    let request = root(16_000_000);
    let error = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
        work.render_text(|out| {
            out.write_str("partial")?;
            Err(fmt::Error)
        })
    })
    .unwrap_err();
    assert_eq!(
        error.diagnostic_code(),
        QueryError::invariant().diagnostic_code()
    );
    assert_eq!(request.observed(Resource::PredicateExpressionSteps), 7);
}
