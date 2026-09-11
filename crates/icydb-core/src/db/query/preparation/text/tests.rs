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
    value::{Value, ValueEnum},
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

#[test]
fn decimal_label_scratch_uses_current_authority_and_preserves_successful_text() {
    for value in [
        Value::NatBig("18446744073709551616".parse().unwrap()),
        Value::IntBig("-18446744073709551616".parse().unwrap()),
        Value::Enum(ValueEnum::test_payload(
            2,
            3,
            Value::Map(vec![(
                Value::Text("key".into()),
                Value::List(vec![
                    Value::NatBig("18446744073709551616".parse().unwrap()),
                    Value::IntBig("-18446744073709551616".parse().unwrap()),
                ]),
            )]),
        )),
    ] {
        let expr = Expr::Literal(value);
        let expected = render_scalar_projection_expr_plan_label(&expr);
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            let render = |request: &RequestExecutionRoot| {
                PreparationWork::run(&request.scope(), lane, |work| {
                    work.render_text(|out| write_scalar_projection_expr_plan_label(&expr, out))
                })
            };
            let generous = root(16_000_000);
            assert_eq!(render(&generous).unwrap(), expected);
            for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
                let used = generous.observed(resource);
                assert!(used > expected.len() as u64);
                for allowance in [used - 1, used, 2 * used] {
                    let request = RequestExecutionRoot::new_for_tests(
                        HardExecutionBudget::uniform_for_tests(
                            16_000_000,
                            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                        )
                        .with_limit_for_tests(resource, allowance),
                    );
                    if allowance < used {
                        assert!(render(&request).is_err());
                        continue;
                    }
                    for _ in 0..allowance / used {
                        assert_eq!(render(&request).unwrap(), expected);
                    }
                    assert_eq!(request.observed(resource), allowance);
                    let error = render(&request).unwrap_err();
                    assert!(
                        error
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
                    );
                    assert_eq!(request.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }
}

#[test]
fn decimal_scratch_rejection_precedes_output_and_cannot_be_swallowed() {
    let expr = Expr::Literal(Value::NatBig("18446744073709551616".parse().unwrap()));
    let request = root(27);
    let error = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
        work.render_text(|out| {
            let _ = write_scalar_projection_expr_plan_label(&expr, out);
            let _ = out.write_str("must not continue");
            Ok(())
        })
    })
    .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw(),
    )));
    assert_eq!(request.observed(Resource::TemporaryBytes), 28);
    assert_eq!(request.observed(Resource::PredicateExpressionSteps), 0);
}

#[test]
fn nested_decimal_conversion_rejection_is_sticky_and_cumulative() {
    let value = crate::types::NatBig::from_biguint(num_bigint::BigUint::from(1_u32) << 524_288);
    let expr = Expr::Literal(Value::List(vec![Value::NatBig(value)]));
    let request = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::PredicateExpressionSteps, 1024),
    );
    for _ in 0..2 {
        let before = request.observed(Resource::PredicateExpressionSteps);
        let error = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
            work.render_text(|out| {
                let _ = write_scalar_projection_expr_plan_label(&expr, out);
                let _ = out.write_str("must not continue");
                Ok(())
            })
        })
        .unwrap_err();
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::PredicateExpressionSteps.raw(),
        )));
        assert!(request.observed(Resource::PredicateExpressionSteps) > before);
        assert_eq!(request.observed(Resource::RowsVisited), 0);
    }
}
