use super::ConstructionBudget;
use crate::db::{
    QueryError, RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    query::preparation::PreparationWork,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn request(bytes: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, bytes),
    )
}

#[test]
fn vector_reservation_charges_replacement_capacity_before_mutation() {
    let exact = (4 + 8) * size_of::<u64>() as u64;
    for limit in [exact - 1, exact] {
        let root = request(limit);
        PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
            let mut values = Vec::new();
            work.reserve_vec(&mut values, 4)?;
            values.extend_from_slice(&[3_u64, 1, 2, 1]);
            let pointer = values.as_ptr();
            let capacity = values.capacity();
            let result = (work as &dyn ConstructionBudget).reserve_vec(&mut values, 1);
            if limit == exact {
                result.unwrap();
                assert!(values.capacity() >= 5);
                // A reserve that fits existing backing is free.
                work.reserve_vec(&mut values, 1)?;
            } else {
                let error = QueryError::execute(result.unwrap_err());
                assert!(error.diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::TemporaryBytes.raw()
                )));
                assert_eq!(values.capacity(), capacity);
                assert_eq!(values.as_ptr(), pointer);
            }
            assert_eq!(values, [3, 1, 2, 1]);
            Ok(())
        })
        .unwrap();
        assert_eq!(root.observed(Resource::TemporaryBytes), exact);
    }
}

#[test]
fn text_reservation_and_append_share_the_same_growth_charge() {
    for limit in [11, 12] {
        let root = request(limit);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            let budget: &dyn ConstructionBudget = work;
            let mut text = String::new();
            budget
                .reserve_string(&mut text, 4)
                .map_err(QueryError::execute)?;
            budget
                .push_text(&mut text, "éé")
                .map_err(QueryError::execute)?;
            let pointer = text.as_ptr();
            let capacity = text.capacity();
            let result = budget.push_text(&mut text, "!");
            if limit == 12 {
                result.unwrap();
                assert_eq!(text, "éé!");
            } else {
                assert!(result.is_err());
                assert_eq!(text, "éé");
                assert_eq!(text.capacity(), capacity);
                assert_eq!(text.as_ptr(), pointer);
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(root.observed(Resource::TemporaryBytes), 12);
        assert_eq!(root.observed(Resource::PredicateExpressionSteps), 5);
    }
}

#[test]
fn scalar_formatting_uses_existing_execution_authority_and_preserves_scratch_failure() {
    use crate::db::{
        executor::budget::{
            ExecutionConstructionBudget, HardExecutionContext,
            with_query_execution_budget_for_tests,
        },
        query::{
            builder::scalar_projection::write_scalar_projection_expr_plan_label, plan::expr::Expr,
        },
    };
    use crate::value::Value;
    use icydb_diagnostic_code::DiagnosticExecutionBudgetScope as Scope;

    let expr = Expr::Literal(Value::NatBig("18446744073709551616".parse().unwrap()));
    let construction = &ExecutionConstructionBudget as &dyn ConstructionBudget;
    assert!(
        construction
            .render_text(|out| write_scalar_projection_expr_plan_label(&expr, out))
            .is_err()
    );
    for limit in [27, 16_000_000] {
        let result = with_query_execution_budget_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::TemporaryBytes, limit),
            HardExecutionContext::new(Scope::Execution, Lane::PublicRead, 0),
            || {
                construction
                    .render_text(|out| {
                        // Even a formatter swallowing its failure cannot turn rejected
                        // conversion scratch into successful partial output.
                        let _ = write_scalar_projection_expr_plan_label(&expr, out);
                        Ok(())
                    })
                    .map_err(QueryError::execute)
            },
        );
        if limit == 27 {
            assert!(result.unwrap_err().diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::TemporaryBytes.raw(),
            )));
        } else {
            assert_eq!(result.unwrap(), "18_446_744_073_709_551_616");
        }
    }
}
