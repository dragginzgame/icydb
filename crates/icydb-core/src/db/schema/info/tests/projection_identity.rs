//! Projection identity uses accepted slot order, including gaps and reordering.

use super::newtype_query_schema;
use crate::db::{
    RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    query::{
        plan::expr::{Alias, Expr, ProjectionField, ProjectionSpec},
        preparation::PreparationWork,
    },
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
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

fn fields(names: &[&str]) -> Vec<ProjectionField> {
    names
        .iter()
        .map(|name| ProjectionField::Scalar {
            expr: Expr::Field((*name).into()),
            alias: None,
        })
        .collect()
}

#[test]
fn identity_preserves_physical_order_and_rejects_non_identity_shapes() {
    for sparse in [false, true] {
        let mut schema = newtype_query_schema();
        if sparse {
            for (_, field) in &mut schema.fields {
                field.slot = 7 + (3 - field.slot) * 4;
            }
        }
        let canonical = fields(&schema.field_names_in_slot_order());
        let mut reversed = canonical.clone();
        reversed.reverse();
        let mut duplicate = canonical.clone();
        duplicate[1] = duplicate[0].clone();
        let mut aliased = canonical.clone();
        let ProjectionField::Scalar { alias, .. } = &mut aliased[0];
        *alias = Some(Alias::new("alias"));
        let mut unknown = canonical.clone();
        unknown[0] = fields(&["absent"]).pop().unwrap();
        let mut computed = canonical.clone();
        computed[0] = ProjectionField::Scalar {
            expr: Expr::Literal(crate::value::Value::Null),
            alias: None,
        };
        let mut subset = canonical.clone();
        subset.pop();
        for (fields, expected) in [
            (canonical, true),
            (reversed, false),
            (duplicate, false),
            (aliased, false),
            (unknown, false),
            (computed, false),
            (subset, false),
        ] {
            let root = request(16_000_000);
            let projection = ProjectionSpec::from_fields_for_test(fields);
            let actual =
                PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                    projection.is_schema_identity_for(&schema, work)
                })
                .unwrap();
            assert_eq!(actual, expected);
            assert_eq!(root.observed(Resource::TemporaryBytes), 0);
        }
    }
}

#[test]
fn identity_exact_budget_and_early_mismatch_keep_exhaustion_distinct() {
    let schema = newtype_query_schema();
    let names = schema.field_names_in_slot_order();
    let projection = ProjectionSpec::from_fields_for_test(fields(&names));
    let exact = 1
        + names.len() as u64
        + names.iter().map(|name| name.len() as u64).sum::<u64>()
        + (names.len() - 1) as u64;
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
    ] {
        for limit in [exact - 1, exact] {
            let root = request(limit);
            let result = PreparationWork::run(&root.scope(), lane, |work| {
                projection.is_schema_identity_for(&schema, work)
            });
            if limit == exact {
                assert!(result.unwrap());
            } else {
                assert!(result.unwrap_err().diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::PredicateExpressionSteps.raw()
                )));
            }
            assert_eq!(root.observed(Resource::PredicateExpressionSteps), exact);
            assert_eq!(root.observed(Resource::TemporaryBytes), 0);
            assert_eq!(root.observed(Resource::RowsVisited), 0);
        }
    }
    let empty = ProjectionSpec::from_fields_for_test(Vec::new());
    let root = request(1);
    assert!(
        !PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
            empty.is_schema_identity_for(&schema, work)
        })
        .unwrap()
    );
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 1);
}
