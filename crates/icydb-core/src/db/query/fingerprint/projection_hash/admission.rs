//! Query-only admission before the shared projection/filter identity encoder.
//! Does not change encoding semantics or persisted-intent admission policy.

use crate::{
    db::query::{
        construction::ConstructionBudget,
        plan::{
            AggregateSemanticKeyRef,
            expr::{Expr, ProjectionSpec},
        },
    },
    error::InternalError,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

// Covers the largest fixed node framing (literal tag + 16-byte digest),
// aggregate tags/counts and both admission/encoding visits. Variable labels
// and value encoding are charged separately. This is a conservative bound,
// not an instruction measurement; callers supply depth-admitted planner trees.
const EXPR_HASH_NODE_STEPS: u64 = 32;

pub(in crate::db::query::fingerprint) fn admit_projection_hash(
    projection: &ProjectionSpec,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    budget.charge(Resource::PredicateExpressionSteps, 5)?; // tag + field count
    for field in projection.fields() {
        budget.charge(Resource::PredicateExpressionSteps, 2)?; // visit + field tag
        admit_expr_hash(field.expr(), budget)?;
    }
    Ok(())
}

pub(in crate::db::query::fingerprint) fn admit_expr_hash(
    expr: &Expr,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    expr.try_for_each_tree_expr(&mut |node| {
        budget.charge(Resource::PredicateExpressionSteps, EXPR_HASH_NODE_STEPS)?;
        match node {
            Expr::Field(field) => budget.charge(
                Resource::PredicateExpressionSteps,
                field.as_str().len() as u64,
            ),
            Expr::FieldPath(path) => {
                budget.charge(
                    Resource::PredicateExpressionSteps,
                    path.root().as_str().len() as u64,
                )?;
                for segment in path.segments() {
                    budget.charge(Resource::PredicateExpressionSteps, 5 + segment.len() as u64)?;
                }
                Ok(())
            }
            Expr::Literal(value) => budget.admit_value_hash(value),
            Expr::FunctionCall { function, .. } => budget.charge(
                Resource::PredicateExpressionSteps,
                function.canonical_label().len() as u64,
            ),
            Expr::Aggregate(aggregate) => {
                // The visitor treats aggregates as leaves. Follow only the
                // canonical operands the encoder uses, including COUNT's
                // discarded-input normalization, without cloning raw trees.
                let key = AggregateSemanticKeyRef::from_aggregate_expr(aggregate);
                for child in [key.input_expr(), key.filter_expr()].into_iter().flatten() {
                    admit_expr_hash(child, budget)?;
                }
                Ok(())
            }
            Expr::Unary { .. } | Expr::Binary { .. } | Expr::Case { .. } => Ok(()),
            #[cfg(test)]
            Expr::Alias { .. } => Ok(()),
        }
    })
}
