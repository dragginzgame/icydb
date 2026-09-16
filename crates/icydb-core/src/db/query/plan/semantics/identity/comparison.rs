//! Admitted borrowed aggregate equality shared by planner validation and hashing.
//! Canonical meaning and structural equality stay with the semantic-key owner.

use crate::{
    db::query::{
        construction::ConstructionBudget,
        plan::{AggregateSemanticKeyRef, expr::Expr},
    },
    error::InternalError,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

impl AggregateSemanticKeyRef<'_> {
    /// Admit one borrowed semantic comparison before inspecting operand trees.
    /// Header mismatches avoid operand work; equality and normalization retain
    /// their existing authority. Callers supply depth-admitted planner trees.
    pub(in crate::db) fn try_eq_for_preparation(
        self,
        other: Self,
        budget: &dyn ConstructionBudget,
    ) -> Result<bool, InternalError> {
        budget.charge(Resource::PredicateExpressionSteps, 1)?;
        if self.kind() != other.kind()
            || self.distinct() != other.distinct()
            || self.input_expr().is_some() != other.input_expr().is_some()
            || self.filter_expr().is_some() != other.filter_expr().is_some()
        {
            return Ok(false);
        }
        admit_semantic_key_comparison(other, budget)?;
        Ok(self == other)
    }
}

// Derived structural equality can visit no more than one side's full tree:
// different variants/container lengths stop early, and matching lengths visit
// corresponding children. Admit that conservative extent before equality. The
// walk itself is charged incrementally (two visits per node cover both walks),
// uses only length metadata, and neither formats nor copies operands. Callers
// supply input-admitted trees; this is not a replacement depth-admission scope.
fn admit_semantic_key_comparison(
    key: AggregateSemanticKeyRef<'_>,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    for expr in [key.input_expr(), key.filter_expr()].into_iter().flatten() {
        admit_expr(expr, budget)?;
    }
    Ok(())
}

fn admit_expr(expr: &Expr, budget: &dyn ConstructionBudget) -> Result<(), InternalError> {
    expr.try_for_each_tree_expr(&mut |node| {
        budget.charge(Resource::PredicateExpressionSteps, 2)?;
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
                    budget.charge(Resource::PredicateExpressionSteps, 2 + segment.len() as u64)?;
                }
                Ok(())
            }
            Expr::Literal(value) => budget.admit_value_comparison(value),
            // The general expression visitor deliberately treats aggregates
            // as leaves, but raw nested aggregate equality includes children.
            Expr::Aggregate(aggregate) => {
                for child in [aggregate.input_expr(), aggregate.filter_expr()]
                    .into_iter()
                    .flatten()
                {
                    admit_expr(child, budget)?;
                }
                Ok(())
            }
            Expr::FunctionCall { .. }
            | Expr::Unary { .. }
            | Expr::Binary { .. }
            | Expr::Case { .. } => Ok(()),
            #[cfg(test)]
            Expr::Alias { name, .. } => budget.charge(
                Resource::PredicateExpressionSteps,
                name.as_str().len() as u64,
            ),
        }
    })
}
