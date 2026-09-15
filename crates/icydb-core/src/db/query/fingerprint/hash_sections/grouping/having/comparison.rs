//! Admission for borrowed HAVING slot equality; does not define equality rules.

use crate::{
    db::query::{
        construction::ConstructionBudget,
        plan::{AggregateSemanticKeyRef, expr::Expr},
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

// Derived structural equality can visit no more than one side's full tree:
// different variants/container lengths stop early, and matching lengths visit
// corresponding children. Admit that conservative extent before equality. The
// walk itself is charged incrementally (two visits per node cover both walks),
// uses only length metadata, and neither formats nor copies operands. Callers
// supply input-admitted trees; this is not a replacement depth-admission scope.
pub(super) fn admit_semantic_key_comparison(
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
            Expr::Literal(value) => admit_value(value, budget),
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

fn admit_value(value: &Value, budget: &dyn ConstructionBudget) -> Result<(), InternalError> {
    budget.charge(Resource::NestedValueSteps, 2)?;
    let bytes = match value {
        Value::Text(text) => text.len() as u64,
        Value::Blob(blob) => blob.len() as u64,
        Value::IntBig(integer) => integer.magnitude_bits().div_ceil(64).saturating_mul(8),
        Value::NatBig(integer) => integer.magnitude_bits().div_ceil(64).saturating_mul(8),
        Value::List(values) => {
            for value in values {
                admit_value(value, budget)?;
            }
            0
        }
        Value::Map(entries) => {
            // Structural equality compares stored entries; it does not sort.
            for (key, value) in entries {
                admit_value(key, budget)?;
                admit_value(value, budget)?;
            }
            0
        }
        Value::Enum(value) => {
            if let Some(payload) = value.payload() {
                admit_value(payload, budget)?;
            }
            0
        }
        Value::Account(_)
        | Value::Bool(_)
        | Value::Date(_)
        | Value::Decimal(_)
        | Value::Duration(_)
        | Value::Float32(_)
        | Value::Float64(_)
        | Value::Int64(_)
        | Value::Int128(_)
        | Value::Nat64(_)
        | Value::Nat128(_)
        | Value::Null
        | Value::Principal(_)
        | Value::Subaccount(_)
        | Value::Timestamp(_)
        | Value::U256(_)
        | Value::Ulid(_)
        | Value::Unit => 0,
    };
    budget.charge(Resource::PredicateExpressionSteps, bytes)
}
