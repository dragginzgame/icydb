//! Module: query::explain::projection
//! Responsibility: explain-only projection labels derived from query projection DTOs.
//! Does not own: executor descriptor assembly or projection runtime evaluation.
//! Boundary: query-owned projection expression shape -> stable explain display labels.
//! Cross-layer rule: executor callers must request richer labels here instead
//! of importing projection expression internals and matching `Expr` locally.

use crate::db::query::plan::expr::{Expr, ProjectionField};

/// Builds the query-owned explain label for one projected field.
///
/// This is the boundary used by executor explain descriptors when route or
/// projection diagnostics need a compact field label. Add new display cases
/// here rather than in executor code so projection expression interpretation
/// stays owned by `db::query`.
///
pub(in crate::db) fn explain_projection_field_name(field: &ProjectionField) -> String {
    match field {
        ProjectionField::Scalar { expr, .. } => explain_projection_expr_name(expr),
    }
}

// Collapse query projection expressions into the intentionally small label
// vocabulary used by EXPLAIN route diagnostics.
fn explain_projection_expr_name(expr: &Expr) -> String {
    match expr {
        Expr::Field(field) => field.as_str().to_string(),
        Expr::FieldPath(_) | Expr::Literal(_) | Expr::FunctionCall { .. } => "expr".to_string(),
        Expr::Aggregate(_) => "aggregate".to_string(),
        #[cfg(test)]
        Expr::Alias { expr, .. } => explain_projection_expr_name(expr),
        Expr::Unary { expr, .. } => explain_projection_expr_name(expr),
        Expr::Case { .. } | Expr::Binary { .. } => "expr".to_string(),
    }
}
