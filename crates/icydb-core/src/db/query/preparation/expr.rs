//! Preparation error adapters for the shared expression construction owner.

#[cfg(test)]
mod tests;

use crate::db::{
    QueryError,
    query::{
        construction::ConstructionBudget,
        plan::{OrderSpec, expr::Expr},
        preparation::PreparationWork,
    },
};

impl PreparationWork<'_> {
    /// Copy ordering through the shared construction owner without normalization.
    pub(in crate::db) fn copy_order_spec(
        &self,
        order: &OrderSpec,
    ) -> Result<OrderSpec, QueryError> {
        (self as &dyn ConstructionBudget)
            .copy_order_spec(order)
            .map_err(QueryError::execute)
    }

    /// Copy admitted syntax under this preparation request's existing authority.
    pub(in crate::db) fn copy_expr(&self, expr: &Expr) -> Result<Expr, QueryError> {
        (self as &dyn ConstructionBudget)
            .copy_expr(expr)
            .map_err(QueryError::execute)
    }
}
