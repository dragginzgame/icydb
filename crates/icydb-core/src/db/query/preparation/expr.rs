//! Copy admitted expression operands without normalization or a sizing walk.

use crate::db::{
    QueryError,
    query::{
        plan::{
            OrderSpec, OrderTerm,
            expr::{CaseWhenArm, Expr, FieldId, FieldPath},
        },
        preparation::PreparationWork,
    },
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

impl PreparationWork<'_> {
    /// Copy ordering operands without canonicalization or a second sizing walk.
    pub(in crate::db) fn copy_order_spec(
        &self,
        order: &OrderSpec,
    ) -> Result<OrderSpec, QueryError> {
        Ok(OrderSpec {
            fields: self.copy_slice(&order.fields, |term| {
                Ok(OrderTerm::new(
                    self.copy_expr(term.expr())?,
                    term.direction(),
                ))
            })?,
        })
    }

    /// Copy an input-admitted tree under the current request. Callers must
    /// validate input depth before entering this recursive construction path;
    /// partial results cannot exceed that depth and are discarded on failure.
    pub(in crate::db) fn copy_expr(&self, expr: &Expr) -> Result<Expr, QueryError> {
        self.charge(Resource::PredicateExpressionSteps, 1)?;
        Ok(match expr {
            Expr::Field(field) => Expr::Field(FieldId::new(self.copy_text(field.as_str())?)),
            Expr::FieldPath(path) => Expr::FieldPath(FieldPath::new(
                self.copy_text(path.root().as_str())?,
                self.copy_slice(path.segments(), |segment| self.copy_text(segment))?,
            )),
            Expr::Literal(value) => Expr::Literal(self.copy_value(value)?),
            Expr::FunctionCall { function, args } => Expr::FunctionCall {
                function: *function,
                args: self.copy_slice(args, |arg| self.copy_expr(arg))?,
            },
            Expr::Unary { op, expr } => Expr::Unary {
                op: *op,
                expr: self.copy_boxed_expr(expr)?,
            },
            Expr::Binary { op, left, right } => Expr::Binary {
                op: *op,
                left: self.copy_boxed_expr(left)?,
                right: self.copy_boxed_expr(right)?,
            },
            Expr::Case {
                when_then_arms,
                else_expr,
            } => Expr::Case {
                when_then_arms: self.copy_slice(when_then_arms, |arm| {
                    Ok(CaseWhenArm::new(
                        self.copy_expr(arm.condition())?,
                        self.copy_expr(arm.result())?,
                    ))
                })?,
                else_expr: self.copy_boxed_expr(else_expr)?,
            },
            Expr::Aggregate(aggregate) => Expr::Aggregate(aggregate.copy_for_preparation(self)?),
            #[cfg(test)]
            Expr::Alias { expr, name } => Expr::Alias {
                expr: self.copy_boxed_expr(expr)?,
                name: self.copy_text(name.as_str())?.into(),
            },
        })
    }

    /// Charge the retained operand box before copying its admitted child.
    pub(in crate::db) fn copy_boxed_expr(&self, expr: &Expr) -> Result<Box<Expr>, QueryError> {
        self.charge(Resource::TemporaryBytes, size_of::<Expr>() as u64)?;
        Ok(Box::new(self.copy_expr(expr)?))
    }
}

#[cfg(test)]
mod tests;
