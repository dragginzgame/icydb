//! Module: db::query::expr::order
//! Responsibility: typed fluent ORDER BY expression DTOs and lowering.
//! Does not own: planner validation or execution-time order evaluation.
//! Boundary: converts fluent order inputs into planner-owned order terms.

use crate::db::{
    QueryError,
    query::{
        builder::{
            AggregateExpr, FieldRef, NumericProjectionExpr, RoundProjectionExpr, TextProjectionExpr,
        },
        plan::{
            OrderDirection, OrderTerm as PlannedOrderTerm,
            expr::{Expr, FieldId},
        },
        preparation::PreparationWork,
    },
};

///
/// OrderExpr
///
/// Typed fluent ORDER BY expression wrapper.
/// This exists so fluent code can construct planner-owned ORDER BY
/// semantics directly at the query boundary.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderExpr {
    expr: Expr,
}

impl OrderExpr {
    /// Build one direct field ORDER BY expression.
    #[must_use]
    pub fn field(field: impl Into<String>) -> Self {
        let field = field.into();

        Self {
            expr: Expr::Field(FieldId::new(field)),
        }
    }

    /// Freeze one typed fluent order expression onto the planner-owned
    /// semantic expression now that labels are derived only at explain/hash
    /// edges instead of being stored in fluent order shells.
    const fn new(expr: Expr) -> Self {
        Self { expr }
    }
}

impl From<&str> for OrderExpr {
    fn from(value: &str) -> Self {
        Self::field(value)
    }
}

impl From<String> for OrderExpr {
    fn from(value: String) -> Self {
        Self::field(value)
    }
}

impl From<FieldRef> for OrderExpr {
    fn from(value: FieldRef) -> Self {
        Self::field(value.as_str())
    }
}

impl From<TextProjectionExpr> for OrderExpr {
    fn from(value: TextProjectionExpr) -> Self {
        Self::new(value.expr().clone())
    }
}

impl From<NumericProjectionExpr> for OrderExpr {
    fn from(value: NumericProjectionExpr) -> Self {
        Self::new(value.expr().clone())
    }
}

impl From<RoundProjectionExpr> for OrderExpr {
    fn from(value: RoundProjectionExpr) -> Self {
        Self::new(value.expr().clone())
    }
}

impl From<AggregateExpr> for OrderExpr {
    fn from(value: AggregateExpr) -> Self {
        Self::new(Expr::Aggregate(value))
    }
}

///
/// OrderTerm
///
/// Typed fluent ORDER BY term.
/// Carries one typed ORDER BY expression plus direction so fluent builders can
/// express deterministic ordering directly at the query boundary.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderTerm {
    expr: OrderExpr,
    direction: OrderDirection,
}

impl OrderTerm {
    /// Copy admitted ordering syntax without changing direction or expression shape.
    pub(in crate::db) fn copy_for_preparation(
        &self,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        Ok(Self {
            expr: OrderExpr::new(work.copy_expr(self.expression())?),
            direction: self.direction,
        })
    }

    /// Borrow authored syntax for admission before request preparation copies it.
    pub(in crate::db) const fn expression(&self) -> &Expr {
        &self.expr.expr
    }

    /// Build one ascending ORDER BY term from one typed expression.
    #[must_use]
    pub fn asc(expr: impl Into<OrderExpr>) -> Self {
        Self {
            expr: expr.into(),
            direction: OrderDirection::Asc,
        }
    }

    /// Build one descending ORDER BY term from one typed expression.
    #[must_use]
    pub fn desc(expr: impl Into<OrderExpr>) -> Self {
        Self {
            expr: expr.into(),
            direction: OrderDirection::Desc,
        }
    }

    /// Lower one typed fluent order term directly into the planner-owned
    /// `OrderTerm` contract.
    pub(in crate::db) fn lower(self) -> PlannedOrderTerm {
        PlannedOrderTerm::new(self.expr.expr, self.direction)
    }
}

/// Build one typed direct-field ORDER BY expression.
#[must_use]
pub fn field(field: impl Into<String>) -> OrderExpr {
    OrderExpr::field(field)
}

/// Build one ascending typed ORDER BY term.
#[must_use]
pub fn asc(expr: impl Into<OrderExpr>) -> OrderTerm {
    OrderTerm::asc(expr)
}

/// Build one descending typed ORDER BY term.
#[must_use]
pub fn desc(expr: impl Into<OrderExpr>) -> OrderTerm {
    OrderTerm::desc(expr)
}

#[cfg(test)]
mod tests {
    use super::{OrderExpr, OrderTerm};
    use crate::{
        db::query::{
            builder::sum,
            plan::{OrderDirection, expr::Expr},
        },
        value::Value,
    };

    #[test]
    fn owned_order_lowering_preserves_operands_and_direction() {
        for direction in [OrderDirection::Asc, OrderDirection::Desc] {
            let aggregate = sum("amount")
                .with_filter_expr(Expr::Literal(Value::Text("x".repeat(4096))))
                .distinct();
            let input_address = std::ptr::from_ref(aggregate.input_expr().expect("input"));
            let filter_address = std::ptr::from_ref(aggregate.filter_expr().expect("filter"));
            let expected = Expr::Aggregate(aggregate.clone());
            let term = OrderTerm {
                expr: OrderExpr::from(aggregate),
                direction,
            };

            let lowered = term.lower();
            assert_eq!(lowered.direction(), direction);
            assert_eq!(lowered.expr(), &expected);
            let Expr::Aggregate(aggregate) = lowered.expr() else {
                panic!("aggregate order expression");
            };
            assert_eq!(
                std::ptr::from_ref(aggregate.input_expr().expect("input")),
                input_address
            );
            assert_eq!(
                std::ptr::from_ref(aggregate.filter_expr().expect("filter")),
                filter_address
            );
        }
    }
}
