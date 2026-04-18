//! Module: query::expr
//! Responsibility: schema-agnostic filter/order expression wrappers and lowering.
//! Does not own: planner route selection or executor evaluation.
//! Boundary: intent boundary lowers these to validated predicate/order forms.

use crate::db::{
    predicate::{Predicate, normalize, normalize_enum_literals},
    query::{
        builder::FieldRef,
        builder::{
            AggregateExpr, NumericProjectionExpr, RoundProjectionExpr, TextProjectionExpr,
            ValueProjectionExpr, scalar_projection::render_scalar_projection_expr_sql_label,
        },
        plan::{
            OrderDirection, OrderTerm as PlannedOrderTerm,
            expr::{Expr, FieldId},
        },
    },
    schema::{SchemaInfo, ValidateError, reject_unsupported_query_features, validate},
};

///
/// FilterExpr
/// Schema-agnostic filter expression for dynamic query input.
/// Lowered into a validated predicate at the intent boundary.
///

#[derive(Clone, Debug)]
pub struct FilterExpr(pub Predicate);

impl FilterExpr {
    /// Lower the filter expression into a validated predicate for the provided schema.
    pub(crate) fn lower_with(&self, schema: &SchemaInfo) -> Result<Predicate, ValidateError> {
        // Phase 1: normalize enum literals using schema enum metadata.
        let normalized_enum_literals = normalize_enum_literals(schema, &self.0)?;

        // Phase 2: reject unsupported query features and validate against schema.
        reject_unsupported_query_features(&normalized_enum_literals)?;
        validate(schema, &normalized_enum_literals)?;

        // Phase 3: normalize structural predicate shape for deterministic planning.
        Ok(normalize(&normalized_enum_literals))
    }
}

///
/// OrderExpr
///
/// Typed fluent ORDER BY expression wrapper.
/// This exists so fluent code can construct planner-owned ORDER BY
/// semantics directly at the query boundary.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderExpr {
    label: String,
    expr: Expr,
}

impl OrderExpr {
    /// Build one direct field ORDER BY expression.
    #[must_use]
    pub fn field(field: impl Into<String>) -> Self {
        let field = field.into();

        Self {
            label: field.clone(),
            expr: Expr::Field(FieldId::new(field)),
        }
    }

    // Freeze one typed fluent order expression into its stable planner-facing
    // label plus semantic expression so callers do not rediscover either shape.
    const fn new(label: String, expr: Expr) -> Self {
        Self { label, expr }
    }

    // Lower one typed fluent order expression into the planner-owned order
    // contract now that ordering is expression-based end to end.
    pub(in crate::db) fn lower(&self, direction: OrderDirection) -> PlannedOrderTerm {
        PlannedOrderTerm::new(self.label.clone(), self.expr.clone(), direction)
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
        Self::new(value.sql_label(), value.expr().clone())
    }
}

impl From<NumericProjectionExpr> for OrderExpr {
    fn from(value: NumericProjectionExpr) -> Self {
        Self::new(value.sql_label(), value.expr().clone())
    }
}

impl From<RoundProjectionExpr> for OrderExpr {
    fn from(value: RoundProjectionExpr) -> Self {
        Self::new(value.sql_label(), value.expr().clone())
    }
}

impl From<AggregateExpr> for OrderExpr {
    fn from(value: AggregateExpr) -> Self {
        let expr = Expr::Aggregate(value);

        Self::new(render_scalar_projection_expr_sql_label(&expr), expr)
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

    // Lower one typed fluent order term directly into the planner-owned
    // `OrderTerm` contract.
    pub(in crate::db) fn lower(&self) -> PlannedOrderTerm {
        self.expr.lower(self.direction)
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
