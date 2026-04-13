//! Module: query::builder::numeric_projection
//! Responsibility: shared bounded numeric projection helpers used by fluent
//! terminals and SQL lowering.
//! Does not own: generic arithmetic expression parsing, grouped semantics, or
//! executor routing.
//! Boundary: this models the admitted scalar arithmetic surface without
//! opening a general expression-builder API.

use crate::{
    db::{
        QueryError,
        executor::projection::eval_binary_expr,
        query::{
            builder::{
                ValueProjectionExpr, scalar_projection::render_scalar_projection_expr_sql_label,
            },
            plan::expr::{BinaryOp, Expr, FieldId},
        },
    },
    traits::{FieldValue, NumericValue},
    value::Value,
};

///
/// NumericProjectionExpr
///
/// Shared bounded numeric projection over one source field and one numeric
/// literal.
/// This currently stays on the narrow `field + literal` seam admitted by the
/// shipped SQL and fluent surfaces.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NumericProjectionExpr {
    field: String,
    expr: Expr,
}

impl NumericProjectionExpr {
    // Build one field-plus-literal numeric projection after validating that
    // the literal stays on the admitted numeric seam.
    pub(in crate::db) fn add_value(
        field: impl Into<String>,
        literal: Value,
    ) -> Result<Self, QueryError> {
        if !matches!(
            literal,
            Value::Int(_)
                | Value::Int128(_)
                | Value::IntBig(_)
                | Value::Uint(_)
                | Value::Uint128(_)
                | Value::UintBig(_)
                | Value::Decimal(_)
                | Value::Float32(_)
                | Value::Float64(_)
                | Value::Duration(_)
                | Value::Timestamp(_)
                | Value::Date(_)
        ) {
            return Err(QueryError::unsupported_query(format!(
                "scalar numeric projection requires a numeric literal, found {literal:?}",
            )));
        }

        let field = field.into();

        Ok(Self {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Field(FieldId::new(field.clone()))),
                right: Box::new(Expr::Literal(literal)),
            },
            field,
        })
    }

    // Build one field-plus-literal numeric projection from one typed numeric
    // literal helper.
    pub(in crate::db) fn add_numeric_literal(
        field: impl Into<String>,
        literal: impl FieldValue + NumericValue,
    ) -> Self {
        let literal = literal.to_value();

        Self::add_value(field, literal)
            .expect("typed numeric projection helpers should always produce numeric literals")
    }

    /// Borrow the canonical planner expression carried by this helper.
    #[must_use]
    pub(in crate::db) const fn expr(&self) -> &Expr {
        &self.expr
    }
}

impl ValueProjectionExpr for NumericProjectionExpr {
    fn field(&self) -> &str {
        self.field.as_str()
    }

    fn sql_label(&self) -> String {
        render_scalar_projection_expr_sql_label(&self.expr)
    }

    fn apply_value(&self, value: Value) -> Result<Value, QueryError> {
        let Expr::Binary { op, right, .. } = &self.expr else {
            return Err(QueryError::invariant(
                "numeric projection helper must retain one binary expression",
            ));
        };
        let Expr::Literal(literal) = right.as_ref() else {
            return Err(QueryError::invariant(
                "numeric projection helper must retain one literal right operand",
            ));
        };

        eval_binary_expr(*op, &value, literal)
            .map_err(|err| QueryError::unsupported_query(err.to_string()))
    }
}

/// Build `field + literal`.
#[must_use]
pub fn add(
    field: impl AsRef<str>,
    literal: impl FieldValue + NumericValue,
) -> NumericProjectionExpr {
    NumericProjectionExpr::add_numeric_literal(field.as_ref().to_string(), literal)
}
