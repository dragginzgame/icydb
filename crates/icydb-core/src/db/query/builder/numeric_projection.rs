//! Module: query::builder::numeric_projection
//! Responsibility: shared bounded numeric projection helpers used by fluent
//! terminals and structural lowering.
//! Does not own: generic arithmetic expression parsing, grouped semantics, or
//! executor routing.
//! Boundary: this models the admitted scalar arithmetic surface without
//! opening a general expression-builder API.

use crate::{
    db::{
        QueryError,
        query::{
            builder::{
                ValueProjectionExpr, scalar_projection::render_scalar_projection_expr_plan_label,
            },
            plan::expr::{BinaryOp, Expr, FieldId, Function, eval_builder_expr_for_value_preview},
        },
    },
    traits::NumericValue,
    value::{InputValue, Value},
};

///
/// NumericProjectionExpr
///
/// Shared bounded numeric projection over one source field and one numeric
/// literal.
/// This stays on the narrow `field op literal` seam admitted by the shipped
/// scalar projection surfaces.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NumericProjectionExpr {
    field: String,
    expr: Expr,
}

impl NumericProjectionExpr {
    // Build one bounded field-op-literal numeric projection after validating
    // that the literal stays on the admitted numeric seam.
    fn arithmetic_value(
        field: impl Into<String>,
        op: BinaryOp,
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
                op,
                left: Box::new(Expr::Field(FieldId::new(field.clone()))),
                right: Box::new(Expr::Literal(literal)),
            },
            field,
        })
    }

    // Build one bounded field-op-literal numeric projection from one typed
    // numeric literal helper.
    fn arithmetic_numeric_literal(
        field: impl Into<String>,
        op: BinaryOp,
        literal: impl Into<InputValue> + NumericValue,
    ) -> Self {
        let literal = Value::from(literal.into());

        Self::arithmetic_value(field, op, literal)
            .expect("typed numeric projection helpers should always produce numeric literals")
    }

    // Build one field-plus-literal numeric projection.
    pub(in crate::db) fn add_value(
        field: impl Into<String>,
        literal: Value,
    ) -> Result<Self, QueryError> {
        Self::arithmetic_value(field, BinaryOp::Add, literal)
    }

    // Build one field-minus-literal numeric projection.
    pub(in crate::db) fn sub_value(
        field: impl Into<String>,
        literal: Value,
    ) -> Result<Self, QueryError> {
        Self::arithmetic_value(field, BinaryOp::Sub, literal)
    }

    // Build one field-times-literal numeric projection.
    pub(in crate::db) fn mul_value(
        field: impl Into<String>,
        literal: Value,
    ) -> Result<Self, QueryError> {
        Self::arithmetic_value(field, BinaryOp::Mul, literal)
    }

    // Build one field-divided-by-literal numeric projection.
    pub(in crate::db) fn div_value(
        field: impl Into<String>,
        literal: Value,
    ) -> Result<Self, QueryError> {
        Self::arithmetic_value(field, BinaryOp::Div, literal)
    }

    // Build one field-plus-literal numeric projection from one typed numeric
    // literal helper.
    pub(in crate::db) fn add_numeric_literal(
        field: impl Into<String>,
        literal: impl Into<InputValue> + NumericValue,
    ) -> Self {
        Self::arithmetic_numeric_literal(field, BinaryOp::Add, literal)
    }

    // Build one field-minus-literal numeric projection from one typed numeric
    // literal helper.
    pub(in crate::db) fn sub_numeric_literal(
        field: impl Into<String>,
        literal: impl Into<InputValue> + NumericValue,
    ) -> Self {
        Self::arithmetic_numeric_literal(field, BinaryOp::Sub, literal)
    }

    // Build one field-times-literal numeric projection from one typed numeric
    // literal helper.
    pub(in crate::db) fn mul_numeric_literal(
        field: impl Into<String>,
        literal: impl Into<InputValue> + NumericValue,
    ) -> Self {
        Self::arithmetic_numeric_literal(field, BinaryOp::Mul, literal)
    }

    // Build one field-divided-by-literal numeric projection from one typed
    // numeric literal helper.
    pub(in crate::db) fn div_numeric_literal(
        field: impl Into<String>,
        literal: impl Into<InputValue> + NumericValue,
    ) -> Self {
        Self::arithmetic_numeric_literal(field, BinaryOp::Div, literal)
    }

    /// Borrow the canonical planner expression carried by this helper.
    #[must_use]
    pub(in crate::db) const fn expr(&self) -> &Expr {
        &self.expr
    }

    // Build one rounded projection over either a plain field or one existing
    // bounded numeric expression rooted in the same source field.
    pub(in crate::db) fn round_with_scale(
        &self,
        scale: u32,
    ) -> Result<RoundProjectionExpr, QueryError> {
        RoundProjectionExpr::new(
            self.field.clone(),
            self.expr.clone(),
            Value::Uint(u64::from(scale)),
        )
    }
}

impl ValueProjectionExpr for NumericProjectionExpr {
    fn field(&self) -> &str {
        self.field.as_str()
    }

    fn projection_label(&self) -> String {
        render_scalar_projection_expr_plan_label(&self.expr)
    }

    fn apply_value(&self, value: Value) -> Result<Value, QueryError> {
        eval_builder_expr_for_value_preview(&self.expr, self.field.as_str(), &value)
    }
}

///
/// RoundProjectionExpr
///
/// Shared bounded numeric rounding projection over one source field and one
/// canonical scalar numeric expression.
/// This keeps `ROUND` on the scalar projection seam without opening a generic
/// function-builder surface.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoundProjectionExpr {
    field: String,
    expr: Expr,
}

impl RoundProjectionExpr {
    // Build one bounded `ROUND(expr, scale)` projection after validating that
    // `scale` stays on the admitted non-negative integer seam.
    pub(in crate::db) fn new(
        field: impl Into<String>,
        inner: Expr,
        scale: Value,
    ) -> Result<Self, QueryError> {
        match scale {
            Value::Int(value) if value < 0 => {
                return Err(QueryError::unsupported_query(format!(
                    "ROUND(...) requires non-negative integer scale, found {value}",
                )));
            }
            Value::Int(_) | Value::Uint(_) => {}
            other => {
                return Err(QueryError::unsupported_query(format!(
                    "ROUND(...) requires integer scale, found {other:?}",
                )));
            }
        }

        Ok(Self {
            field: field.into(),
            expr: Expr::FunctionCall {
                function: Function::Round,
                args: vec![inner, Expr::Literal(scale)],
            },
        })
    }

    // Build one rounded field projection.
    pub(in crate::db) fn field(field: impl Into<String>, scale: u32) -> Result<Self, QueryError> {
        let field = field.into();

        Self::new(
            field.clone(),
            Expr::Field(FieldId::new(field)),
            Value::Uint(u64::from(scale)),
        )
    }

    /// Borrow the canonical planner expression carried by this helper.
    #[must_use]
    pub(in crate::db) const fn expr(&self) -> &Expr {
        &self.expr
    }
}

impl ValueProjectionExpr for RoundProjectionExpr {
    fn field(&self) -> &str {
        self.field.as_str()
    }

    fn projection_label(&self) -> String {
        render_scalar_projection_expr_plan_label(&self.expr)
    }

    fn apply_value(&self, value: Value) -> Result<Value, QueryError> {
        eval_builder_expr_for_value_preview(&self.expr, self.field.as_str(), &value)
    }
}

/// Build `field + literal`.
#[must_use]
pub fn add(
    field: impl AsRef<str>,
    literal: impl Into<InputValue> + NumericValue,
) -> NumericProjectionExpr {
    NumericProjectionExpr::add_numeric_literal(field.as_ref().to_string(), literal)
}

/// Build `field - literal`.
#[must_use]
pub fn sub(
    field: impl AsRef<str>,
    literal: impl Into<InputValue> + NumericValue,
) -> NumericProjectionExpr {
    NumericProjectionExpr::sub_numeric_literal(field.as_ref().to_string(), literal)
}

/// Build `field * literal`.
#[must_use]
pub fn mul(
    field: impl AsRef<str>,
    literal: impl Into<InputValue> + NumericValue,
) -> NumericProjectionExpr {
    NumericProjectionExpr::mul_numeric_literal(field.as_ref().to_string(), literal)
}

/// Build `field / literal`.
#[must_use]
pub fn div(
    field: impl AsRef<str>,
    literal: impl Into<InputValue> + NumericValue,
) -> NumericProjectionExpr {
    NumericProjectionExpr::div_numeric_literal(field.as_ref().to_string(), literal)
}

/// Build `ROUND(field, scale)`.
pub fn round(field: impl AsRef<str>, scale: u32) -> RoundProjectionExpr {
    RoundProjectionExpr::field(field.as_ref().to_string(), scale)
        .expect("ROUND(field, scale) helper should always produce a bounded projection")
}

/// Build `ROUND(expr, scale)` for one existing bounded numeric projection.
#[must_use]
pub fn round_expr(projection: &NumericProjectionExpr, scale: u32) -> RoundProjectionExpr {
    projection
        .round_with_scale(scale)
        .expect("ROUND(expr, scale) helper should always produce a bounded projection")
}
