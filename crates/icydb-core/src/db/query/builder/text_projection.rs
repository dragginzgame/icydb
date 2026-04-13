//! Module: query::builder::text_projection
//! Responsibility: shared bounded text-function builder surface and function
//! semantics used by fluent terminals and canonical SQL projection execution.
//! Does not own: generic query planning, grouped semantics, or SQL parsing.
//! Boundary: models the admitted text-function family on top of canonical
//! planner expressions without reopening a generic function registry.

use crate::{
    db::{
        QueryError,
        executor::projection::eval_value_projection_expr_with_value,
        query::{
            builder::{
                ValueProjectionExpr, scalar_projection::render_scalar_projection_expr_sql_label,
            },
            plan::expr::{Expr, FieldId, Function},
        },
    },
    traits::FieldValue,
};

///
/// TextProjectionExpr
///
/// Shared bounded text-function projection over one source field.
/// This stays intentionally narrow even though it now lowers through the same
/// canonical `Expr` surface used by SQL projection planning.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TextProjectionExpr {
    field: String,
    expr: Expr,
}

impl TextProjectionExpr {
    // Build one field-function projection carrying only the source field.
    pub(in crate::db) fn unary(field: impl Into<String>, function: Function) -> Self {
        let field = field.into();

        Self {
            expr: Expr::FunctionCall {
                function,
                args: vec![Expr::Field(FieldId::new(field.clone()))],
            },
            field,
        }
    }

    // Build one field-function projection carrying one literal argument.
    pub(in crate::db) fn with_literal(
        field: impl Into<String>,
        function: Function,
        literal: impl FieldValue,
    ) -> Self {
        let field = field.into();

        Self {
            expr: Expr::FunctionCall {
                function,
                args: vec![
                    Expr::Field(FieldId::new(field.clone())),
                    Expr::Literal(literal.to_value()),
                ],
            },
            field,
        }
    }

    // Build one field-function projection carrying two literal arguments.
    pub(in crate::db) fn with_two_literals(
        field: impl Into<String>,
        function: Function,
        literal: impl FieldValue,
        literal2: impl FieldValue,
    ) -> Self {
        let field = field.into();

        Self {
            expr: Expr::FunctionCall {
                function,
                args: vec![
                    Expr::Field(FieldId::new(field.clone())),
                    Expr::Literal(literal.to_value()),
                    Expr::Literal(literal2.to_value()),
                ],
            },
            field,
        }
    }

    // Build one `POSITION(literal, field)` projection.
    pub(in crate::db) fn position(field: impl Into<String>, literal: impl FieldValue) -> Self {
        let field = field.into();

        Self {
            expr: Expr::FunctionCall {
                function: Function::Position,
                args: vec![
                    Expr::Literal(literal.to_value()),
                    Expr::Field(FieldId::new(field.clone())),
                ],
            },
            field,
        }
    }

    /// Borrow the canonical planner expression carried by this helper.
    #[must_use]
    pub(in crate::db) const fn expr(&self) -> &Expr {
        &self.expr
    }
}

impl ValueProjectionExpr for TextProjectionExpr {
    fn field(&self) -> &str {
        self.field.as_str()
    }

    fn sql_label(&self) -> String {
        render_scalar_projection_expr_sql_label(&self.expr)
    }

    fn apply_value(&self, value: crate::value::Value) -> Result<crate::value::Value, QueryError> {
        eval_value_projection_expr_with_value(&self.expr, self.field.as_str(), &value)
    }
}

/// Build `TRIM(field)`.
#[must_use]
pub fn trim(field: impl AsRef<str>) -> TextProjectionExpr {
    TextProjectionExpr::unary(field.as_ref().to_string(), Function::Trim)
}

/// Build `LTRIM(field)`.
#[must_use]
pub fn ltrim(field: impl AsRef<str>) -> TextProjectionExpr {
    TextProjectionExpr::unary(field.as_ref().to_string(), Function::Ltrim)
}

/// Build `RTRIM(field)`.
#[must_use]
pub fn rtrim(field: impl AsRef<str>) -> TextProjectionExpr {
    TextProjectionExpr::unary(field.as_ref().to_string(), Function::Rtrim)
}

/// Build `LOWER(field)`.
#[must_use]
pub fn lower(field: impl AsRef<str>) -> TextProjectionExpr {
    TextProjectionExpr::unary(field.as_ref().to_string(), Function::Lower)
}

/// Build `UPPER(field)`.
#[must_use]
pub fn upper(field: impl AsRef<str>) -> TextProjectionExpr {
    TextProjectionExpr::unary(field.as_ref().to_string(), Function::Upper)
}

/// Build `LENGTH(field)`.
#[must_use]
pub fn length(field: impl AsRef<str>) -> TextProjectionExpr {
    TextProjectionExpr::unary(field.as_ref().to_string(), Function::Length)
}

/// Build `LEFT(field, length)`.
#[must_use]
pub fn left(field: impl AsRef<str>, length: impl FieldValue) -> TextProjectionExpr {
    TextProjectionExpr::with_literal(field.as_ref().to_string(), Function::Left, length)
}

/// Build `RIGHT(field, length)`.
#[must_use]
pub fn right(field: impl AsRef<str>, length: impl FieldValue) -> TextProjectionExpr {
    TextProjectionExpr::with_literal(field.as_ref().to_string(), Function::Right, length)
}

/// Build `STARTS_WITH(field, literal)`.
#[must_use]
pub fn starts_with(field: impl AsRef<str>, literal: impl FieldValue) -> TextProjectionExpr {
    TextProjectionExpr::with_literal(field.as_ref().to_string(), Function::StartsWith, literal)
}

/// Build `ENDS_WITH(field, literal)`.
#[must_use]
pub fn ends_with(field: impl AsRef<str>, literal: impl FieldValue) -> TextProjectionExpr {
    TextProjectionExpr::with_literal(field.as_ref().to_string(), Function::EndsWith, literal)
}

/// Build `CONTAINS(field, literal)`.
#[must_use]
pub fn contains(field: impl AsRef<str>, literal: impl FieldValue) -> TextProjectionExpr {
    TextProjectionExpr::with_literal(field.as_ref().to_string(), Function::Contains, literal)
}

/// Build `POSITION(literal, field)`.
#[must_use]
pub fn position(field: impl AsRef<str>, literal: impl FieldValue) -> TextProjectionExpr {
    TextProjectionExpr::position(field.as_ref().to_string(), literal)
}

/// Build `REPLACE(field, from, to)`.
#[must_use]
pub fn replace(
    field: impl AsRef<str>,
    from: impl FieldValue,
    to: impl FieldValue,
) -> TextProjectionExpr {
    TextProjectionExpr::with_two_literals(field.as_ref().to_string(), Function::Replace, from, to)
}

/// Build `SUBSTRING(field, start)`.
#[must_use]
pub fn substring(field: impl AsRef<str>, start: impl FieldValue) -> TextProjectionExpr {
    TextProjectionExpr::with_literal(field.as_ref().to_string(), Function::Substring, start)
}

/// Build `SUBSTRING(field, start, length)`.
#[must_use]
pub fn substring_with_length(
    field: impl AsRef<str>,
    start: impl FieldValue,
    length: impl FieldValue,
) -> TextProjectionExpr {
    TextProjectionExpr::with_two_literals(
        field.as_ref().to_string(),
        Function::Substring,
        start,
        length,
    )
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn lower_text_projection_renders_sql_label() {
        assert_eq!(lower("name").sql_label(), "LOWER(name)");
    }

    #[test]
    fn replace_text_projection_applies_shared_transform() {
        let value = replace("name", "Ada", "Eve")
            .apply_value(Value::Text("Ada Ada".to_string()))
            .expect("replace projection should apply");

        assert_eq!(value, Value::Text("Eve Eve".to_string()));
    }
}
