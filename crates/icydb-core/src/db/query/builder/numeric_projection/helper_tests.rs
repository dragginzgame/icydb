//! Test-only constructors and adapters for the parent module.

use super::*;

impl RoundProjectionExpr {
    pub(in crate::db) fn new(
        field: impl Into<String>,
        inner: Expr,
        scale: Value,
    ) -> Result<Self, QueryError> {
        match scale {
            Value::Int64(value) if value < 0 => {
                return Err(QueryError::unsupported_projection(
                    icydb_diagnostic_code::QueryProjectionCode::NumericScaleArguments,
                ));
            }
            Value::Int64(_) | Value::Nat64(_) => {}
            _ => {
                return Err(QueryError::unsupported_projection(
                    icydb_diagnostic_code::QueryProjectionCode::NumericScaleArguments,
                ));
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
}
