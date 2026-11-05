use super::{RoundProjectionExpr, add};
use crate::{
    db::{
        QueryError,
        query::plan::expr::{BinaryOp, Expr, FieldId},
    },
    types::U256,
    value::Value,
};
use icydb_diagnostic_code::{DiagnosticCode, DiagnosticDetail, QueryProjectionCode};

fn assert_query_projection_error(err: QueryError, reason: QueryProjectionCode) {
    let diagnostic = err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        DiagnosticCode::QueryUnsupportedProjection
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&DiagnosticDetail::QueryProjection { reason }),
    );
}

#[test]
fn fluent_numeric_projection_accepts_constructed_u256_literals() {
    let projection = add("amount", U256::from(3_u64));

    assert_eq!(
        projection.expr(),
        &Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("amount"))),
            right: Box::new(Expr::Literal(Value::U256(U256::from(3_u64)))),
        },
    );
}

#[test]
fn round_projection_rejects_negative_scale_with_compact_projection_code() {
    let err = RoundProjectionExpr::new("age", Expr::Field(FieldId::new("age")), Value::Int64(-1))
        .expect_err("negative ROUND scale should fail closed");

    assert_query_projection_error(err, QueryProjectionCode::NumericScaleArguments);
}

#[test]
fn round_projection_rejects_non_integer_scale_with_compact_projection_code() {
    let err = RoundProjectionExpr::new(
        "age",
        Expr::Field(FieldId::new("age")),
        Value::Text("invalid".to_string()),
    )
    .expect_err("non-integer ROUND scale should fail closed");

    assert_query_projection_error(err, QueryProjectionCode::NumericScaleArguments);
}
