use crate::{
    db::{
        query::{
            plan::expr::{BinaryOp, Expr, FieldId},
            preparation::with_preparation_work,
        },
        sql::{
            lowering::{
                SqlLoweringError,
                expr::{SqlExprPhase, lower_sql_expr, validate_numeric_scale_function_scale},
            },
            parser::{SqlExpr, SqlExprBinaryOp},
        },
    },
    types::{
        Date, Decimal, Duration, Float32 as F32, Float64 as F64, IntBig, NatBig, Timestamp, U256,
    },
    value::Value,
};
use icydb_diagnostic_code::{DiagnosticDetail, QueryProjectionCode};

fn arithmetic(op: SqlExprBinaryOp, value: Value) -> SqlExpr {
    SqlExpr::Binary {
        op,
        left: Box::new(SqlExpr::Field("amount".to_string())),
        right: Box::new(SqlExpr::Literal(value)),
    }
}

#[test]
fn arithmetic_preserves_all_dynamic_numeric_families_and_operators() {
    let values = [
        Value::Int64(-3),
        Value::Int128(-3),
        Value::IntBig(IntBig::from(-3)),
        Value::Nat64(3),
        Value::Nat128(3),
        Value::NatBig(NatBig::from(3_u64)),
        Value::U256(U256::from(3_u64)),
        Value::Decimal(Decimal::new(123, 2)),
        Value::Float32(F32::try_new(1.25).unwrap()),
        Value::Float64(F64::try_new(2.5).unwrap()),
        Value::Duration(Duration::from_secs(1)),
        Value::Timestamp(Timestamp::from_secs(1)),
        Value::Date(Date::try_new(2024, 1, 2).unwrap()),
    ];
    with_preparation_work(|work| {
        for (sql_op, op) in [
            (SqlExprBinaryOp::Add, BinaryOp::Add),
            (SqlExprBinaryOp::Sub, BinaryOp::Sub),
            (SqlExprBinaryOp::Mul, BinaryOp::Mul),
            (SqlExprBinaryOp::Div, BinaryOp::Div),
        ] {
            for value in &values {
                let syntax = arithmetic(sql_op, value.clone());
                for phase in [
                    SqlExprPhase::Scalar,
                    SqlExprPhase::Where,
                    SqlExprPhase::PreAggregate,
                    SqlExprPhase::PostAggregate,
                ] {
                    assert_eq!(
                        lower_sql_expr(&syntax, phase, work).unwrap(),
                        Expr::Binary {
                            op,
                            left: Box::new(Expr::Field(FieldId::new("amount"))),
                            right: Box::new(Expr::Literal(value.clone())),
                        }
                    );
                }
            }
        }
    });
}

#[test]
fn arithmetic_rejects_non_numeric_literals_with_current_projection_detail() {
    with_preparation_work(|work| {
        for op in [
            SqlExprBinaryOp::Add,
            SqlExprBinaryOp::Sub,
            SqlExprBinaryOp::Mul,
            SqlExprBinaryOp::Div,
        ] {
            for value in [
                Value::Null,
                Value::Bool(true),
                Value::Text("3".to_string()),
                Value::Blob(vec![3]),
                Value::List(vec![Value::Nat64(3)]),
                Value::Map(vec![]),
                Value::Unit,
            ] {
                let err =
                    lower_sql_expr(&arithmetic(op, value), SqlExprPhase::Scalar, work).unwrap_err();
                let SqlLoweringError::Query(err) = err else {
                    panic!("expected query error");
                };
                assert_eq!(
                    err.diagnostic().detail(),
                    Some(&DiagnosticDetail::QueryProjection {
                        reason: QueryProjectionCode::NumericLiteralRequired,
                    })
                );
            }
        }
    });
}

#[test]
fn comparisons_do_not_acquire_arithmetic_literal_restrictions() {
    with_preparation_work(|work| {
        for op in [
            SqlExprBinaryOp::Eq,
            SqlExprBinaryOp::Ne,
            SqlExprBinaryOp::Lt,
            SqlExprBinaryOp::Lte,
            SqlExprBinaryOp::Gt,
            SqlExprBinaryOp::Gte,
        ] {
            assert!(
                lower_sql_expr(
                    &arithmetic(op, Value::Text("x".to_string())),
                    SqlExprPhase::Where,
                    work
                )
                .is_ok()
            );
        }
    });
}

#[test]
fn numeric_scale_validation_borrows_and_preserves_integer_bounds() {
    for (value, expected) in [
        (Value::Int64(0), 0),
        (Value::Nat64(u64::from(u32::MAX)), u32::MAX),
    ] {
        assert_eq!(
            validate_numeric_scale_function_scale(&value).unwrap(),
            expected
        );
    }
    for value in [
        Value::Int64(-1),
        Value::Nat64(u64::from(u32::MAX) + 1),
        Value::Text("0".repeat(4096)),
        Value::List(vec![Value::Nat64(1)]),
    ] {
        assert!(validate_numeric_scale_function_scale(&value).is_err());
    }
}
