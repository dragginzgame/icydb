use super::newtype_query_schema;
use crate::{
    db::query::{
        builder::count,
        plan::expr::{
            BinaryOp, CaseWhenArm, CompiledExpr, Expr, FieldId, FieldPath, Function, UnaryOp,
            compile_scalar_projection_expr_with_schema,
        },
    },
    value::Value,
};

fn literal(value: Value) -> Expr {
    Expr::Literal(value)
}

fn case(condition: Expr, result: Expr, otherwise: Expr) -> Expr {
    Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(condition, result)],
        else_expr: Box::new(otherwise),
    }
}

#[test]
fn scalar_compilation_resolves_accepted_slots_and_preserves_syntax() {
    let schema = newtype_query_schema();
    let cases = [
        (
            Expr::Field(FieldId::new("id")),
            CompiledExpr::Slot {
                slot: 0,
                field: "id".to_string(),
            },
        ),
        (
            Expr::FieldPath(FieldPath::new("profile", vec!["name".to_string()])),
            CompiledExpr::FieldPath {
                root_slot: 2,
                field: "profile.name".to_string(),
                segments: vec!["name".to_string()].into_boxed_slice(),
                segment_bytes: vec![b"name".to_vec().into_boxed_slice()].into_boxed_slice(),
            },
        ),
        (
            Expr::FunctionCall {
                function: Function::Coalesce,
                args: vec![
                    literal(Value::Null),
                    literal(Value::Text("évidence".to_string())),
                ],
            },
            CompiledExpr::FunctionCall {
                function: Function::Coalesce,
                args: vec![
                    CompiledExpr::Literal(Value::Null),
                    CompiledExpr::Literal(Value::Text("évidence".to_string())),
                ]
                .into_boxed_slice(),
            },
        ),
        (
            Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(literal(Value::Bool(true))),
            },
            CompiledExpr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(CompiledExpr::Literal(Value::Bool(true))),
            },
        ),
        (
            Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Field(FieldId::new("id"))),
                right: Box::new(literal(Value::Nat64(3))),
            },
            CompiledExpr::BinarySlotLiteral {
                op: BinaryOp::Add,
                slot: 0,
                field: "id".to_string(),
                literal: Value::Nat64(3),
                slot_on_left: true,
            },
        ),
    ];
    for (expr, expected) in cases {
        let original = expr.clone();
        for _ in 0..2 {
            assert_eq!(
                crate::db::query::preparation::with_preparation_work(|work| {
                    compile_scalar_projection_expr_with_schema(&schema, &expr, work)
                })
                .unwrap(),
                Some(expected.clone())
            );
            assert_eq!(expr, original);
        }
    }
}

#[test]
fn scalar_compilation_validates_discarded_case_branches() {
    let schema = newtype_query_schema();
    let invalid = [
        Expr::Field(FieldId::new("missing")),
        Expr::FieldPath(FieldPath::new("missing", vec!["name".to_string()])),
        Expr::Aggregate(count()),
        Expr::FunctionCall {
            function: Function::Coalesce,
            args: vec![literal(Value::Null), Expr::Field(FieldId::new("missing"))],
        },
    ];
    for rejected in invalid {
        let cases = [
            case(
                literal(Value::Bool(false)),
                rejected.clone(),
                literal(Value::Nat64(1)),
            ),
            case(
                literal(Value::Null),
                rejected.clone(),
                literal(Value::Nat64(1)),
            ),
            case(
                literal(Value::Bool(true)),
                literal(Value::Nat64(1)),
                rejected.clone(),
            ),
            case(
                rejected.clone(),
                literal(Value::Nat64(1)),
                literal(Value::Nat64(2)),
            ),
            // An outer constant must not hide invalid syntax in an inner CASE.
            case(
                literal(Value::Bool(true)),
                literal(Value::Nat64(1)),
                case(
                    literal(Value::Bool(false)),
                    rejected.clone(),
                    literal(Value::Nat64(2)),
                ),
            ),
            Expr::Case {
                when_then_arms: vec![
                    CaseWhenArm::new(literal(Value::Bool(true)), literal(Value::Nat64(1))),
                    CaseWhenArm::new(literal(Value::Bool(false)), rejected),
                ],
                else_expr: Box::new(literal(Value::Nat64(2))),
            },
        ];
        for expr in cases {
            assert!(
                crate::db::query::preparation::with_preparation_work(|work| {
                    compile_scalar_projection_expr_with_schema(&schema, &expr, work)
                })
                .unwrap()
                .is_none()
            );
        }
    }
}

#[test]
fn scalar_compilation_specializes_admitted_constant_cases() {
    let schema = newtype_query_schema();
    for (condition, expected) in [
        (Value::Bool(true), 1),
        (Value::Bool(false), 2),
        (Value::Null, 2),
    ] {
        let expr = case(
            literal(condition),
            literal(Value::Nat64(1)),
            literal(Value::Nat64(2)),
        );
        assert_eq!(
            crate::db::query::preparation::with_preparation_work(|work| {
                compile_scalar_projection_expr_with_schema(&schema, &expr, work)
            })
            .unwrap(),
            Some(CompiledExpr::Literal(Value::Nat64(expected)))
        );
    }
}
