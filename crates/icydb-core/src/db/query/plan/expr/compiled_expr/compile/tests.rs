use super::PreviewConstructionBudget;
use crate::{
    db::query::plan::expr::{BinaryOp, CompiledExpr, CompiledExprCaseArm, CompiledExprValueReader},
    value::Value,
};
use std::borrow::Cow;

struct Row([Value; 2]);

impl CompiledExprValueReader for Row {
    fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>> {
        self.0.get(slot).map(Cow::Borrowed)
    }

    fn read_group_key(&self, _: usize) -> Option<Cow<'_, Value>> {
        None
    }

    fn read_aggregate(&self, _: usize) -> Option<Cow<'_, Value>> {
        None
    }
}

const fn slot(slot: usize) -> CompiledExpr {
    CompiledExpr::Slot { slot }
}

#[test]
fn case_specialization_preserves_zero_single_and_multiple_arm_results() {
    for count in [0, 1, 2] {
        for selected in [false, true] {
            let text = "selected result".to_string();
            let pointer = text.as_ptr();
            let mut arms = Vec::new();
            if count == 2 {
                arms.push(CompiledExprCaseArm::new(
                    CompiledExpr::Literal(Value::Bool(false)),
                    CompiledExpr::Literal(Value::Text("not selected".into())),
                ));
            }
            if count > 0 {
                arms.push(CompiledExprCaseArm::new(
                    CompiledExpr::Literal(Value::Bool(selected)),
                    CompiledExpr::Literal(Value::Text(text)),
                ));
            }
            let compiled = CompiledExpr::compile_case(
                arms,
                CompiledExpr::Literal(Value::Text("else".into())),
                &PreviewConstructionBudget,
            )
            .unwrap();
            let row = Row([Value::Null, Value::Null]);
            let result = compiled.evaluate(&row).unwrap();
            if count > 0 && selected {
                let Value::Text(text) = result.as_ref() else {
                    panic!("expected text");
                };
                assert_eq!(text, "selected result");
                assert_eq!(text.as_ptr(), pointer);
            } else {
                assert_eq!(result.as_ref(), &Value::Text("else".into()));
            }
        }
    }
}

#[test]
fn binary_specialization_preserves_slots_and_moves_literals() {
    let compiled =
        CompiledExpr::compile_binary(BinaryOp::Add, slot(0), slot(1), &PreviewConstructionBudget)
            .unwrap();
    let CompiledExpr::Add {
        left_slot,
        right_slot,
    } = compiled
    else {
        panic!("slot arithmetic should retain its direct form");
    };
    assert_eq!((left_slot, right_slot), (0, 1));

    for slot_on_left in [true, false] {
        let literal = "large literal".repeat(100);
        let pointer = literal.as_ptr();
        let field = slot(1);
        let literal = CompiledExpr::Literal(Value::Text(literal));
        let (left, right) = if slot_on_left {
            (field, literal)
        } else {
            (literal, field)
        };
        let compiled =
            CompiledExpr::compile_binary(BinaryOp::Lt, left, right, &PreviewConstructionBudget)
                .unwrap();
        let CompiledExpr::BinarySlotLiteral {
            op,
            slot,
            literal: Value::Text(literal),
            slot_on_left: actual,
        } = compiled
        else {
            panic!("slot/literal comparison should retain its direct form");
        };
        assert_eq!(op, BinaryOp::Lt);
        assert_eq!(slot, 1);
        assert_eq!(actual, slot_on_left);
        assert_eq!(literal.as_ptr(), pointer);
    }
}

#[test]
fn binary_specialization_matches_generic_evaluation() {
    for op in [
        BinaryOp::Add,
        BinaryOp::Sub,
        BinaryOp::Mul,
        BinaryOp::Div,
        BinaryOp::Eq,
        BinaryOp::Ne,
        BinaryOp::Lt,
        BinaryOp::Lte,
        BinaryOp::Gt,
        BinaryOp::Gte,
        BinaryOp::And,
        BinaryOp::Or,
    ] {
        for values in [
            [Value::Nat64(8), Value::Nat64(2)],
            [Value::Nat64(8), Value::Nat64(0)],
            [Value::Bool(false), Value::Bool(true)],
            [Value::Null, Value::Nat64(1)],
            [Value::Text("a".into()), Value::Text("b".into())],
        ] {
            let row = Row(values);
            for (left, right) in [
                (slot(0), slot(1)),
                (slot(0), CompiledExpr::Literal(row.0[1].clone())),
                (CompiledExpr::Literal(row.0[0].clone()), slot(1)),
                (
                    CompiledExpr::Literal(row.0[0].clone()),
                    CompiledExpr::Literal(row.0[1].clone()),
                ),
            ] {
                let generic = CompiledExpr::Binary {
                    op,
                    left: Box::new(left.clone()),
                    right: Box::new(right.clone()),
                };
                let compiled =
                    CompiledExpr::compile_binary(op, left, right, &PreviewConstructionBudget)
                        .unwrap();
                assert_eq!(compiled.evaluate(&row), generic.evaluate(&row), "{op:?}");
            }
        }
    }
}

#[test]
fn compiled_expr_constant_case_condition_is_hoisted() {
    assert_eq!(
        CompiledExpr::compile_case(
            vec![CompiledExprCaseArm::new(
                CompiledExpr::Literal(Value::Bool(false)),
                CompiledExpr::Literal(Value::Text("then".into())),
            )],
            CompiledExpr::Literal(Value::Text("else".into())),
            &PreviewConstructionBudget,
        )
        .unwrap(),
        CompiledExpr::Literal(Value::Text("else".to_string())),
    );
}
