//! Module: executor::aggregate::contracts::state::grouped_expr
//! Responsibility: grouped aggregate input/filter expression evaluation.
//! Does not own: planner expression lowering or grouped projection output.
//! Boundary: compiles planner scalar expressions into slot-indexed grouped
//! evaluators so grouped fold rows do not traverse planner AST nodes.

use crate::{
    db::{
        executor::{
            pipeline::runtime::RowView,
            projection::{ProjectionEvalError, eval_binary_expr, eval_unary_expr},
        },
        query::plan::expr::{
            BinaryOp, Function, ProjectionFunctionEvalError, ScalarProjectionCaseArm,
            ScalarProjectionExpr, UnaryOp, admit_true_only_boolean_value,
            eval_projection_function_call_checked,
        },
    },
    value::Value,
};
use std::borrow::Cow;

///
/// GroupedCompiledExpr
///
/// GroupedCompiledExpr is the grouped-fold-local expression program produced
/// once when the grouped aggregate bundle is prepared.
/// It stores resolved slot indexes directly and keeps common slot/arithmetic
/// shapes on dedicated variants so the per-row grouped fold path avoids
/// planner-expression traversal.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor::aggregate) enum GroupedCompiledExpr {
    Slot {
        slot: usize,
        field: String,
    },
    Literal(Value),
    Add {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Sub {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Mul {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Div {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    BinarySlotLiteral {
        op: BinaryOp,
        slot: usize,
        field: String,
        literal: Value,
        slot_on_left: bool,
    },
    FieldPathUnsupported {
        field: String,
        index: usize,
    },
    FunctionCall {
        function: Function,
        args: Box<[Self]>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    Case {
        when_then_arms: Box<[GroupedCompiledCaseArm]>,
        else_expr: Box<Self>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
}

impl GroupedCompiledExpr {
    /// Compile one planner scalar projection tree into grouped-fold-local form.
    #[must_use]
    pub(in crate::db::executor::aggregate) fn compile(expr: &ScalarProjectionExpr) -> Self {
        match expr {
            ScalarProjectionExpr::Field(field) => Self::Slot {
                slot: field.slot(),
                field: field.field().to_string(),
            },
            ScalarProjectionExpr::FieldPath(path) => Self::FieldPathUnsupported {
                field: render_field_path_label(path.root(), path.segments()),
                index: path.root_slot(),
            },
            ScalarProjectionExpr::Literal(value) => Self::Literal(value.clone()),
            ScalarProjectionExpr::FunctionCall { function, args } => Self::FunctionCall {
                function: *function,
                args: args
                    .iter()
                    .map(Self::compile)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            },
            ScalarProjectionExpr::Unary { op, expr } => Self::Unary {
                op: *op,
                expr: Box::new(Self::compile(expr)),
            },
            ScalarProjectionExpr::Case {
                when_then_arms,
                else_expr,
            } => Self::Case {
                when_then_arms: when_then_arms
                    .iter()
                    .map(GroupedCompiledCaseArm::compile)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                else_expr: Box::new(Self::compile(else_expr)),
            },
            ScalarProjectionExpr::Binary { op, left, right } => {
                let left = Self::compile(left);
                let right = Self::compile(right);

                Self::compile_binary(*op, left, right)
            }
        }
    }

    // Collapse direct slot arithmetic into dedicated variants. These are the
    // grouped aggregate input shapes that previously paid a full expression
    // traversal for every row.
    fn compile_binary(op: BinaryOp, left: Self, right: Self) -> Self {
        if let Some(compiled) = Self::compile_slot_slot_binary(op, &left, &right) {
            return compiled;
        }

        if let Some(compiled) = Self::compile_slot_literal_binary(op, &left, &right) {
            return compiled;
        }

        Self::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    // Keep slot-vs-slot arithmetic as fully direct variants. Other binary
    // operators may still need shared boolean/comparison semantics, so they
    // stay on the generic binary path unless a narrower fast path handles them.
    fn compile_slot_slot_binary(op: BinaryOp, left: &Self, right: &Self) -> Option<Self> {
        let (
            Self::Slot {
                field: left_field,
                slot: left_slot,
            },
            Self::Slot {
                field: right_field,
                slot: right_slot,
            },
        ) = (left, right)
        else {
            return None;
        };

        Some(match op {
            BinaryOp::Add => Self::Add {
                left_slot: *left_slot,
                left_field: left_field.clone(),
                right_slot: *right_slot,
                right_field: right_field.clone(),
            },
            BinaryOp::Sub => Self::Sub {
                left_slot: *left_slot,
                left_field: left_field.clone(),
                right_slot: *right_slot,
                right_field: right_field.clone(),
            },
            BinaryOp::Mul => Self::Mul {
                left_slot: *left_slot,
                left_field: left_field.clone(),
                right_slot: *right_slot,
                right_field: right_field.clone(),
            },
            BinaryOp::Div => Self::Div {
                left_slot: *left_slot,
                left_field: left_field.clone(),
                right_slot: *right_slot,
                right_field: right_field.clone(),
            },
            BinaryOp::Or
            | BinaryOp::And
            | BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Lt
            | BinaryOp::Lte
            | BinaryOp::Gt
            | BinaryOp::Gte => return None,
        })
    }

    // Slot-literal comparisons are common in grouped CASE filters such as
    // `CASE WHEN age >= 30 THEN ...`. This variant removes recursive literal
    // evaluation and preserves operand order for asymmetric operators.
    fn compile_slot_literal_binary(op: BinaryOp, left: &Self, right: &Self) -> Option<Self> {
        match (left, right) {
            (Self::Slot { field, slot }, Self::Literal(literal)) => Some(Self::BinarySlotLiteral {
                op,
                slot: *slot,
                field: field.clone(),
                literal: literal.clone(),
                slot_on_left: true,
            }),
            (Self::Literal(literal), Self::Slot { field, slot }) => Some(Self::BinarySlotLiteral {
                op,
                slot: *slot,
                field: field.clone(),
                literal: literal.clone(),
                slot_on_left: false,
            }),
            _ => None,
        }
    }

    /// Evaluate one grouped compiled expression against a decoded row view.
    pub(in crate::db::executor::aggregate::contracts::state) fn evaluate<'row>(
        &'row self,
        row_view: &'row RowView,
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        match self {
            Self::Slot { slot, field } => Self::evaluate_slot(row_view, *slot, field),
            Self::Literal(value) => Ok(Cow::Borrowed(value)),
            Self::Add {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary(
                row_view,
                BinaryOp::Add,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Sub {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary(
                row_view,
                BinaryOp::Sub,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Mul {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary(
                row_view,
                BinaryOp::Mul,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Div {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary(
                row_view,
                BinaryOp::Div,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::BinarySlotLiteral {
                op,
                slot,
                field,
                literal,
                slot_on_left,
            } => Self::evaluate_slot_literal_binary(
                row_view,
                *op,
                *slot,
                field,
                literal,
                *slot_on_left,
            ),
            Self::FieldPathUnsupported { field, index } => Err(missing_field_value(field, *index)),
            Self::FunctionCall { function, args } => {
                Self::evaluate_function_call(row_view, *function, args)
            }
            Self::Unary { op, expr } => {
                let value = expr.evaluate(row_view)?;

                eval_unary_expr(*op, value.as_ref()).map(Cow::Owned)
            }
            Self::Case {
                when_then_arms,
                else_expr,
            } => Self::evaluate_case(row_view, when_then_arms, else_expr),
            Self::Binary { op, left, right } => {
                let left = left.evaluate(row_view)?;
                let right = right.evaluate(row_view)?;

                eval_binary_expr(*op, left.as_ref(), right.as_ref()).map(Cow::Owned)
            }
        }
    }

    // Resolve one required slot through row-view storage without constructing
    // a caller closure or walking a planner expression node.
    fn evaluate_slot<'row>(
        row_view: &'row RowView,
        slot: usize,
        field: &str,
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        row_view
            .slot_value(slot)
            .ok_or_else(|| missing_field_value(field, slot))
    }

    // Evaluate one dedicated direct-slot arithmetic variant. NULL propagation
    // and checked numeric behavior stay delegated to the shared binary
    // operator implementation so semantics remain identical.
    fn evaluate_slot_binary<'row>(
        row_view: &'row RowView,
        op: BinaryOp,
        left: (usize, &str),
        right: (usize, &str),
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        let (left_slot, left_field) = left;
        let (right_slot, right_field) = right;
        let left = row_view
            .slot_value(left_slot)
            .ok_or_else(|| missing_field_value(left_field, left_slot))?;
        let right = row_view
            .slot_value(right_slot)
            .ok_or_else(|| missing_field_value(right_field, right_slot))?;

        eval_binary_expr(op, left.as_ref(), right.as_ref()).map(Cow::Owned)
    }

    // Evaluate one slot-literal binary variant without recursively visiting
    // either operand node. Operand order remains explicit because comparisons
    // and division are not commutative.
    fn evaluate_slot_literal_binary<'row>(
        row_view: &'row RowView,
        op: BinaryOp,
        slot: usize,
        field: &str,
        literal: &Value,
        slot_on_left: bool,
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        let value = row_view
            .slot_value(slot)
            .ok_or_else(|| missing_field_value(field, slot))?;
        let result = if slot_on_left {
            eval_binary_expr(op, value.as_ref(), literal)
        } else {
            eval_binary_expr(op, literal, value.as_ref())
        }?;

        Ok(Cow::Owned(result))
    }

    // Evaluate searched CASE through compiled condition/result programs.
    // Only TRUE selects an arm; FALSE and NULL fall through through the
    // shared boolean admission helper.
    fn evaluate_case<'row>(
        row_view: &'row RowView,
        when_then_arms: &'row [GroupedCompiledCaseArm],
        else_expr: &'row Self,
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        for arm in when_then_arms {
            let condition = arm.condition.evaluate(row_view)?;
            if admit_true_only_boolean_value(condition.as_ref(), |found| {
                ProjectionEvalError::InvalidCaseCondition {
                    found: Box::new(found.clone()),
                }
            })? {
                return arm.result.evaluate(row_view);
            }
        }

        else_expr.evaluate(row_view)
    }

    // Evaluate scalar function calls without heap allocation for common arities.
    // Larger dynamic functions still allocate their argument vector, matching
    // the existing semantics while keeping common grouped aggregate expressions
    // allocation-free.
    fn evaluate_function_call<'row>(
        row_view: &'row RowView,
        function: Function,
        args: &'row [Self],
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        let value = match args {
            [] => eval_grouped_function_call(function, &[])?,
            [arg] => {
                let arg = arg.evaluate(row_view)?.into_owned();
                let args = [arg];

                eval_grouped_function_call(function, &args)?
            }
            [left, right] => {
                let left = left.evaluate(row_view)?.into_owned();
                let right = right.evaluate(row_view)?.into_owned();
                let args = [left, right];

                eval_grouped_function_call(function, &args)?
            }
            [first, second, third] => {
                let first = first.evaluate(row_view)?.into_owned();
                let second = second.evaluate(row_view)?.into_owned();
                let third = third.evaluate(row_view)?.into_owned();
                let args = [first, second, third];

                eval_grouped_function_call(function, &args)?
            }
            args => {
                let mut evaluated_args = Vec::with_capacity(args.len());
                for arg in args {
                    evaluated_args.push(arg.evaluate(row_view)?.into_owned());
                }

                eval_grouped_function_call(function, evaluated_args.as_slice())?
            }
        };

        Ok(Cow::Owned(value))
    }
}

///
/// GroupedCompiledCaseArm
///
/// GroupedCompiledCaseArm stores one searched-CASE condition/result pair after
/// both expressions have been compiled into grouped-fold-local slot programs.
/// It exists so CASE evaluation no longer needs to borrow planner-owned
/// `ScalarProjectionCaseArm` nodes inside the grouped fold hot path.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor::aggregate) struct GroupedCompiledCaseArm {
    condition: GroupedCompiledExpr,
    result: GroupedCompiledExpr,
}

impl GroupedCompiledCaseArm {
    // Compile one searched-CASE arm from the planner scalar projection form.
    fn compile(arm: &ScalarProjectionCaseArm) -> Self {
        Self {
            condition: GroupedCompiledExpr::compile(arm.condition()),
            result: GroupedCompiledExpr::compile(arm.result()),
        }
    }
}

fn eval_grouped_function_call(
    function: Function,
    args: &[Value],
) -> Result<Value, ProjectionEvalError> {
    eval_projection_function_call_checked(function, args).map_err(|err| match err {
        ProjectionFunctionEvalError::Numeric(err) => ProjectionEvalError::Numeric(err),
        ProjectionFunctionEvalError::Query(err) => ProjectionEvalError::InvalidFunctionCall {
            function: function.projection_eval_name().to_string(),
            message: err.to_string(),
        },
    })
}

fn missing_field_value(field: &str, index: usize) -> ProjectionEvalError {
    ProjectionEvalError::MissingFieldValue {
        field: field.to_string(),
        index,
    }
}

fn render_field_path_label(root: &str, segments: &[String]) -> String {
    let mut label = root.to_string();
    for segment in segments {
        label.push('.');
        label.push_str(segment);
    }

    label
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            executor::{
                aggregate::contracts::state::GroupedCompiledExpr, pipeline::runtime::RowView,
            },
            query::plan::expr::{BinaryOp, Function},
        },
        value::Value,
    };
    use std::cmp::Ordering;

    fn row_view() -> RowView {
        RowView::new(vec![
            Some(Value::Uint(7)),
            Some(Value::Int(3)),
            Some(Value::Null),
            Some(Value::Text("MiXeD".to_string())),
            Some(Value::Bool(true)),
        ])
    }

    fn evaluate(expr: &GroupedCompiledExpr) -> Value {
        expr.evaluate(&row_view())
            .expect("grouped compiled expression should evaluate")
            .into_owned()
    }

    #[test]
    fn grouped_compiled_expr_reads_slots_without_cloning_contract_drift() {
        let expr = GroupedCompiledExpr::Slot {
            slot: 0,
            field: "age".to_string(),
        };

        assert_eq!(evaluate(&expr), Value::Uint(7));
    }

    #[test]
    fn grouped_compiled_expr_preserves_slot_arithmetic_semantics() {
        let expr = GroupedCompiledExpr::Add {
            left_slot: 0,
            left_field: "age".to_string(),
            right_slot: 1,
            right_field: "rank".to_string(),
        };
        let value = evaluate(&expr);

        assert_eq!(
            value.cmp_numeric(&Value::Int(10)),
            Some(Ordering::Equal),
            "direct slot arithmetic must preserve shared numeric coercion semantics",
        );
    }

    #[test]
    fn grouped_compiled_expr_case_only_true_selects_branch() {
        let expr = GroupedCompiledExpr::Case {
            when_then_arms: vec![
                super::GroupedCompiledCaseArm {
                    condition: GroupedCompiledExpr::Literal(Value::Null),
                    result: GroupedCompiledExpr::Literal(Value::Text("null".to_string())),
                },
                super::GroupedCompiledCaseArm {
                    condition: GroupedCompiledExpr::BinarySlotLiteral {
                        op: BinaryOp::Gt,
                        slot: 0,
                        field: "age".to_string(),
                        literal: Value::Uint(5),
                        slot_on_left: true,
                    },
                    result: GroupedCompiledExpr::Literal(Value::Text("selected".to_string())),
                },
            ]
            .into_boxed_slice(),
            else_expr: Box::new(GroupedCompiledExpr::Literal(Value::Text(
                "else".to_string(),
            ))),
        };

        assert_eq!(evaluate(&expr), Value::Text("selected".to_string()));
    }

    #[test]
    fn grouped_compiled_expr_case_false_and_null_fall_through() {
        let expr = GroupedCompiledExpr::Case {
            when_then_arms: vec![
                super::GroupedCompiledCaseArm {
                    condition: GroupedCompiledExpr::Literal(Value::Null),
                    result: GroupedCompiledExpr::Literal(Value::Text("null".to_string())),
                },
                super::GroupedCompiledCaseArm {
                    condition: GroupedCompiledExpr::Literal(Value::Bool(false)),
                    result: GroupedCompiledExpr::Literal(Value::Text("false".to_string())),
                },
            ]
            .into_boxed_slice(),
            else_expr: Box::new(GroupedCompiledExpr::Literal(Value::Text(
                "else".to_string(),
            ))),
        };

        assert_eq!(evaluate(&expr), Value::Text("else".to_string()));
    }

    #[test]
    fn grouped_compiled_expr_function_calls_reuse_projection_semantics() {
        let expr = GroupedCompiledExpr::FunctionCall {
            function: Function::Lower,
            args: vec![GroupedCompiledExpr::Slot {
                slot: 3,
                field: "name".to_string(),
            }]
            .into_boxed_slice(),
        };

        assert_eq!(evaluate(&expr), Value::Text("mixed".to_string()));
    }

    #[test]
    fn grouped_compiled_expr_missing_slot_keeps_field_diagnostic() {
        let expr = GroupedCompiledExpr::Slot {
            slot: 99,
            field: "missing_field".to_string(),
        };
        let err = expr
            .evaluate(&row_view())
            .expect_err("missing grouped slot should stay a projection error");

        assert_eq!(
            err.to_string(),
            "projection expression could not read field 'missing_field' at index=99",
        );
    }
}
