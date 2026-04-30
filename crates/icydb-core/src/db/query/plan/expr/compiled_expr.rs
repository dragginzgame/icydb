//! Module: query::plan::expr::compiled_expr
//! Responsibility: compiled expression programs and expression evaluation.
//! Does not own: executor row loops, grouped aggregate reducer mechanics, or
//! scan/projection orchestration.
//! Boundary: expression-layer programs evaluate already-loaded slot values so
//! executor code can stay on row loading, reducer updates, and LIMIT handling.

use crate::{
    db::{
        executor::projection::ProjectionEvalError,
        numeric::NumericEvalError,
        query::{
            builder::AggregateExpr,
            plan::{
                FieldSlot, GroupedAggregateExecutionSpec,
                expr::{
                    BinaryOp, Expr, FieldPath, Function, ProjectionFunctionEvalError,
                    ProjectionSpec, ScalarProjectionCaseArm, ScalarProjectionExpr, UnaryOp,
                    admit_true_only_boolean_value, collapse_true_only_boolean_admission,
                    eval_projection_function_call_checked,
                },
            },
        },
    },
    value::{
        Value,
        ops::{numeric as value_numeric, ordering as value_ordering},
    },
};
use std::borrow::Cow;

///
/// CompiledExpr
///
/// CompiledExpr is the common expression-layer interface for value-producing
/// programs that have already crossed planning and lowering boundaries.
/// Executors provide loaded slot values and consume only the computed value.
///

#[expect(
    dead_code,
    reason = "compiled expression callers are being migrated to the unified interface"
)]
pub(in crate::db) trait CompiledExpr {
    /// Evaluate this compiled expression against already-loaded slot values.
    fn eval(&self, slots: &[Value]) -> Value;
}

///
/// CompiledExprSlotReader
///
/// CompiledExprSlotReader is the expression-layer adapter for row-local slot
/// access.
/// Executors implement it for their row views so compiled evaluators can read
/// values without depending on executor-owned row-view concrete types.
///

pub(in crate::db) trait CompiledExprSlotReader {
    /// Borrow or materialize one loaded slot value for expression evaluation.
    fn compiled_slot_value(&self, slot: usize) -> Option<Cow<'_, Value>>;
}

///
/// GroupedProjectionValueReader
///
/// GroupedProjectionValueReader is the expression-layer adapter for finalized
/// grouped output values.
/// Grouped executors expose key and aggregate slices through this contract so
/// projection and HAVING evaluation do not depend on executor row-view types.
///

pub(in crate::db) trait GroupedProjectionValueReader {
    /// Borrow one grouped key value by compiled grouped-field offset.
    fn grouped_key_value(&self, offset: usize) -> Option<&Value>;

    /// Borrow one finalized aggregate value by compiled aggregate output index.
    fn grouped_aggregate_value(&self, index: usize) -> Option<&Value>;

    /// Return the number of finalized aggregate values visible to this row.
    fn grouped_aggregate_count(&self) -> usize;
}

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
pub(in crate::db) enum GroupedCompiledExpr {
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
    Eq {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Ne {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Lt {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Lte {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Gt {
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    },
    Gte {
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
    CaseSlotLiteral {
        op: BinaryOp,
        slot: usize,
        field: String,
        literal: Value,
        slot_on_left: bool,
        then_expr: Box<Self>,
        else_expr: Box<Self>,
    },
    CaseSlotBool {
        slot: usize,
        field: String,
        then_expr: Box<Self>,
        else_expr: Box<Self>,
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
    pub(in crate::db) fn compile(expr: &ScalarProjectionExpr) -> Self {
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
            } => Self::compile_case(when_then_arms, else_expr),
            ScalarProjectionExpr::Binary { op, left, right } => {
                let left = Self::compile(left);
                let right = Self::compile(right);

                Self::compile_binary(*op, left, right)
            }
        }
    }

    // Collapse one-arm CASE programs into condition-specialized forms when
    // the condition shape can be decided without evaluating a boolean Value.
    // Multi-arm searched CASE keeps the generic arm list to preserve normal
    // short-circuit behavior without adding a broader expression VM.
    fn compile_case(
        when_then_arms: &[ScalarProjectionCaseArm],
        else_expr: &ScalarProjectionExpr,
    ) -> Self {
        let else_expr = Self::compile(else_expr);
        let [arm] = when_then_arms else {
            return Self::Case {
                when_then_arms: when_then_arms
                    .iter()
                    .map(GroupedCompiledCaseArm::compile)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                else_expr: Box::new(else_expr),
            };
        };

        let condition = Self::compile(arm.condition());
        let then_expr = Self::compile(arm.result());

        Self::compile_single_arm_case(condition, then_expr, else_expr)
    }

    // Convert common searched-CASE conditions into direct slot predicates.
    // Constant TRUE/FALSE/NULL conditions are selected once during grouped
    // setup, which removes invariant condition evaluation from the row loop.
    fn compile_single_arm_case(condition: Self, then_expr: Self, else_expr: Self) -> Self {
        match condition {
            Self::Literal(Value::Bool(true)) => then_expr,
            Self::Literal(Value::Bool(false) | Value::Null) => else_expr,
            Self::BinarySlotLiteral {
                op,
                slot,
                field,
                literal,
                slot_on_left,
            } if is_comparison_op(op) => Self::CaseSlotLiteral {
                op,
                slot,
                field,
                literal,
                slot_on_left,
                then_expr: Box::new(then_expr),
                else_expr: Box::new(else_expr),
            },
            Self::Slot { slot, field } => Self::CaseSlotBool {
                slot,
                field,
                then_expr: Box::new(then_expr),
                else_expr: Box::new(else_expr),
            },
            condition => Self::Case {
                when_then_arms: vec![GroupedCompiledCaseArm {
                    condition,
                    result: then_expr,
                }]
                .into_boxed_slice(),
                else_expr: Box::new(else_expr),
            },
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
            BinaryOp::Eq => Self::Eq {
                left_slot: *left_slot,
                left_field: left_field.clone(),
                right_slot: *right_slot,
                right_field: right_field.clone(),
            },
            BinaryOp::Ne => Self::Ne {
                left_slot: *left_slot,
                left_field: left_field.clone(),
                right_slot: *right_slot,
                right_field: right_field.clone(),
            },
            BinaryOp::Lt => Self::Lt {
                left_slot: *left_slot,
                left_field: left_field.clone(),
                right_slot: *right_slot,
                right_field: right_field.clone(),
            },
            BinaryOp::Lte => Self::Lte {
                left_slot: *left_slot,
                left_field: left_field.clone(),
                right_slot: *right_slot,
                right_field: right_field.clone(),
            },
            BinaryOp::Gt => Self::Gt {
                left_slot: *left_slot,
                left_field: left_field.clone(),
                right_slot: *right_slot,
                right_field: right_field.clone(),
            },
            BinaryOp::Gte => Self::Gte {
                left_slot: *left_slot,
                left_field: left_field.clone(),
                right_slot: *right_slot,
                right_field: right_field.clone(),
            },
            BinaryOp::Or | BinaryOp::And => return None,
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
    #[expect(
        clippy::too_many_lines,
        reason = "explicit compiled opcode dispatch keeps grouped hot-loop behavior auditably direct"
    )]
    pub(in crate::db) fn evaluate<'row>(
        &'row self,
        row_view: &'row dyn CompiledExprSlotReader,
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        match self {
            Self::Slot { slot, field } => Self::evaluate_slot(row_view, *slot, field),
            Self::Literal(value) => Ok(Cow::Borrowed(value)),
            Self::Add {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_arithmetic(
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
            } => Self::evaluate_slot_binary_arithmetic(
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
            } => Self::evaluate_slot_binary_arithmetic(
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
            } => Self::evaluate_slot_binary_arithmetic(
                row_view,
                BinaryOp::Div,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Eq {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_comparison(
                row_view,
                BinaryOp::Eq,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Ne {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_comparison(
                row_view,
                BinaryOp::Ne,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Lt {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_comparison(
                row_view,
                BinaryOp::Lt,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Lte {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_comparison(
                row_view,
                BinaryOp::Lte,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Gt {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_comparison(
                row_view,
                BinaryOp::Gt,
                (*left_slot, left_field),
                (*right_slot, right_field),
            ),
            Self::Gte {
                left_slot,
                left_field,
                right_slot,
                right_field,
            } => Self::evaluate_slot_binary_comparison(
                row_view,
                BinaryOp::Gte,
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
            Self::CaseSlotLiteral {
                op,
                slot,
                field,
                literal,
                slot_on_left,
                then_expr,
                else_expr,
            } => Self::evaluate_case_slot_literal(
                row_view,
                *op,
                (*slot, field),
                literal,
                *slot_on_left,
                then_expr,
                else_expr,
            ),
            Self::CaseSlotBool {
                slot,
                field,
                then_expr,
                else_expr,
            } => Self::evaluate_case_slot_bool(row_view, *slot, field, then_expr, else_expr),
            Self::FieldPathUnsupported { field, index } => Err(missing_field_value(field, *index)),
            Self::FunctionCall { function, args } => {
                Self::evaluate_function_call(row_view, *function, args)
            }
            Self::Unary { op, expr } => {
                let value = expr.evaluate(row_view)?;

                evaluate_unary_expr(*op, value.as_ref()).map(Cow::Owned)
            }
            Self::Case {
                when_then_arms,
                else_expr,
            } => Self::evaluate_case(row_view, when_then_arms, else_expr),
            Self::Binary { op, left, right } => {
                let left = left.evaluate(row_view)?;
                let right = right.evaluate(row_view)?;

                evaluate_binary_expr(*op, left.as_ref(), right.as_ref()).map(Cow::Owned)
            }
        }
    }

    // Resolve one required slot through row-view storage without constructing
    // a caller closure or walking a planner expression node.
    fn evaluate_slot<'row>(
        row_view: &'row dyn CompiledExprSlotReader,
        slot: usize,
        field: &str,
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        row_view
            .compiled_slot_value(slot)
            .ok_or_else(|| missing_field_value(field, slot))
    }

    // Evaluate one dedicated direct-slot arithmetic variant. NULL propagation
    // and checked numeric behavior stay delegated to `value::ops::numeric`
    // instead of the generic projection expression evaluator.
    fn evaluate_slot_binary_arithmetic<'row>(
        row_view: &'row dyn CompiledExprSlotReader,
        op: BinaryOp,
        left: (usize, &str),
        right: (usize, &str),
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        let (left_slot, left_field) = left;
        let (right_slot, right_field) = right;
        let left = row_view
            .compiled_slot_value(left_slot)
            .ok_or_else(|| missing_field_value(left_field, left_slot))?;
        let right = row_view
            .compiled_slot_value(right_slot)
            .ok_or_else(|| missing_field_value(right_field, right_slot))?;

        evaluate_numeric_binary_expr(op, left.as_ref(), right.as_ref()).map(Cow::Owned)
    }

    // Evaluate one dedicated direct-slot comparison variant using the
    // value-local ordering helpers. This keeps grouped CASE/FILTER predicates
    // away from generic binary expression dispatch for slot-vs-slot shapes.
    fn evaluate_slot_binary_comparison<'row>(
        row_view: &'row dyn CompiledExprSlotReader,
        op: BinaryOp,
        left: (usize, &str),
        right: (usize, &str),
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        let (left_slot, left_field) = left;
        let (right_slot, right_field) = right;
        let left = row_view
            .compiled_slot_value(left_slot)
            .ok_or_else(|| missing_field_value(left_field, left_slot))?;
        let right = row_view
            .compiled_slot_value(right_slot)
            .ok_or_else(|| missing_field_value(right_field, right_slot))?;

        evaluate_compare_binary_expr(op, left.as_ref(), right.as_ref()).map(Cow::Owned)
    }

    // Evaluate one slot-literal binary variant without recursively visiting
    // either operand node. Operand order remains explicit because comparisons
    // and division are not commutative.
    fn evaluate_slot_literal_binary<'row>(
        row_view: &'row dyn CompiledExprSlotReader,
        op: BinaryOp,
        slot: usize,
        field: &str,
        literal: &Value,
        slot_on_left: bool,
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        let value = row_view
            .compiled_slot_value(slot)
            .ok_or_else(|| missing_field_value(field, slot))?;
        let result = if slot_on_left {
            evaluate_binary_expr(op, value.as_ref(), literal)
        } else {
            evaluate_binary_expr(op, literal, value.as_ref())
        }?;

        Ok(Cow::Owned(result))
    }

    // Evaluate a one-arm CASE with a direct slot/literal comparison. This
    // avoids producing an intermediate `Value::Bool`; NULL comparison results
    // still fall through exactly like the generic CASE admission helper.
    fn evaluate_case_slot_literal<'row>(
        row_view: &'row dyn CompiledExprSlotReader,
        op: BinaryOp,
        slot_ref: (usize, &str),
        literal: &Value,
        slot_on_left: bool,
        then_expr: &'row Self,
        else_expr: &'row Self,
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        if Self::evaluate_slot_literal_condition(row_view, op, slot_ref, literal, slot_on_left)? {
            return then_expr.evaluate(row_view);
        }

        else_expr.evaluate(row_view)
    }

    // Evaluate a one-arm CASE whose condition is a boolean slot. NULL follows
    // SQL searched-CASE behavior and does not select the branch; non-boolean
    // values retain the existing CASE-condition diagnostic.
    fn evaluate_case_slot_bool<'row>(
        row_view: &'row dyn CompiledExprSlotReader,
        slot: usize,
        field: &str,
        then_expr: &'row Self,
        else_expr: &'row Self,
    ) -> Result<Cow<'row, Value>, ProjectionEvalError> {
        let condition = row_view
            .compiled_slot_value(slot)
            .ok_or_else(|| missing_field_value(field, slot))?;
        let select_then = match condition.as_ref() {
            Value::Bool(value) => *value,
            Value::Null => false,
            found => {
                return Err(ProjectionEvalError::InvalidCaseCondition {
                    found: Box::new(found.clone()),
                });
            }
        };

        if select_then {
            return then_expr.evaluate(row_view);
        }

        else_expr.evaluate(row_view)
    }

    // Compare one slot against one literal as a searched-CASE predicate. The
    // helper mirrors comparison expression NULL and invalid-operand semantics,
    // but returns the branch decision directly instead of wrapping it in Value.
    fn evaluate_slot_literal_condition(
        row_view: &dyn CompiledExprSlotReader,
        op: BinaryOp,
        slot_ref: (usize, &str),
        literal: &Value,
        slot_on_left: bool,
    ) -> Result<bool, ProjectionEvalError> {
        let (slot, field) = slot_ref;
        let slot_value = row_view
            .compiled_slot_value(slot)
            .ok_or_else(|| missing_field_value(field, slot))?;
        let (left, right) = if slot_on_left {
            (slot_value.as_ref(), literal)
        } else {
            (literal, slot_value.as_ref())
        };

        evaluate_compare_binary_condition(op, left, right)
    }

    // Evaluate searched CASE through compiled condition/result programs.
    // Only TRUE selects an arm; FALSE and NULL fall through through the
    // shared boolean admission helper.
    fn evaluate_case<'row>(
        row_view: &'row dyn CompiledExprSlotReader,
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
        row_view: &'row dyn CompiledExprSlotReader,
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
pub(in crate::db) struct GroupedCompiledCaseArm {
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

impl CompiledExpr for GroupedCompiledExpr {
    fn eval(&self, slots: &[Value]) -> Value {
        self.evaluate(&SliceCompiledExprSlotReader { slots })
            .expect("compiled expression eval requires valid loaded slots and operands")
            .into_owned()
    }
}

///
/// SliceCompiledExprSlotReader
///
/// SliceCompiledExprSlotReader adapts the trait-required `&[Value]` interface
/// to the checked expression evaluator.
/// Production executors use row-view adapters so missing-slot diagnostics stay
/// contextual; this adapter exists to make the common compiled interface real.
///

struct SliceCompiledExprSlotReader<'a> {
    slots: &'a [Value],
}

impl CompiledExprSlotReader for SliceCompiledExprSlotReader<'_> {
    fn compiled_slot_value(&self, slot: usize) -> Option<Cow<'_, Value>> {
        self.slots.get(slot).map(Cow::Borrowed)
    }
}

///
/// GroupedProjectionExpr
///
/// GroupedProjectionExpr is the compiled grouped-output projection tree used
/// by grouped finalization and grouped-row materialization.
/// Group-field offsets and aggregate indexes are resolved once so grouped
/// output loops only call the compiled evaluator at execution time.
///

#[derive(Clone, Debug)]
pub(in crate::db) enum GroupedProjectionExpr {
    Field(GroupedProjectionField),
    Aggregate(GroupedProjectionAggregate),
    Literal(Value),
    FunctionCall {
        function: Function,
        args: Vec<Self>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    Case {
        when_then_arms: Vec<GroupedProjectionCaseArm>,
        else_expr: Box<Self>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
}

///
/// GroupedProjectionCaseArm
///
/// GroupedProjectionCaseArm stores one compiled grouped searched-CASE
/// condition/result pair.
/// It exists so grouped projection and HAVING execution can remain lazy while
/// keeping CASE expression branching inside the expression layer.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct GroupedProjectionCaseArm {
    condition: GroupedProjectionExpr,
    result: GroupedProjectionExpr,
}

impl GroupedProjectionCaseArm {
    /// Build one compiled grouped CASE arm.
    #[must_use]
    pub(in crate::db) const fn new(
        condition: GroupedProjectionExpr,
        result: GroupedProjectionExpr,
    ) -> Self {
        Self { condition, result }
    }

    /// Borrow the compiled grouped condition expression.
    #[must_use]
    pub(in crate::db) const fn condition(&self) -> &GroupedProjectionExpr {
        &self.condition
    }

    /// Borrow the compiled grouped result expression.
    #[must_use]
    pub(in crate::db) const fn result(&self) -> &GroupedProjectionExpr {
        &self.result
    }
}

///
/// GroupedProjectionField
///
/// GroupedProjectionField is one resolved grouped-field leaf inside a compiled
/// grouped projection expression.
/// It preserves field-name diagnostics while turning grouped field access into
/// one direct grouped-key lookup through `GroupedProjectionValueReader`.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct GroupedProjectionField {
    field: String,
    offset: usize,
}

///
/// GroupedProjectionAggregate
///
/// GroupedProjectionAggregate is one resolved grouped aggregate leaf inside a
/// compiled grouped projection expression.
/// It preserves aggregate-index diagnostics while turning grouped aggregate
/// access into one direct aggregate-value lookup through
/// `GroupedProjectionValueReader`.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct GroupedProjectionAggregate {
    index: usize,
}

/// Compile one grouped projection spec into direct grouped field/aggregate lookups.
pub(in crate::db) fn compile_grouped_projection_plan(
    projection: &ProjectionSpec,
    group_fields: &[FieldSlot],
    aggregate_execution_specs: &[GroupedAggregateExecutionSpec],
) -> Result<Vec<GroupedProjectionExpr>, ProjectionEvalError> {
    let mut compiled_fields = Vec::with_capacity(projection.len());

    for field in projection.fields() {
        compiled_fields.push(compile_grouped_projection_expr(
            field.expr(),
            group_fields,
            aggregate_execution_specs,
        )?);
    }

    Ok(compiled_fields)
}

pub(in crate::db) fn compile_grouped_projection_expr(
    expr: &Expr,
    group_fields: &[FieldSlot],
    aggregate_execution_specs: &[GroupedAggregateExecutionSpec],
) -> Result<GroupedProjectionExpr, ProjectionEvalError> {
    match expr {
        Expr::Field(field_id) => {
            let field_name = field_id.as_str();
            let Some(offset) = resolve_group_field_offset(group_fields, field_name) else {
                return Err(ProjectionEvalError::UnknownField {
                    field: field_name.to_string(),
                });
            };

            Ok(GroupedProjectionExpr::Field(GroupedProjectionField {
                field: field_name.to_string(),
                offset,
            }))
        }
        Expr::FieldPath(path) => Err(ProjectionEvalError::UnknownField {
            field: render_grouped_projection_field_path_label(path),
        }),
        Expr::Aggregate(aggregate_expr) => {
            let Some(index) =
                resolve_grouped_aggregate_index(aggregate_execution_specs, aggregate_expr)
            else {
                return Err(ProjectionEvalError::UnknownGroupedAggregateExpression {
                    kind: format!("{:?}", aggregate_expr.kind()),
                    target_field: aggregate_expr.target_field().map(str::to_string),
                    distinct: aggregate_expr.is_distinct(),
                });
            };

            Ok(GroupedProjectionExpr::Aggregate(
                GroupedProjectionAggregate { index },
            ))
        }
        Expr::Literal(value) => Ok(GroupedProjectionExpr::Literal(value.clone())),
        Expr::FunctionCall { function, args } => Ok(GroupedProjectionExpr::FunctionCall {
            function: *function,
            args: args
                .iter()
                .map(|arg| {
                    compile_grouped_projection_expr(arg, group_fields, aggregate_execution_specs)
                })
                .collect::<Result<Vec<_>, _>>()?,
        }),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Ok(GroupedProjectionExpr::Case {
            when_then_arms: when_then_arms
                .iter()
                .map(|arm| {
                    Ok::<GroupedProjectionCaseArm, ProjectionEvalError>(
                        GroupedProjectionCaseArm::new(
                            compile_grouped_projection_expr(
                                arm.condition(),
                                group_fields,
                                aggregate_execution_specs,
                            )?,
                            compile_grouped_projection_expr(
                                arm.result(),
                                group_fields,
                                aggregate_execution_specs,
                            )?,
                        ),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            else_expr: Box::new(compile_grouped_projection_expr(
                else_expr.as_ref(),
                group_fields,
                aggregate_execution_specs,
            )?),
        }),
        Expr::Unary { op, expr } => Ok(GroupedProjectionExpr::Unary {
            op: *op,
            expr: Box::new(compile_grouped_projection_expr(
                expr.as_ref(),
                group_fields,
                aggregate_execution_specs,
            )?),
        }),
        Expr::Binary { op, left, right } => Ok(GroupedProjectionExpr::Binary {
            op: *op,
            left: Box::new(compile_grouped_projection_expr(
                left.as_ref(),
                group_fields,
                aggregate_execution_specs,
            )?),
            right: Box::new(compile_grouped_projection_expr(
                right.as_ref(),
                group_fields,
                aggregate_execution_specs,
            )?),
        }),
        #[cfg(test)]
        Expr::Alias { expr, .. } => {
            compile_grouped_projection_expr(expr.as_ref(), group_fields, aggregate_execution_specs)
        }
    }
}

/// Evaluate one compiled grouped HAVING expression against one grouped output row.
pub(in crate::db) fn evaluate_grouped_having_expr(
    expr: &GroupedProjectionExpr,
    grouped_row: &dyn GroupedProjectionValueReader,
) -> Result<bool, ProjectionEvalError> {
    collapse_true_only_boolean_admission(
        eval_grouped_projection_expr(expr, grouped_row)?,
        |found| ProjectionEvalError::InvalidGroupedHavingResult { found },
    )
}

pub(in crate::db) fn eval_grouped_projection_expr(
    expr: &GroupedProjectionExpr,
    grouped_row: &dyn GroupedProjectionValueReader,
) -> Result<Value, ProjectionEvalError> {
    match expr {
        GroupedProjectionExpr::Field(field) => {
            let Some(value) = grouped_row.grouped_key_value(field.offset) else {
                return Err(ProjectionEvalError::MissingFieldValue {
                    field: field.field.clone(),
                    index: field.offset,
                });
            };

            Ok(value.clone())
        }
        GroupedProjectionExpr::Aggregate(aggregate) => {
            let Some(value) = grouped_row.grouped_aggregate_value(aggregate.index) else {
                return Err(ProjectionEvalError::MissingGroupedAggregateValue {
                    aggregate_index: aggregate.index,
                    aggregate_count: grouped_row.grouped_aggregate_count(),
                });
            };

            Ok(value.clone())
        }
        GroupedProjectionExpr::Literal(value) => Ok(value.clone()),
        GroupedProjectionExpr::FunctionCall { function, args } => {
            let evaluated_args = args
                .iter()
                .map(|arg| eval_grouped_projection_expr(arg, grouped_row))
                .collect::<Result<Vec<_>, _>>()?;

            eval_grouped_function_call(*function, evaluated_args.as_slice())
        }
        GroupedProjectionExpr::Unary { op, expr } => {
            let operand = eval_grouped_projection_expr(expr, grouped_row)?;
            evaluate_unary_expr(*op, &operand)
        }
        GroupedProjectionExpr::Case {
            when_then_arms,
            else_expr,
        } => {
            for arm in when_then_arms {
                let condition = eval_grouped_projection_expr(arm.condition(), grouped_row)?;
                if collapse_true_only_boolean_admission(condition, |found| {
                    ProjectionEvalError::InvalidCaseCondition { found }
                })? {
                    return eval_grouped_projection_expr(arm.result(), grouped_row);
                }
            }

            eval_grouped_projection_expr(else_expr.as_ref(), grouped_row)
        }
        GroupedProjectionExpr::Binary { op, left, right } => {
            let left = eval_grouped_projection_expr(left, grouped_row)?;
            let right = eval_grouped_projection_expr(right, grouped_row)?;

            evaluate_binary_expr(*op, &left, &right)
        }
    }
}

fn resolve_group_field_offset(group_fields: &[FieldSlot], field_name: &str) -> Option<usize> {
    for (offset, group_field) in group_fields.iter().enumerate() {
        if group_field.field() == field_name {
            return Some(offset);
        }
    }

    None
}

fn resolve_grouped_aggregate_index(
    aggregate_execution_specs: &[GroupedAggregateExecutionSpec],
    aggregate_expr: &AggregateExpr,
) -> Option<usize> {
    for (index, candidate) in aggregate_execution_specs.iter().enumerate() {
        if candidate.matches_aggregate_expr(aggregate_expr) {
            return Some(index);
        }
    }

    None
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

fn evaluate_unary_expr(op: UnaryOp, value: &Value) -> Result<Value, ProjectionEvalError> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }

    match op {
        UnaryOp::Not => {
            let Value::Bool(value) = value else {
                return Err(ProjectionEvalError::InvalidUnaryOperand {
                    op: unary_op_name(op).to_string(),
                    found: Box::new(value.clone()),
                });
            };

            Ok(Value::Bool(!*value))
        }
    }
}

fn evaluate_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionEvalError> {
    match op {
        BinaryOp::Or | BinaryOp::And => evaluate_boolean_binary_expr(op, left, right),
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => evaluate_compare_binary_expr(op, left, right),
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
            evaluate_numeric_binary_expr(op, left, right)
        }
    }
}

fn evaluate_boolean_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionEvalError> {
    match op {
        BinaryOp::And => evaluate_boolean_and(left, right),
        BinaryOp::Or => evaluate_boolean_or(left, right),
        _ => Err(invalid_binary_operands(op, left, right)),
    }
}

fn evaluate_boolean_and(left: &Value, right: &Value) -> Result<Value, ProjectionEvalError> {
    match (left, right) {
        (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Bool(true) | Value::Null, Value::Null) | (Value::Null, Value::Bool(true)) => {
            Ok(Value::Null)
        }
        _ => Err(invalid_binary_operands(BinaryOp::And, left, right)),
    }
}

fn evaluate_boolean_or(left: &Value, right: &Value) -> Result<Value, ProjectionEvalError> {
    match (left, right) {
        (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Bool(false) | Value::Null, Value::Null) | (Value::Null, Value::Bool(false)) => {
            Ok(Value::Null)
        }
        _ => Err(invalid_binary_operands(BinaryOp::Or, left, right)),
    }
}

fn evaluate_numeric_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionEvalError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }

    let result = match op {
        BinaryOp::Add => value_numeric::add(left, right),
        BinaryOp::Sub => value_numeric::sub(left, right),
        BinaryOp::Mul => value_numeric::mul(left, right),
        BinaryOp::Div => value_numeric::div(left, right),
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => return Err(invalid_binary_operands(op, left, right)),
    }
    .map_err(map_numeric_arithmetic_error)?;
    let Some(result) = result else {
        return Err(invalid_binary_operands(op, left, right));
    };

    Ok(Value::Decimal(result))
}

fn evaluate_compare_binary_expr(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Value, ProjectionEvalError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }

    let result = match op {
        BinaryOp::Eq => value_ordering::eq(left, right),
        BinaryOp::Ne => value_ordering::ne(left, right),
        BinaryOp::Lt => value_ordering::lt(left, right),
        BinaryOp::Lte => value_ordering::lte(left, right),
        BinaryOp::Gt => value_ordering::gt(left, right),
        BinaryOp::Gte => value_ordering::gte(left, right),
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => return Err(invalid_binary_operands(op, left, right)),
    };
    let Some(result) = result else {
        return Err(invalid_binary_operands(op, left, right));
    };

    Ok(Value::Bool(result))
}

fn evaluate_compare_binary_condition(
    op: BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<bool, ProjectionEvalError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(false);
    }

    let result = match op {
        BinaryOp::Eq => value_ordering::eq(left, right),
        BinaryOp::Ne => value_ordering::ne(left, right),
        BinaryOp::Lt => value_ordering::lt(left, right),
        BinaryOp::Lte => value_ordering::lte(left, right),
        BinaryOp::Gt => value_ordering::gt(left, right),
        BinaryOp::Gte => value_ordering::gte(left, right),
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => return Err(invalid_binary_operands(op, left, right)),
    };
    let Some(result) = result else {
        return Err(invalid_binary_operands(op, left, right));
    };

    Ok(result)
}

fn map_numeric_arithmetic_error(err: value_numeric::NumericArithmeticError) -> ProjectionEvalError {
    match err {
        value_numeric::NumericArithmeticError::Overflow => NumericEvalError::Overflow,
        value_numeric::NumericArithmeticError::NotRepresentable => {
            NumericEvalError::NotRepresentable
        }
    }
    .into()
}

fn invalid_binary_operands(op: BinaryOp, left: &Value, right: &Value) -> ProjectionEvalError {
    ProjectionEvalError::InvalidBinaryOperands {
        op: binary_op_name(op).to_string(),
        left: Box::new(left.clone()),
        right: Box::new(right.clone()),
    }
}

const fn unary_op_name(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Not => "not",
    }
}

const fn binary_op_name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Or => "or",
        BinaryOp::And => "and",
        BinaryOp::Eq => "eq",
        BinaryOp::Ne => "ne",
        BinaryOp::Lt => "lt",
        BinaryOp::Lte => "lte",
        BinaryOp::Gt => "gt",
        BinaryOp::Gte => "gte",
        BinaryOp::Add => "add",
        BinaryOp::Sub => "sub",
        BinaryOp::Mul => "mul",
        BinaryOp::Div => "div",
    }
}

const fn is_comparison_op(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Gt | BinaryOp::Gte
    )
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

fn render_grouped_projection_field_path_label(path: &FieldPath) -> String {
    let mut label = path.root().as_str().to_string();
    for segment in path.segments() {
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
        db::query::plan::expr::{
            BinaryOp, CompiledExprSlotReader, Function, GroupedCompiledExpr,
            ScalarProjectionCaseArm, ScalarProjectionExpr,
        },
        value::Value,
    };
    use std::{borrow::Cow, cmp::Ordering};

    struct TestRowView {
        slots: Vec<Option<Value>>,
    }

    impl CompiledExprSlotReader for TestRowView {
        fn compiled_slot_value(&self, slot: usize) -> Option<Cow<'_, Value>> {
            self.slots
                .get(slot)
                .and_then(Option::as_ref)
                .map(Cow::Borrowed)
        }
    }

    fn row_view() -> TestRowView {
        TestRowView {
            slots: vec![
                Some(Value::Uint(7)),
                Some(Value::Int(3)),
                Some(Value::Null),
                Some(Value::Text("MiXeD".to_string())),
                Some(Value::Bool(true)),
            ],
        }
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
    fn grouped_compiled_expr_case_slot_literal_selects_without_condition_value() {
        let expr = GroupedCompiledExpr::CaseSlotLiteral {
            op: BinaryOp::Gt,
            slot: 0,
            field: "age".to_string(),
            literal: Value::Uint(5),
            slot_on_left: true,
            then_expr: Box::new(GroupedCompiledExpr::Literal(Value::Text(
                "selected".to_string(),
            ))),
            else_expr: Box::new(GroupedCompiledExpr::Literal(Value::Text(
                "else".to_string(),
            ))),
        };

        assert_eq!(evaluate(&expr), Value::Text("selected".to_string()));
    }

    #[test]
    fn grouped_compiled_expr_case_slot_bool_preserves_null_fallthrough() {
        let expr = GroupedCompiledExpr::CaseSlotBool {
            slot: 2,
            field: "maybe_flag".to_string(),
            then_expr: Box::new(GroupedCompiledExpr::Literal(Value::Text(
                "selected".to_string(),
            ))),
            else_expr: Box::new(GroupedCompiledExpr::Literal(Value::Text(
                "else".to_string(),
            ))),
        };

        assert_eq!(evaluate(&expr), Value::Text("else".to_string()));
    }

    #[test]
    fn grouped_compiled_expr_constant_case_condition_is_hoisted() {
        let expr = ScalarProjectionExpr::Case {
            when_then_arms: vec![ScalarProjectionCaseArm::new(
                ScalarProjectionExpr::Literal(Value::Bool(false)),
                ScalarProjectionExpr::Literal(Value::Text("then".to_string())),
            )],
            else_expr: Box::new(ScalarProjectionExpr::Literal(Value::Text(
                "else".to_string(),
            ))),
        };

        assert_eq!(
            GroupedCompiledExpr::compile(&expr),
            GroupedCompiledExpr::Literal(Value::Text("else".to_string())),
        );
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
