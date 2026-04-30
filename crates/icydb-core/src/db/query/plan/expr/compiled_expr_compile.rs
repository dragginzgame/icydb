//! Module: query::plan::expr::compiled_expr_compile
//! Responsibility: one-way compilation from planner expression surfaces into
//! the unified compiled expression IR.
//! Does not own: compiled expression evaluation or executor row access.
//! Boundary: this module is the only place planner expression trees are allowed
//! to be translated into `CompiledExpr`.

use crate::{
    db::query::{
        builder::AggregateExpr,
        plan::{
            FieldSlot, GroupedAggregateExecutionSpec,
            expr::{
                BinaryOp, CompiledExpr, CompiledExprCaseArm, Expr, FieldPath, ProjectionEvalError,
                ProjectionSpec, ScalarProjectionCaseArm, ScalarProjectionExpr,
            },
        },
    },
    value::Value,
};

impl CompiledExpr {
    /// Compile one planner scalar projection tree into the unified slot IR.
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
                    .map(CompiledExprCaseArm::compile)
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
                when_then_arms: vec![CompiledExprCaseArm::new(condition, then_expr)]
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
}

impl CompiledExprCaseArm {
    // Compile one searched-CASE arm from the planner scalar projection form.
    fn compile(arm: &ScalarProjectionCaseArm) -> Self {
        Self::new(
            CompiledExpr::compile(arm.condition()),
            CompiledExpr::compile(arm.result()),
        )
    }
}

/// Compile one grouped projection spec into direct grouped field/aggregate lookups.
pub(in crate::db) fn compile_grouped_projection_plan(
    projection: &ProjectionSpec,
    group_fields: &[FieldSlot],
    aggregate_execution_specs: &[GroupedAggregateExecutionSpec],
) -> Result<Vec<CompiledExpr>, ProjectionEvalError> {
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
) -> Result<CompiledExpr, ProjectionEvalError> {
    match expr {
        Expr::Field(field_id) => {
            let field_name = field_id.as_str();
            let Some(offset) = resolve_group_field_offset(group_fields, field_name) else {
                return Err(ProjectionEvalError::UnknownField {
                    field: field_name.to_string(),
                });
            };

            Ok(CompiledExpr::GroupKey {
                offset,
                field: field_name.to_string(),
            })
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

            Ok(CompiledExpr::Aggregate { index })
        }
        Expr::Literal(value) => Ok(CompiledExpr::Literal(value.clone())),
        Expr::FunctionCall { function, args } => Ok(CompiledExpr::FunctionCall {
            function: *function,
            args: args
                .iter()
                .map(|arg| {
                    compile_grouped_projection_expr(arg, group_fields, aggregate_execution_specs)
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_boxed_slice(),
        }),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Ok(CompiledExpr::Case {
            when_then_arms: when_then_arms
                .iter()
                .map(|arm| {
                    Ok::<CompiledExprCaseArm, ProjectionEvalError>(CompiledExprCaseArm::new(
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
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_boxed_slice(),
            else_expr: Box::new(compile_grouped_projection_expr(
                else_expr.as_ref(),
                group_fields,
                aggregate_execution_specs,
            )?),
        }),
        Expr::Unary { op, expr } => Ok(CompiledExpr::Unary {
            op: *op,
            expr: Box::new(compile_grouped_projection_expr(
                expr.as_ref(),
                group_fields,
                aggregate_execution_specs,
            )?),
        }),
        Expr::Binary { op, left, right } => Ok(CompiledExpr::Binary {
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

const fn is_comparison_op(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Gt | BinaryOp::Gte
    )
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
        db::query::plan::expr::{CompiledExpr, ScalarProjectionCaseArm, ScalarProjectionExpr},
        value::Value,
    };

    #[test]
    fn compiled_expr_constant_case_condition_is_hoisted() {
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
            CompiledExpr::compile(&expr),
            CompiledExpr::Literal(Value::Text("else".to_string())),
        );
    }
}
