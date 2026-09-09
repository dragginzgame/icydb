//! Module: query::plan::expr::compiled_expr::compile
//! Responsibility: one-way compilation from planner expression surfaces into
//! the unified compiled expression IR.
//! Does not own: compiled expression evaluation or executor row access.
//! Boundary: this module is the only place planner expression trees are allowed
//! to be translated into `CompiledExpr`.

use crate::{
    db::{
        QueryError,
        query::{
            builder::AggregateExpr,
            plan::{
                GroupedAggregateExecutionSpec,
                expr::{
                    BinaryOp, CompiledExpr, CompiledExprCaseArm, Expr, ProjectionEvalError,
                    ProjectionSpec,
                },
            },
        },
        schema::SchemaInfo,
    },
    value::Value,
};
use icydb_diagnostic_code::QueryProjectionCode;

/// Compile a scalar expression directly against the caller's accepted schema.
/// Every child must be available before its parent is specialized, including
/// branches that a constant CASE condition will subsequently discard.
pub(in crate::db) fn compile_scalar_projection_expr_with_schema(
    schema: &SchemaInfo,
    expr: &Expr,
) -> Option<CompiledExpr> {
    CompiledExpr::compile_scalar(expr, &|leaf| compile_scalar_leaf(schema, leaf).ok_or(())).ok()
}

/// Compile the scalar projection directly into its final row-slot programs.
pub(in crate::db) fn compile_scalar_projection_plan_with_schema(
    schema: &SchemaInfo,
    projection: &ProjectionSpec,
) -> Option<Vec<CompiledExpr>> {
    let mut compiled_fields = Vec::with_capacity(projection.len());
    for field in projection.fields() {
        compiled_fields.push(compile_scalar_projection_expr_with_schema(
            schema,
            field.expr(),
        )?);
    }

    Some(compiled_fields)
}

// Schema and single-value previews keep their existing leaf admission policies;
// recursion, container construction and specialization have one compiler owner.
fn compile_scalar_leaf(schema: &SchemaInfo, expr: &Expr) -> Option<CompiledExpr> {
    Some(match expr {
        Expr::Field(field) => CompiledExpr::Slot {
            slot: schema.field_slot_index(field.as_str())?,
            field: field.as_str().to_string(),
        },
        Expr::FieldPath(path) => {
            let root_slot = schema.field_slot_index(path.root().as_str())?;
            let segment_bytes = path
                .segments()
                .iter()
                .map(|segment| segment.as_bytes().to_vec().into_boxed_slice())
                .collect::<Vec<_>>()
                .into_boxed_slice();
            CompiledExpr::FieldPath {
                root_slot,
                field: path.path_spec().dotted_label(),
                segments: path.segments().to_vec().into_boxed_slice(),
                segment_bytes,
            }
        }
        _ => return None,
    })
}

/// Compile a single-value preview with its existing field/path admission policy.
pub(in crate::db::query::plan::expr) fn compile_builder_preview_expr(
    expr: &Expr,
    field_name: &str,
    value_slot: usize,
) -> Result<CompiledExpr, QueryError> {
    CompiledExpr::compile_scalar(expr, &|leaf| match leaf {
        Expr::Field(field) if field.as_str() == field_name => Ok(CompiledExpr::Slot {
            slot: value_slot,
            field: field.as_str().to_string(),
        }),
        Expr::FieldPath(_) => Err(QueryError::unsupported_projection(
            QueryProjectionCode::NestedFieldPathPreview,
        )),
        _ => Err(QueryError::invariant()),
    })
}

impl CompiledExpr {
    fn compile_scalar<E>(expr: &Expr, leaf: &impl Fn(&Expr) -> Result<Self, E>) -> Result<Self, E> {
        Ok(match expr {
            Expr::Field(_) | Expr::FieldPath(_) | Expr::Aggregate(_) => leaf(expr)?,
            Expr::Literal(value) => Self::Literal(value.clone()),
            Expr::FunctionCall { function, args } => Self::FunctionCall {
                function: *function,
                args: args
                    .iter()
                    .map(|arg| Self::compile_scalar(arg, leaf))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_boxed_slice(),
            },
            Expr::Unary { op, expr } => Self::Unary {
                op: *op,
                expr: Box::new(Self::compile_scalar(expr, leaf)?),
            },
            Expr::Case {
                when_then_arms,
                else_expr,
            } => {
                let arms = when_then_arms
                    .iter()
                    .map(|arm| {
                        Ok(CompiledExprCaseArm::new(
                            Self::compile_scalar(arm.condition(), leaf)?,
                            Self::compile_scalar(arm.result(), leaf)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, E>>()?;
                let else_expr = Self::compile_scalar(else_expr, leaf)?;
                Self::compile_case(arms, else_expr)
            }
            Expr::Binary { op, left, right } => {
                let left = Self::compile_scalar(left, leaf)?;
                let right = Self::compile_scalar(right, leaf)?;

                Self::compile_binary(*op, left, right)
            }
            #[cfg(test)]
            Expr::Alias { expr, .. } => Self::compile_scalar(expr, leaf)?,
        })
    }

    // Collapse one-arm CASE programs into condition-specialized forms when
    // the condition shape can be decided without evaluating a boolean Value.
    // Multi-arm searched CASE keeps the generic arm list to preserve normal
    // short-circuit behavior without adding a broader expression VM.
    fn compile_case(when_then_arms: Vec<CompiledExprCaseArm>, else_expr: Self) -> Self {
        match <[CompiledExprCaseArm; 1]>::try_from(when_then_arms) {
            Ok([arm]) => {
                let CompiledExprCaseArm { condition, result } = arm;
                Self::compile_single_arm_case(condition, result, else_expr)
            }
            Err(arms) => Self::Case {
                when_then_arms: arms.into_boxed_slice(),
                else_expr: Box::new(else_expr),
            },
        }
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

    // Specialization consumes the already-owned operands. Moving labels and
    // literals avoids copying payloads that the temporary operand nodes then drop.
    fn compile_binary(op: BinaryOp, left: Self, right: Self) -> Self {
        match (left, right) {
            (
                Self::Slot {
                    slot: left_slot,
                    field: left_field,
                },
                Self::Slot {
                    slot: right_slot,
                    field: right_field,
                },
            ) => Self::compile_slot_slot_binary(op, left_slot, left_field, right_slot, right_field),
            (Self::Slot { field, slot }, Self::Literal(literal)) => Self::BinarySlotLiteral {
                op,
                slot,
                field,
                literal,
                slot_on_left: true,
            },
            (Self::Literal(literal), Self::Slot { field, slot }) => Self::BinarySlotLiteral {
                op,
                slot,
                field,
                literal,
                slot_on_left: false,
            },
            (left, right) => Self::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            },
        }
    }

    // Preserve the established direct arithmetic/comparison variants. Boolean
    // slot pairs retain generic evaluation and its existing boolean semantics.
    fn compile_slot_slot_binary(
        op: BinaryOp,
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
    ) -> Self {
        match op {
            BinaryOp::Add => Self::Add {
                left_slot,
                left_field,
                right_slot,
                right_field,
            },
            BinaryOp::Sub => Self::Sub {
                left_slot,
                left_field,
                right_slot,
                right_field,
            },
            BinaryOp::Mul => Self::Mul {
                left_slot,
                left_field,
                right_slot,
                right_field,
            },
            BinaryOp::Div => Self::Div {
                left_slot,
                left_field,
                right_slot,
                right_field,
            },
            BinaryOp::Eq => Self::Eq {
                left_slot,
                left_field,
                right_slot,
                right_field,
            },
            BinaryOp::Ne => Self::Ne {
                left_slot,
                left_field,
                right_slot,
                right_field,
            },
            BinaryOp::Lt => Self::Lt {
                left_slot,
                left_field,
                right_slot,
                right_field,
            },
            BinaryOp::Lte => Self::Lte {
                left_slot,
                left_field,
                right_slot,
                right_field,
            },
            BinaryOp::Gt => Self::Gt {
                left_slot,
                left_field,
                right_slot,
                right_field,
            },
            BinaryOp::Gte => Self::Gte {
                left_slot,
                left_field,
                right_slot,
                right_field,
            },
            BinaryOp::Or | BinaryOp::And => Self::Binary {
                op,
                left: Box::new(Self::Slot {
                    slot: left_slot,
                    field: left_field,
                }),
                right: Box::new(Self::Slot {
                    slot: right_slot,
                    field: right_field,
                }),
            },
        }
    }
}

/// Compile one grouped projection spec into direct grouped field/aggregate lookups.
pub(in crate::db) fn compile_grouped_projection_plan(
    projection: &ProjectionSpec,
    group_fields: &crate::db::query::plan::GroupFieldSet,
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
    group_fields: &crate::db::query::plan::GroupFieldSet,
    aggregate_execution_specs: &[GroupedAggregateExecutionSpec],
) -> Result<CompiledExpr, ProjectionEvalError> {
    match expr {
        Expr::Field(field_id) => {
            let field_name = field_id.as_str();
            let Some(offset) = resolve_group_field_offset(group_fields, expr) else {
                return Err(ProjectionEvalError::unknown_group_field());
            };

            Ok(CompiledExpr::GroupKey {
                offset,
                field: field_name.to_string(),
            })
        }
        Expr::FieldPath(path) => {
            let Some(offset) = resolve_group_field_offset(group_fields, expr) else {
                return Err(ProjectionEvalError::unknown_group_field());
            };
            Ok(CompiledExpr::GroupKey {
                offset,
                field: path.path_spec().dotted_label(),
            })
        }
        Expr::Aggregate(aggregate_expr) => {
            let Some(index) =
                resolve_grouped_aggregate_index(aggregate_execution_specs, aggregate_expr)
            else {
                return Err(ProjectionEvalError::unknown_grouped_aggregate_expression(
                    aggregate_expr.kind(),
                ));
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

fn resolve_group_field_offset(
    group_fields: &crate::db::query::plan::GroupFieldSet,
    expr: &Expr,
) -> Option<usize> {
    for (offset, group_field) in group_fields.iter().enumerate() {
        if group_field.matches_expr(expr) {
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::query::plan::expr::{
            BinaryOp, CompiledExpr, CompiledExprCaseArm, CompiledExprValueReader,
        },
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

    fn slot(slot: usize, field: String) -> CompiledExpr {
        CompiledExpr::Slot { slot, field }
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
                );
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
    fn binary_specialization_moves_owned_labels_and_literals() {
        let left = "left".to_string();
        let right = "right".to_string();
        let pointers = (left.as_ptr(), right.as_ptr());
        let compiled = CompiledExpr::compile_binary(BinaryOp::Add, slot(0, left), slot(1, right));
        let CompiledExpr::Add {
            left_slot,
            left_field,
            right_slot,
            right_field,
        } = compiled
        else {
            panic!("slot arithmetic should retain its direct form");
        };
        assert_eq!((left_slot, right_slot), (0, 1));
        assert_eq!((left_field.as_ptr(), right_field.as_ptr()), pointers);

        for slot_on_left in [true, false] {
            let field = "field".to_string();
            let literal = "large literal".repeat(100);
            let pointers = (field.as_ptr(), literal.as_ptr());
            let field = slot(1, field);
            let literal = CompiledExpr::Literal(Value::Text(literal));
            let (left, right) = if slot_on_left {
                (field, literal)
            } else {
                (literal, field)
            };
            let compiled = CompiledExpr::compile_binary(BinaryOp::Lt, left, right);
            let CompiledExpr::BinarySlotLiteral {
                op,
                slot,
                field,
                literal: Value::Text(literal),
                slot_on_left: actual,
            } = compiled
            else {
                panic!("slot/literal comparison should retain its direct form");
            };
            assert_eq!(op, BinaryOp::Lt);
            assert_eq!(slot, 1);
            assert_eq!(actual, slot_on_left);
            assert_eq!((field.as_ptr(), literal.as_ptr()), pointers);
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
                    (slot(0, "left".into()), slot(1, "right".into())),
                    (
                        slot(0, "left".into()),
                        CompiledExpr::Literal(row.0[1].clone()),
                    ),
                    (
                        CompiledExpr::Literal(row.0[0].clone()),
                        slot(1, "right".into()),
                    ),
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
                    let compiled = CompiledExpr::compile_binary(op, left, right);
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
                CompiledExpr::Literal(Value::Text("else".into()))
            ),
            CompiledExpr::Literal(Value::Text("else".to_string())),
        );
    }
}
