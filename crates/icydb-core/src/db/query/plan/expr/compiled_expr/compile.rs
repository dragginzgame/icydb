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
            construction::ConstructionBudget,
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
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, QueryProjectionCode};

/// Compile against accepted authority, distinguishing unsupported syntax from budget failure.
pub(in crate::db) fn compile_scalar_projection_expr_with_schema(
    schema: &SchemaInfo,
    expr: &Expr,
    budget: &dyn ConstructionBudget,
) -> Result<Option<CompiledExpr>, InternalError> {
    match CompiledExpr::compile_scalar(
        expr,
        &|leaf| {
            compile_scalar_leaf(schema, leaf, budget)
                .map_err(ScalarCompilationError::Budget)?
                .ok_or(ScalarCompilationError::Unavailable)
        },
        budget,
        ScalarCompilationError::Budget,
    ) {
        Ok(compiled) => Ok(Some(compiled)),
        Err(ScalarCompilationError::Unavailable) => Ok(None),
        Err(ScalarCompilationError::Budget(error)) => Err(error),
    }
}

// Internal-only distinction; callers retain their existing unsupported-shape errors.
enum ScalarCompilationError {
    Unavailable,
    Budget(InternalError),
}

/// Compile the scalar projection directly into its final row-slot programs.
pub(in crate::db) fn compile_scalar_projection_plan_with_schema(
    schema: &SchemaInfo,
    projection: &ProjectionSpec,
    budget: &dyn ConstructionBudget,
) -> Result<Option<Vec<CompiledExpr>>, InternalError> {
    let mut fields = budget.vec_with_capacity(projection.len())?;
    for field in projection.fields() {
        let Some(compiled) =
            compile_scalar_projection_expr_with_schema(schema, field.expr(), budget)?
        else {
            return Ok(None);
        };
        fields.push(compiled);
    }
    Ok(Some(fields))
}

// Slot lookup internals remain separately owned; labels and path buffers charge here.
fn compile_scalar_leaf(
    schema: &SchemaInfo,
    expr: &Expr,
    budget: &dyn ConstructionBudget,
) -> Result<Option<CompiledExpr>, InternalError> {
    Ok(Some(match expr {
        Expr::Field(field) => {
            budget.charge(
                Resource::PredicateExpressionSteps,
                1 + field.as_str().len() as u64,
            )?;
            let Some(slot) = schema.field_slot_index(field.as_str()) else {
                return Ok(None);
            };
            CompiledExpr::Slot {
                slot,
                field: budget.copy_text(field.as_str())?,
            }
        }
        Expr::FieldPath(path) => {
            budget.charge(
                Resource::PredicateExpressionSteps,
                1 + path.root().as_str().len() as u64,
            )?;
            let Some(root_slot) = schema.field_slot_index(path.root().as_str()) else {
                return Ok(None);
            };
            let mut field = budget.copy_text(path.root().as_str())?;
            let mut segments = budget.vec_with_capacity(path.segments().len())?;
            let mut segment_bytes = budget.vec_with_capacity(path.segments().len())?;
            for segment in path.segments() {
                segments.push(budget.copy_text(segment)?);
                budget.charge(Resource::PredicateExpressionSteps, segment.len() as u64)?;
                let mut bytes = budget.vec_with_capacity(segment.len())?;
                bytes.extend_from_slice(segment.as_bytes());
                segment_bytes.push(bytes.into_boxed_slice());
                budget.push_text(&mut field, ".")?;
                budget.push_text(&mut field, segment)?;
            }
            CompiledExpr::FieldPath {
                root_slot,
                field,
                segments: segments.into_boxed_slice(),
                segment_bytes: segment_bytes.into_boxed_slice(),
            }
        }
        _ => return Ok(None),
    }))
}

// Standalone builders have no database request. This private adapter cannot be
// selected by a database caller; request/execution compilation requires an owner.
struct PreviewConstructionBudget;
impl ConstructionBudget for PreviewConstructionBudget {
    fn charge(&self, _resource: Resource, _amount: u64) -> Result<(), InternalError> {
        Ok(())
    }
}

/// Compile a single-value preview with its existing field/path admission policy.
pub(in crate::db::query::plan::expr) fn compile_builder_preview_expr(
    expr: &Expr,
    field_name: &str,
    value_slot: usize,
) -> Result<CompiledExpr, QueryError> {
    CompiledExpr::compile_scalar(
        expr,
        &|leaf| match leaf {
            Expr::Field(field) if field.as_str() == field_name => Ok(CompiledExpr::Slot {
                slot: value_slot,
                field: field.as_str().to_string(),
            }),
            Expr::FieldPath(_) => Err(QueryError::unsupported_projection(
                QueryProjectionCode::NestedFieldPathPreview,
            )),
            _ => Err(QueryError::invariant()),
        },
        &PreviewConstructionBudget,
        QueryError::execute,
    )
}

impl CompiledExpr {
    fn compile_scalar<E>(
        expr: &Expr,
        leaf: &impl Fn(&Expr) -> Result<Self, E>,
        budget: &dyn ConstructionBudget,
        budget_error: fn(InternalError) -> E,
    ) -> Result<Self, E> {
        budget
            .charge(Resource::PredicateExpressionSteps, 1)
            .map_err(budget_error)?;
        Ok(match expr {
            Expr::Field(_) | Expr::FieldPath(_) | Expr::Aggregate(_) => leaf(expr)?,
            Expr::Literal(value) => Self::Literal(budget.copy_value(value).map_err(budget_error)?),
            Expr::FunctionCall { function, args } => {
                let mut compiled = budget.vec_with_capacity(args.len()).map_err(budget_error)?;
                for arg in args {
                    compiled.push(Self::compile_scalar(arg, leaf, budget, budget_error)?);
                }
                Self::FunctionCall {
                    function: *function,
                    args: compiled.into_boxed_slice(),
                }
            }
            Expr::Unary { op, expr } => Self::Unary {
                op: *op,
                expr: budget
                    .boxed(Self::compile_scalar(expr, leaf, budget, budget_error)?)
                    .map_err(budget_error)?,
            },
            Expr::Case {
                when_then_arms,
                else_expr,
            } => {
                let mut arms = budget
                    .vec_with_capacity(when_then_arms.len())
                    .map_err(budget_error)?;
                // Admit every condition/result/ELSE before parent specialization.
                for arm in when_then_arms {
                    arms.push(CompiledExprCaseArm::new(
                        Self::compile_scalar(arm.condition(), leaf, budget, budget_error)?,
                        Self::compile_scalar(arm.result(), leaf, budget, budget_error)?,
                    ));
                }
                let else_expr = Self::compile_scalar(else_expr, leaf, budget, budget_error)?;
                Self::compile_case(arms, else_expr, budget).map_err(budget_error)?
            }
            Expr::Binary { op, left, right } => {
                let left = Self::compile_scalar(left, leaf, budget, budget_error)?;
                let right = Self::compile_scalar(right, leaf, budget, budget_error)?;
                Self::compile_binary(*op, left, right, budget).map_err(budget_error)?
            }
            #[cfg(test)]
            Expr::Alias { expr, .. } => Self::compile_scalar(expr, leaf, budget, budget_error)?,
        })
    }

    // Collapse one-arm CASE programs into condition-specialized forms when
    // the condition shape can be decided without evaluating a boolean Value.
    // Multi-arm searched CASE keeps the generic arm list to preserve normal
    // short-circuit behavior without adding a broader expression VM.
    fn compile_case(
        when_then_arms: Vec<CompiledExprCaseArm>,
        else_expr: Self,
        budget: &dyn ConstructionBudget,
    ) -> Result<Self, InternalError> {
        match <[CompiledExprCaseArm; 1]>::try_from(when_then_arms) {
            Ok([arm]) => {
                let CompiledExprCaseArm { condition, result } = arm;
                Self::compile_single_arm_case(condition, result, else_expr, budget)
            }
            Err(arms) => Ok(Self::Case {
                when_then_arms: arms.into_boxed_slice(),
                else_expr: budget.boxed(else_expr)?,
            }),
        }
    }

    // Convert common searched-CASE conditions into direct slot predicates.
    // Constant TRUE/FALSE/NULL conditions are selected once during grouped
    // setup, which removes invariant condition evaluation from the row loop.
    fn compile_single_arm_case(
        condition: Self,
        then_expr: Self,
        else_expr: Self,
        budget: &dyn ConstructionBudget,
    ) -> Result<Self, InternalError> {
        Ok(match condition {
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
                then_expr: budget.boxed(then_expr)?,
                else_expr: budget.boxed(else_expr)?,
            },
            Self::Slot { slot, field } => Self::CaseSlotBool {
                slot,
                field,
                then_expr: budget.boxed(then_expr)?,
                else_expr: budget.boxed(else_expr)?,
            },
            condition => {
                let mut arms = budget.vec_with_capacity(1)?;
                arms.push(CompiledExprCaseArm::new(condition, then_expr));
                Self::Case {
                    when_then_arms: arms.into_boxed_slice(),
                    else_expr: budget.boxed(else_expr)?,
                }
            }
        })
    }

    // Specialization consumes the already-owned operands. Moving labels and
    // literals avoids copying payloads that the temporary operand nodes then drop.
    fn compile_binary(
        op: BinaryOp,
        left: Self,
        right: Self,
        budget: &dyn ConstructionBudget,
    ) -> Result<Self, InternalError> {
        Ok(match (left, right) {
            (
                Self::Slot {
                    slot: left_slot,
                    field: left_field,
                },
                Self::Slot {
                    slot: right_slot,
                    field: right_field,
                },
            ) => Self::compile_slot_slot_binary(
                op,
                left_slot,
                left_field,
                right_slot,
                right_field,
                budget,
            )?,
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
                left: budget.boxed(left)?,
                right: budget.boxed(right)?,
            },
        })
    }

    // Preserve the established direct arithmetic/comparison variants. Boolean
    // slot pairs retain generic evaluation and its existing boolean semantics.
    fn compile_slot_slot_binary(
        op: BinaryOp,
        left_slot: usize,
        left_field: String,
        right_slot: usize,
        right_field: String,
        budget: &dyn ConstructionBudget,
    ) -> Result<Self, InternalError> {
        Ok(match op {
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
                left: budget.boxed(Self::Slot {
                    slot: left_slot,
                    field: left_field,
                })?,
                right: budget.boxed(Self::Slot {
                    slot: right_slot,
                    field: right_field,
                })?,
            },
        })
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
    use super::PreviewConstructionBudget;
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
    fn binary_specialization_moves_owned_labels_and_literals() {
        let left = "left".to_string();
        let right = "right".to_string();
        let pointers = (left.as_ptr(), right.as_ptr());
        let compiled = CompiledExpr::compile_binary(
            BinaryOp::Add,
            slot(0, left),
            slot(1, right),
            &PreviewConstructionBudget,
        )
        .unwrap();
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
            let compiled =
                CompiledExpr::compile_binary(BinaryOp::Lt, left, right, &PreviewConstructionBudget)
                    .unwrap();
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
}
