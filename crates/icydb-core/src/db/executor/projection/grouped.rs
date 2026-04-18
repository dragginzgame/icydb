//! Module: db::executor::projection::grouped
//! Defines grouped-row projection evaluation over finalized group keys and
//! aggregate outputs.

use crate::{
    db::{
        executor::projection::eval::{
            ProjectionEvalError, collapse_true_only_boolean_admission, eval_binary_expr,
            eval_projection_function_call, eval_unary_expr, projection_function_name,
        },
        query::{
            builder::AggregateExpr,
            plan::{
                FieldSlot, GroupedAggregateExecutionSpec, PlannedProjectionLayout,
                expr::{BinaryOp, Expr, Function, ProjectionSpec, UnaryOp, projection_field_expr},
            },
        },
    },
    error::InternalError,
    value::Value,
};

///
/// GroupedRowView
///
/// Read-only grouped-row adapter for expression evaluation over finalized
/// grouped-key and aggregate outputs.
///

pub(in crate::db::executor) struct GroupedRowView<'a> {
    pub(in crate::db::executor::projection) key_values: &'a [Value],
    pub(in crate::db::executor::projection) aggregate_values: &'a [Value],
    #[cfg(test)]
    group_fields: &'a [FieldSlot],
    #[cfg(test)]
    pub(in crate::db::executor::projection) aggregate_execution_specs:
        &'a [GroupedAggregateExecutionSpec],
}

impl<'a> GroupedRowView<'a> {
    /// Build one grouped-row adapter from grouped finalization payloads.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        key_values: &'a [Value],
        aggregate_values: &'a [Value],
        group_fields: &'a [FieldSlot],
        aggregate_execution_specs: &'a [GroupedAggregateExecutionSpec],
    ) -> Self {
        #[cfg(not(test))]
        let _ = (group_fields, aggregate_execution_specs);

        Self {
            key_values,
            aggregate_values,
            #[cfg(test)]
            group_fields,
            #[cfg(test)]
            aggregate_execution_specs,
        }
    }

    /// Borrow grouped key values in grouped-field declaration order.
    #[must_use]
    pub(in crate::db::executor) const fn key_values(&self) -> &'a [Value] {
        self.key_values
    }

    /// Borrow finalized grouped aggregate values in execution-spec order.
    #[must_use]
    pub(in crate::db::executor) const fn aggregate_values(&self) -> &'a [Value] {
        self.aggregate_values
    }

    /// Borrow grouped field slots used to interpret grouped key offsets.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::executor) const fn group_fields(&self) -> &'a [FieldSlot] {
        self.group_fields
    }
}

///
/// GroupedProjectionExpr
///
/// GroupedProjectionExpr is the compiled grouped-output projection tree used
/// by grouped finalization and grouped-row materialization.
/// Group-field offsets and aggregate indexes are resolved once so hot grouped
/// output loops only do direct slice indexing at evaluation time.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) enum GroupedProjectionExpr {
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
/// Compiled grouped searched-CASE arm used by grouped projection and grouped
/// HAVING evaluation. This keeps grouped branch execution lazy while staying on
/// the same compiled grouped-expression seam as other scalar/grouped nodes.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) struct GroupedProjectionCaseArm {
    condition: GroupedProjectionExpr,
    result: GroupedProjectionExpr,
}

impl GroupedProjectionCaseArm {
    /// Build one compiled grouped CASE arm.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        condition: GroupedProjectionExpr,
        result: GroupedProjectionExpr,
    ) -> Self {
        Self { condition, result }
    }

    /// Borrow the compiled grouped condition expression.
    #[must_use]
    pub(in crate::db::executor) const fn condition(&self) -> &GroupedProjectionExpr {
        &self.condition
    }

    /// Borrow the compiled grouped result expression.
    #[must_use]
    pub(in crate::db::executor) const fn result(&self) -> &GroupedProjectionExpr {
        &self.result
    }
}

///
/// GroupedProjectionField
///
/// GroupedProjectionField is one resolved grouped-field leaf inside a compiled
/// grouped projection expression.
/// It preserves field-name diagnostics while turning grouped field access into
/// one direct grouped-key slice lookup.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) struct GroupedProjectionField {
    field: String,
    offset: usize,
}

///
/// GroupedProjectionAggregate
///
/// GroupedProjectionAggregate is one resolved grouped aggregate leaf inside a
/// compiled grouped projection expression.
/// It preserves aggregate-index diagnostics while turning grouped aggregate
/// access into one direct aggregate-value slice lookup.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) struct GroupedProjectionAggregate {
    index: usize,
}

///
/// CompiledGroupedProjectionPlan
///
/// Executor-owned grouped projection compilation contract.
/// This keeps the grouped identity short-circuit and compiled projection
/// carriage under the projection boundary so grouped runtime lanes consume one
/// shared compiled evaluator contract instead of open-coding it.
///

#[derive(Clone)]
pub(in crate::db::executor) struct CompiledGroupedProjectionPlan<'a> {
    compiled_projection: Vec<GroupedProjectionExpr>,
    projection_layout: &'a PlannedProjectionLayout,
    group_fields: &'a [FieldSlot],
    aggregate_execution_specs: &'a [GroupedAggregateExecutionSpec],
}

impl<'a> CompiledGroupedProjectionPlan<'a> {
    /// Build one compiled grouped projection contract from already-compiled expressions.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::executor) const fn from_parts_for_test(
        compiled_projection: Vec<GroupedProjectionExpr>,
        projection_layout: &'a PlannedProjectionLayout,
        group_fields: &'a [FieldSlot],
        aggregate_execution_specs: &'a [GroupedAggregateExecutionSpec],
    ) -> Self {
        Self {
            compiled_projection,
            projection_layout,
            group_fields,
            aggregate_execution_specs,
        }
    }

    /// Borrow the compiled grouped projection expression slice.
    #[must_use]
    pub(in crate::db::executor) const fn compiled_projection(&self) -> &[GroupedProjectionExpr] {
        self.compiled_projection.as_slice()
    }

    /// Borrow the planner-owned grouped projection layout.
    #[must_use]
    pub(in crate::db::executor) const fn projection_layout(&self) -> &'a PlannedProjectionLayout {
        self.projection_layout
    }

    /// Borrow grouped key field slots used by grouped projection evaluation.
    #[must_use]
    pub(in crate::db::executor) const fn group_fields(&self) -> &'a [FieldSlot] {
        self.group_fields
    }

    /// Borrow grouped aggregate execution specs used by grouped projection evaluation.
    #[must_use]
    pub(in crate::db::executor) const fn aggregate_execution_specs(
        &self,
    ) -> &'a [GroupedAggregateExecutionSpec] {
        self.aggregate_execution_specs
    }
}

/// Compile one grouped projection contract only when the planner has not
/// already proved the grouped output projection is row-identical.
pub(in crate::db::executor) fn compile_grouped_projection_plan_if_needed<'a>(
    projection: &ProjectionSpec,
    projection_is_identity: bool,
    projection_layout: &'a PlannedProjectionLayout,
    group_fields: &'a [FieldSlot],
    aggregate_execution_specs: &'a [GroupedAggregateExecutionSpec],
) -> Result<Option<CompiledGroupedProjectionPlan<'a>>, InternalError> {
    if projection_is_identity {
        return Ok(None);
    }

    let compiled_projection =
        compile_grouped_projection_plan(projection, group_fields, aggregate_execution_specs)
            .map_err(ProjectionEvalError::into_grouped_projection_internal_error)?;

    Ok(Some(CompiledGroupedProjectionPlan {
        compiled_projection,
        projection_layout,
        group_fields,
        aggregate_execution_specs,
    }))
}

/// Compile one grouped projection spec into direct grouped field/aggregate lookups.
pub(in crate::db::executor) fn compile_grouped_projection_plan(
    projection: &ProjectionSpec,
    group_fields: &[FieldSlot],
    aggregate_execution_specs: &[GroupedAggregateExecutionSpec],
) -> Result<Vec<GroupedProjectionExpr>, ProjectionEvalError> {
    let mut compiled_fields = Vec::with_capacity(projection.len());

    for field in projection.fields() {
        compiled_fields.push(compile_grouped_projection_expr(
            projection_field_expr(field),
            group_fields,
            aggregate_execution_specs,
        )?);
    }

    Ok(compiled_fields)
}

/// Evaluate one compiled grouped projection plan into ordered projected values.
#[cfg(test)]
pub(in crate::db::executor) fn evaluate_grouped_projection_values(
    compiled_projection: &[GroupedProjectionExpr],
    grouped_row: &GroupedRowView<'_>,
) -> Result<Vec<Value>, ProjectionEvalError> {
    let mut projected_values = Vec::with_capacity(compiled_projection.len());

    for expr in compiled_projection {
        projected_values.push(eval_grouped_projection_expr(expr, grouped_row)?);
    }

    Ok(projected_values)
}

/// Evaluate one grouped projection expression against one grouped output row view.

/// Evaluate one compiled grouped HAVING expression against one grouped output row.
pub(in crate::db::executor) fn evaluate_grouped_having_expr(
    expr: &GroupedProjectionExpr,
    grouped_row: &GroupedRowView<'_>,
) -> Result<bool, ProjectionEvalError> {
    collapse_true_only_boolean_admission(
        eval_grouped_projection_expr(expr, grouped_row)?,
        |found| ProjectionEvalError::InvalidGroupedHavingResult { found },
    )
}

pub(in crate::db::executor) fn eval_grouped_projection_expr(
    expr: &GroupedProjectionExpr,
    grouped_row: &GroupedRowView<'_>,
) -> Result<Value, ProjectionEvalError> {
    match expr {
        GroupedProjectionExpr::Field(field) => {
            let Some(value) = grouped_row.key_values().get(field.offset) else {
                return Err(ProjectionEvalError::MissingFieldValue {
                    field: field.field.clone(),
                    index: field.offset,
                });
            };

            Ok(value.clone())
        }
        GroupedProjectionExpr::Aggregate(aggregate) => {
            let Some(value) = grouped_row.aggregate_values().get(aggregate.index) else {
                return Err(ProjectionEvalError::MissingGroupedAggregateValue {
                    aggregate_index: aggregate.index,
                    aggregate_count: grouped_row.aggregate_values().len(),
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

            eval_projection_function_call(*function, evaluated_args.as_slice()).map_err(|err| {
                ProjectionEvalError::InvalidFunctionCall {
                    function: projection_function_name(*function).to_string(),
                    message: err.to_string(),
                }
            })
        }
        GroupedProjectionExpr::Unary { op, expr } => {
            let operand = eval_grouped_projection_expr(expr, grouped_row)?;
            eval_unary_expr(*op, &operand)
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

            eval_binary_expr(*op, &left, &right)
        }
    }
}

pub(in crate::db::executor::projection) fn resolve_group_field_offset(
    group_fields: &[FieldSlot],
    field_name: &str,
) -> Option<usize> {
    for (offset, group_field) in group_fields.iter().enumerate() {
        if group_field.field() == field_name {
            return Some(offset);
        }
    }

    None
}

pub(in crate::db::executor::projection) fn resolve_grouped_aggregate_index(
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

pub(in crate::db::executor) fn compile_grouped_projection_expr(
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
                    Ok(GroupedProjectionCaseArm::new(
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
