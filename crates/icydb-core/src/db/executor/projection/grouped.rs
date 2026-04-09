//! Module: db::executor::projection::grouped
//! Responsibility: module-local ownership and contracts for db::executor::projection::grouped.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::projection::eval::{ProjectionEvalError, eval_binary_expr, eval_unary_expr},
        query::{
            builder::AggregateExpr,
            plan::{
                FieldSlot, GroupedAggregateExecutionSpec,
                expr::{BinaryOp, Expr, ProjectionField, ProjectionSpec, UnaryOp},
            },
        },
    },
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
    pub(in crate::db::executor::projection) group_fields: &'a [FieldSlot],
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
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
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

/// Compile one grouped projection spec into direct grouped field/aggregate lookups.
pub(in crate::db::executor) fn compile_grouped_projection_plan(
    projection: &ProjectionSpec,
    group_fields: &[FieldSlot],
    aggregate_execution_specs: &[GroupedAggregateExecutionSpec],
) -> Result<Vec<GroupedProjectionExpr>, ProjectionEvalError> {
    let mut compiled_fields = Vec::with_capacity(projection.len());

    for field in projection.fields() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                compiled_fields.push(compile_grouped_projection_expr(
                    expr,
                    group_fields,
                    aggregate_execution_specs,
                )?);
            }
        }
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
pub(in crate::db::executor) fn eval_grouped_projection_expr(
    expr: &GroupedProjectionExpr,
    grouped_row: &GroupedRowView<'_>,
) -> Result<Value, ProjectionEvalError> {
    match expr {
        GroupedProjectionExpr::Field(field) => {
            let Some(value) = grouped_row.key_values.get(field.offset) else {
                return Err(ProjectionEvalError::MissingFieldValue {
                    field: field.field.clone(),
                    index: field.offset,
                });
            };

            Ok(value.clone())
        }
        GroupedProjectionExpr::Aggregate(aggregate) => {
            let Some(value) = grouped_row.aggregate_values.get(aggregate.index) else {
                return Err(ProjectionEvalError::MissingGroupedAggregateValue {
                    aggregate_index: aggregate.index,
                    aggregate_count: grouped_row.aggregate_values.len(),
                });
            };

            Ok(value.clone())
        }
        GroupedProjectionExpr::Literal(value) => Ok(value.clone()),
        GroupedProjectionExpr::Unary { op, expr } => {
            let operand = eval_grouped_projection_expr(expr, grouped_row)?;
            eval_unary_expr(*op, operand)
        }
        GroupedProjectionExpr::Binary { op, left, right } => {
            let left = eval_grouped_projection_expr(left, grouped_row)?;
            let right = eval_grouped_projection_expr(right, grouped_row)?;

            eval_binary_expr(*op, left, right)
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
        Expr::Alias { expr, .. } => {
            compile_grouped_projection_expr(expr.as_ref(), group_fields, aggregate_execution_specs)
        }
    }
}
