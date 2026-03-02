//! Module: executor::load::projection
//! Responsibility: scalar projection expression evaluation over materialized rows.
//! Does not own: planner expression typing/validation or grouped aggregate folds.
//! Boundary: pure evaluator + projected-row materialization for scalar load paths.

use crate::{
    db::{
        executor::load::LoadExecutor,
        numeric::{
            NumericArithmeticOp, apply_numeric_arithmetic, coerce_numeric_decimal,
            compare_numeric_eq, compare_numeric_order,
        },
        query::builder::AggregateExpr,
        query::plan::{
            AccessPlannedQuery, FieldSlot,
            expr::{BinaryOp, Expr, ProjectionField, ProjectionSpec, UnaryOp},
        },
        response::ProjectedRow,
    },
    error::InternalError,
    model::entity::resolve_field_slot,
    traits::{EntityKind, EntityValue},
    types::{Decimal, Id},
    value::Value,
};
use std::cmp::Ordering;
use thiserror::Error as ThisError;

///
/// ExecutionError
///
/// Pure expression-evaluation failures for scalar projection execution.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(in crate::db::executor) enum ExecutionError {
    #[error("projection expression references unknown field '{field}'")]
    UnknownField { field: String },

    #[error("projection expression could not read field '{field}' at index={index}")]
    MissingFieldValue { field: String, index: usize },

    #[error("projection expression cannot evaluate aggregate '{kind}' in scalar row context")]
    AggregateNotEvaluable { kind: String },

    #[error("projection unary operator '{op}' is incompatible with operand value {found:?}")]
    InvalidUnaryOperand { op: String, found: Box<Value> },

    #[error(
        "projection binary operator '{op}' is incompatible with operand values ({left:?}, {right:?})"
    )]
    InvalidBinaryOperands {
        op: String,
        left: Box<Value>,
        right: Box<Value>,
    },

    #[error(
        "grouped projection expression references unknown aggregate expression kind={kind} target_field={target_field:?} distinct={distinct}"
    )]
    UnknownGroupedAggregateExpression {
        kind: String,
        target_field: Option<String>,
        distinct: bool,
    },

    #[error(
        "grouped projection expression references aggregate output index={aggregate_index} but only {aggregate_count} outputs are available"
    )]
    MissingGroupedAggregateValue {
        aggregate_index: usize,
        aggregate_count: usize,
    },
}

///
/// GroupedRowView
///
/// Read-only grouped-row adapter for expression evaluation over finalized
/// grouped-key and aggregate outputs.
///

pub(in crate::db::executor) struct GroupedRowView<'a> {
    key_values: &'a [Value],
    aggregate_values: &'a [Value],
    group_fields: &'a [FieldSlot],
    aggregate_exprs: &'a [AggregateExpr],
}

impl<'a> GroupedRowView<'a> {
    /// Build one grouped-row adapter from grouped finalization payloads.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        key_values: &'a [Value],
        aggregate_values: &'a [Value],
        group_fields: &'a [FieldSlot],
        aggregate_exprs: &'a [AggregateExpr],
    ) -> Self {
        Self {
            key_values,
            aggregate_values,
            group_fields,
            aggregate_exprs,
        }
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Evaluate scalar projection semantics over materialized rows when the
    /// projection is no longer identity (`SELECT *`).
    pub(in crate::db::executor) fn project_materialized_rows_if_needed(
        plan: &AccessPlannedQuery<E::Key>,
        rows: &[(Id<E>, E)],
    ) -> Result<Option<Vec<ProjectedRow<E>>>, InternalError> {
        let projection = plan.projection_spec(E::MODEL);
        if projection_is_model_identity::<E>(&projection) {
            return Ok(None);
        }

        let projected = project_rows_from_projection::<E>(&projection, rows)
            .map_err(|err| InternalError::query_invalid_logical_plan(err.to_string()))?;

        Ok(Some(projected))
    }
}

/// Evaluate one projection expression against one entity row.
pub(in crate::db::executor) fn eval_expr<E>(expr: &Expr, row: &E) -> Result<Value, ExecutionError>
where
    E: EntityKind + EntityValue,
{
    match expr {
        Expr::Field(field_id) => {
            let field_name = field_id.as_str();
            let Some(field_index) = resolve_field_slot(E::MODEL, field_name) else {
                return Err(ExecutionError::UnknownField {
                    field: field_name.to_string(),
                });
            };
            let Some(value) = row.get_value_by_index(field_index) else {
                return Err(ExecutionError::MissingFieldValue {
                    field: field_name.to_string(),
                    index: field_index,
                });
            };

            Ok(value)
        }
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Unary { op, expr } => {
            let operand = eval_expr(expr.as_ref(), row)?;
            eval_unary_expr(*op, operand)
        }
        Expr::Binary { op, left, right } => {
            let left_value = eval_expr(left.as_ref(), row)?;
            let right_value = eval_expr(right.as_ref(), row)?;

            eval_binary_expr(*op, left_value, right_value)
        }
        Expr::Aggregate(aggregate) => Err(ExecutionError::AggregateNotEvaluable {
            kind: format!("{:?}", aggregate.kind()),
        }),
        Expr::Alias { expr, .. } => eval_expr(expr.as_ref(), row),
    }
}

/// Evaluate one projection expression against one grouped output row view.
pub(in crate::db::executor) fn eval_expr_grouped(
    expr: &Expr,
    grouped_row: &GroupedRowView<'_>,
) -> Result<Value, ExecutionError> {
    match expr {
        Expr::Field(field_id) => {
            let Some(group_field_offset) =
                resolve_group_field_offset(grouped_row, field_id.as_str())
            else {
                return Err(ExecutionError::UnknownField {
                    field: field_id.as_str().to_string(),
                });
            };
            let Some(value) = grouped_row.key_values.get(group_field_offset) else {
                return Err(ExecutionError::MissingFieldValue {
                    field: field_id.as_str().to_string(),
                    index: group_field_offset,
                });
            };

            Ok(value.clone())
        }
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Unary { op, expr } => {
            let operand = eval_expr_grouped(expr.as_ref(), grouped_row)?;
            eval_unary_expr(*op, operand)
        }
        Expr::Binary { op, left, right } => {
            let left_value = eval_expr_grouped(left.as_ref(), grouped_row)?;
            let right_value = eval_expr_grouped(right.as_ref(), grouped_row)?;

            eval_binary_expr(*op, left_value, right_value)
        }
        Expr::Aggregate(aggregate_expr) => {
            let Some(aggregate_index) =
                resolve_grouped_aggregate_index(grouped_row, aggregate_expr)
            else {
                return Err(ExecutionError::UnknownGroupedAggregateExpression {
                    kind: format!("{:?}", aggregate_expr.kind()),
                    target_field: aggregate_expr.target_field().map(str::to_string),
                    distinct: aggregate_expr.is_distinct(),
                });
            };
            let Some(value) = grouped_row.aggregate_values.get(aggregate_index) else {
                return Err(ExecutionError::MissingGroupedAggregateValue {
                    aggregate_index,
                    aggregate_count: grouped_row.aggregate_values.len(),
                });
            };

            Ok(value.clone())
        }
        Expr::Alias { expr, .. } => eval_expr_grouped(expr.as_ref(), grouped_row),
    }
}

/// Evaluate one grouped projection spec into ordered projected values.
pub(in crate::db::executor) fn evaluate_grouped_projection_values(
    projection: &ProjectionSpec,
    grouped_row: &GroupedRowView<'_>,
) -> Result<Vec<Value>, ExecutionError> {
    let mut projected_values = Vec::with_capacity(projection.len());
    for field in projection.fields() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                projected_values.push(eval_expr_grouped(expr, grouped_row)?);
            }
        }
    }

    Ok(projected_values)
}

fn resolve_group_field_offset(grouped_row: &GroupedRowView<'_>, field_name: &str) -> Option<usize> {
    for (offset, group_field) in grouped_row.group_fields.iter().enumerate() {
        if group_field.field() == field_name {
            return Some(offset);
        }
    }

    None
}

fn resolve_grouped_aggregate_index(
    grouped_row: &GroupedRowView<'_>,
    aggregate_expr: &AggregateExpr,
) -> Option<usize> {
    for (index, candidate) in grouped_row.aggregate_exprs.iter().enumerate() {
        if candidate == aggregate_expr {
            return Some(index);
        }
    }

    None
}

fn eval_unary_expr(op: UnaryOp, value: Value) -> Result<Value, ExecutionError> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }

    match op {
        UnaryOp::Neg => {
            let Some(decimal) = coerce_numeric_decimal(&value) else {
                return Err(ExecutionError::InvalidUnaryOperand {
                    op: unary_op_name(op).to_string(),
                    found: Box::new(value),
                });
            };

            Ok(Value::Decimal(Decimal::ZERO - decimal))
        }
        UnaryOp::Not => {
            let Value::Bool(v) = value else {
                return Err(ExecutionError::InvalidUnaryOperand {
                    op: unary_op_name(op).to_string(),
                    found: Box::new(value),
                });
            };

            Ok(Value::Bool(!v))
        }
    }
}

fn eval_binary_expr(op: BinaryOp, left: Value, right: Value) -> Result<Value, ExecutionError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }

    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
            eval_numeric_binary_expr(op, left, right)
        }
        BinaryOp::And | BinaryOp::Or => eval_boolean_binary_expr(op, left, right),
        BinaryOp::Eq | BinaryOp::Ne => eval_equality_binary_expr(op, left, right),
        BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Gt | BinaryOp::Gte => {
            eval_compare_binary_expr(op, left, right)
        }
    }
}

fn eval_numeric_binary_expr(
    op: BinaryOp,
    left: Value,
    right: Value,
) -> Result<Value, ExecutionError> {
    let Some(arithmetic_op) = numeric_arithmetic_op(op) else {
        return Err(ExecutionError::InvalidBinaryOperands {
            op: binary_op_name(op).to_string(),
            left: Box::new(left),
            right: Box::new(right),
        });
    };
    let Some(result) = apply_numeric_arithmetic(arithmetic_op, &left, &right) else {
        return Err(ExecutionError::InvalidBinaryOperands {
            op: binary_op_name(op).to_string(),
            left: Box::new(left),
            right: Box::new(right),
        });
    };

    Ok(Value::Decimal(result))
}

fn eval_boolean_binary_expr(
    op: BinaryOp,
    left: Value,
    right: Value,
) -> Result<Value, ExecutionError> {
    let (Value::Bool(left_bool), Value::Bool(right_bool)) = (&left, &right) else {
        return Err(ExecutionError::InvalidBinaryOperands {
            op: binary_op_name(op).to_string(),
            left: Box::new(left),
            right: Box::new(right),
        });
    };

    let result = match op {
        BinaryOp::And => *left_bool && *right_bool,
        BinaryOp::Or => *left_bool || *right_bool,
        BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => unreachable!("boolean binary evaluator called with non-boolean op"),
    };

    Ok(Value::Bool(result))
}

fn eval_equality_binary_expr(
    op: BinaryOp,
    left: Value,
    right: Value,
) -> Result<Value, ExecutionError> {
    let are_equal = if left.supports_numeric_coercion() || right.supports_numeric_coercion() {
        let Some(are_equal) = compare_numeric_eq(&left, &right) else {
            return Err(ExecutionError::InvalidBinaryOperands {
                op: binary_op_name(op).to_string(),
                left: Box::new(left),
                right: Box::new(right),
            });
        };

        are_equal
    } else {
        left == right
    };

    let result = match op {
        BinaryOp::Eq => are_equal,
        BinaryOp::Ne => !are_equal,
        BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => unreachable!("equality evaluator called with non-equality op"),
    };

    Ok(Value::Bool(result))
}

fn eval_compare_binary_expr(
    op: BinaryOp,
    left: Value,
    right: Value,
) -> Result<Value, ExecutionError> {
    let ordering = compare_ordering(op, &left, &right).ok_or_else(|| {
        ExecutionError::InvalidBinaryOperands {
            op: binary_op_name(op).to_string(),
            left: Box::new(left.clone()),
            right: Box::new(right.clone()),
        }
    })?;

    let result = match op {
        BinaryOp::Lt => ordering.is_lt(),
        BinaryOp::Lte => ordering.is_le(),
        BinaryOp::Gt => ordering.is_gt(),
        BinaryOp::Gte => ordering.is_ge(),
        BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Eq
        | BinaryOp::Ne => unreachable!("comparison evaluator called with non-comparison op"),
    };

    Ok(Value::Bool(result))
}

fn compare_ordering(op: BinaryOp, left: &Value, right: &Value) -> Option<Ordering> {
    let _ = op;
    if left.supports_numeric_coercion() && right.supports_numeric_coercion() {
        return compare_numeric_order(left, right);
    }

    Value::strict_order_cmp(left, right)
}

const fn numeric_arithmetic_op(op: BinaryOp) -> Option<NumericArithmeticOp> {
    match op {
        BinaryOp::Add => Some(NumericArithmeticOp::Add),
        BinaryOp::Sub => Some(NumericArithmeticOp::Sub),
        BinaryOp::Mul => Some(NumericArithmeticOp::Mul),
        BinaryOp::Div => Some(NumericArithmeticOp::Div),
        BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => None,
    }
}

fn project_rows_from_projection<E>(
    projection: &ProjectionSpec,
    rows: &[(Id<E>, E)],
) -> Result<Vec<ProjectedRow<E>>, ExecutionError>
where
    E: EntityKind + EntityValue,
{
    let mut projected_rows = Vec::with_capacity(rows.len());
    for (id, entity) in rows {
        let mut values = Vec::with_capacity(projection.len());
        for field in projection.fields() {
            match field {
                ProjectionField::Scalar { expr, .. } => {
                    values.push(eval_expr(expr, entity)?);
                }
            }
        }
        projected_rows.push(ProjectedRow::new(*id, values));
    }

    Ok(projected_rows)
}

fn projection_is_model_identity<E>(projection: &ProjectionSpec) -> bool
where
    E: EntityKind,
{
    if projection.len() != E::MODEL.fields.len() {
        return false;
    }

    for (field_model, projected_field) in E::MODEL.fields.iter().zip(projection.fields()) {
        match projected_field {
            ProjectionField::Scalar {
                expr: Expr::Field(field_id),
                alias: None,
            } if field_id.as_str() == field_model.name => {}
            ProjectionField::Scalar { .. } => return false,
        }
    }

    true
}

const fn unary_op_name(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Neg => "neg",
        UnaryOp::Not => "not",
    }
}

const fn binary_op_name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "add",
        BinaryOp::Sub => "sub",
        BinaryOp::Mul => "mul",
        BinaryOp::Div => "div",
        BinaryOp::And => "and",
        BinaryOp::Or => "or",
        BinaryOp::Eq => "eq",
        BinaryOp::Ne => "ne",
        BinaryOp::Lt => "lt",
        BinaryOp::Lte => "lte",
        BinaryOp::Gt => "gt",
        BinaryOp::Gte => "gte",
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::query::{
            builder::aggregate::{count, sum},
            fingerprint::projection_hash_for_test,
            plan::{
                FieldSlot,
                expr::{Alias, Expr, FieldId, ProjectionField, ProjectionSpec},
            },
        },
        db::response::Response,
        model::{field::FieldKind, index::IndexModel},
        traits::EntityValue,
        types::Ulid,
        value::Value,
    };
    use icydb_derive::FieldProjection;
    use serde::{Deserialize, Serialize};
    use std::cmp::Ordering;

    use super::{
        BinaryOp, GroupedRowView, ProjectedRow, eval_expr, eval_expr_grouped,
        evaluate_grouped_projection_values, project_rows_from_projection,
    };

    const EMPTY_INDEX_FIELDS: [&str; 0] = [];
    const EMPTY_INDEX: IndexModel = IndexModel::new(
        "query::executor::projection::idx_empty",
        "query::executor::projection::Store",
        &EMPTY_INDEX_FIELDS,
        false,
    );

    #[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
    struct ProjectionEvalEntity {
        id: Ulid,
        rank: i64,
        flag: bool,
        label: String,
    }

    crate::test_canister! {
        ident = ProjectionEvalCanister,
        commit_memory_id = crate::testing::test_commit_memory_id(),
    }

    crate::test_store! {
        ident = ProjectionEvalStore,
        canister = ProjectionEvalCanister,
    }

    crate::test_entity_schema! {
        ident = ProjectionEvalEntity,
        id = Ulid,
        id_field = id,
        entity_name = "ProjectionEvalEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("rank", FieldKind::Int),
            ("flag", FieldKind::Bool),
            ("label", FieldKind::Text),
        ],
        indexes = [&EMPTY_INDEX],
        store = ProjectionEvalStore,
        canister = ProjectionEvalCanister,
    }

    fn row(
        id: u128,
        rank: i64,
        flag: bool,
    ) -> (crate::types::Id<ProjectionEvalEntity>, ProjectionEvalEntity) {
        let entity = ProjectionEvalEntity {
            id: Ulid::from_u128(id),
            rank,
            flag,
            label: format!("label-{id}"),
        };

        (entity.id(), entity)
    }

    #[test]
    fn eval_expr_supports_arithmetic_projection() {
        let (_, entity) = row(1, 7, true);
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        };

        let value = eval_expr::<ProjectionEvalEntity>(&expr, &entity)
            .expect("numeric projection expression should evaluate");

        assert_eq!(
            value.cmp_numeric(&Value::Int(8)),
            Some(Ordering::Equal),
            "arithmetic projection must preserve numeric semantics",
        );
    }

    #[test]
    fn eval_expr_supports_boolean_projection() {
        let (_, entity) = row(2, 3, true);
        let expr = Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Field(FieldId::new("flag"))),
            right: Box::new(Expr::Literal(Value::Bool(true))),
        };

        let value = eval_expr::<ProjectionEvalEntity>(&expr, &entity)
            .expect("boolean projection expression should evaluate");

        assert_eq!(value, Value::Bool(true));
    }

    #[test]
    fn eval_expr_supports_numeric_equality_widening() {
        let (_, entity) = row(12, 7, true);
        let expr = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Uint(7))),
        };

        let value = eval_expr::<ProjectionEvalEntity>(&expr, &entity)
            .expect("numeric equality should widen");

        assert_eq!(value, Value::Bool(true));
    }

    #[test]
    fn eval_expr_rejects_numeric_and_non_numeric_equality_mix() {
        let (_, entity) = row(13, 7, true);
        let expr = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Field(FieldId::new("label"))),
        };

        let err = eval_expr::<ProjectionEvalEntity>(&expr, &entity)
            .expect_err("mixed numeric/non-numeric equality should fail invariant checks");
        assert!(matches!(
            err,
            crate::db::executor::load::projection::ExecutionError::InvalidBinaryOperands { op, .. }
                if op == "eq"
        ));
    }

    #[test]
    fn eval_expr_propagates_null_values() {
        let (_, entity) = row(3, 5, false);
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Null)),
        };

        let value = eval_expr::<ProjectionEvalEntity>(&expr, &entity)
            .expect("null propagation should remain deterministic");

        assert_eq!(value, Value::Null);
    }

    #[test]
    fn eval_expr_alias_wrapper_is_semantic_no_op() {
        let (_, entity) = row(4, 11, true);
        let plain = Expr::Field(FieldId::new("rank"));
        let aliased = Expr::Alias {
            expr: Box::new(Expr::Field(FieldId::new("rank"))),
            name: Alias::new("rank_alias"),
        };

        let plain_value = eval_expr::<ProjectionEvalEntity>(&plain, &entity)
            .expect("plain field expression should evaluate");
        let alias_value = eval_expr::<ProjectionEvalEntity>(&aliased, &entity)
            .expect("aliased expression should evaluate identically");

        assert_eq!(plain_value, alias_value);
    }

    #[test]
    fn projection_hash_alias_identity_matches_evaluated_projection_output() {
        let row = row(5, 42, true);
        let base_projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("rank")),
            alias: None,
        }]);
        let aliased_projection =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Alias {
                    expr: Box::new(Expr::Field(FieldId::new("rank"))),
                    name: Alias::new("rank_expr"),
                },
                alias: Some(Alias::new("rank_out")),
            }]);

        let base_rows: Vec<ProjectedRow<ProjectionEvalEntity>> =
            project_rows_from_projection(&base_projection, std::slice::from_ref(&row))
                .expect("base projection should evaluate");
        let aliased_rows: Vec<ProjectedRow<ProjectionEvalEntity>> =
            project_rows_from_projection(&aliased_projection, std::slice::from_ref(&row))
                .expect("aliased projection should evaluate");

        assert_eq!(
            projection_hash_for_test(&base_projection),
            projection_hash_for_test(&aliased_projection),
            "alias-insensitive projection hash must align with evaluator output identity",
        );
        assert_eq!(
            base_rows[0].values(),
            aliased_rows[0].values(),
            "alias wrappers must not affect evaluated projection values",
        );
        assert_eq!(
            base_rows[0].id(),
            aliased_rows[0].id(),
            "projection identity checks must preserve source row identity",
        );
    }

    #[test]
    fn scalar_arithmetic_projection_returns_computed_values() {
        let rows = [row(7, 41, true)];
        let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Field(FieldId::new("rank"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            },
            alias: None,
        }]);

        let projected = project_rows_from_projection(&projection, rows.as_slice())
            .expect("arithmetic scalar projection should evaluate");
        let only_value = projected[0]
            .values()
            .first()
            .expect("projection should emit one value");
        assert_eq!(
            only_value.cmp_numeric(&Value::Int(42)),
            Some(Ordering::Equal),
            "arithmetic scalar projection should emit computed expression result",
        );
    }

    #[test]
    fn ordering_is_preserved_when_projecting_computed_fields() {
        // Input rows are already in execution order; projection must preserve that
        // row ordering while evaluating computed scalar expressions.
        let rows = [row(8, 1, true), row(9, 2, true), row(10, 3, true)];
        let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Field(FieldId::new("rank"))),
                right: Box::new(Expr::Literal(Value::Int(100))),
            },
            alias: None,
        }]);

        let projected = project_rows_from_projection(&projection, rows.as_slice())
            .expect("computed projection should evaluate deterministically");

        let projected_ids: Vec<_> = projected.iter().map(ProjectedRow::id).collect();
        let expected_ids: Vec<_> = rows.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            projected_ids, expected_ids,
            "projection phase must preserve established row ordering",
        );
        let expected_values = [Value::Int(101), Value::Int(102), Value::Int(103)];
        for (actual, expected) in projected
            .iter()
            .map(|row| row.values()[0].clone())
            .zip(expected_values)
        {
            assert_eq!(
                actual.cmp_numeric(&expected),
                Some(Ordering::Equal),
                "computed projection values must align with preserved row order",
            );
        }
    }

    #[test]
    fn grouped_projection_arithmetic_over_group_field_evaluates() {
        let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
        let aggregate_exprs: [crate::db::query::builder::aggregate::AggregateExpr; 0] = [];
        let grouped_row = GroupedRowView::new(
            &[Value::Int(7)],
            &[],
            group_fields.as_slice(),
            aggregate_exprs.as_slice(),
        );
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(2))),
        };

        let value =
            eval_expr_grouped(&expr, &grouped_row).expect("grouped arithmetic should evaluate");
        assert_eq!(
            value.cmp_numeric(&Value::Int(9)),
            Some(Ordering::Equal),
            "grouped arithmetic projection should evaluate over grouped keys",
        );
    }

    #[test]
    fn grouped_projection_supports_numeric_equality_widening() {
        let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
        let aggregate_exprs: [crate::db::query::builder::aggregate::AggregateExpr; 0] = [];
        let grouped_row = GroupedRowView::new(
            &[Value::Int(7)],
            &[],
            group_fields.as_slice(),
            aggregate_exprs.as_slice(),
        );
        let expr = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Uint(7))),
        };

        let value = eval_expr_grouped(&expr, &grouped_row)
            .expect("grouped numeric equality should widen deterministically");
        assert_eq!(value, Value::Bool(true));
    }

    #[test]
    fn grouped_projection_rejects_numeric_and_non_numeric_equality_mix() {
        let group_fields = [
            FieldSlot::from_parts_for_test(1, "rank"),
            FieldSlot::from_parts_for_test(2, "label"),
        ];
        let aggregate_exprs: [crate::db::query::builder::aggregate::AggregateExpr; 0] = [];
        let key_values = [Value::Int(7), Value::Text("label-7".to_string())];
        let grouped_row = GroupedRowView::new(
            key_values.as_slice(),
            &[],
            group_fields.as_slice(),
            aggregate_exprs.as_slice(),
        );
        let expr = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Field(FieldId::new("label"))),
        };

        let err = eval_expr_grouped(&expr, &grouped_row)
            .expect_err("grouped mixed numeric/non-numeric equality should fail");
        assert!(matches!(
            err,
            crate::db::executor::load::projection::ExecutionError::InvalidBinaryOperands { op, .. }
                if op == "eq"
        ));
    }

    #[test]
    fn grouped_projection_mixing_aggregate_and_arithmetic_evaluates() {
        let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
        let aggregate_exprs = [sum("rank")];
        let grouped_row = GroupedRowView::new(
            &[Value::Int(7)],
            &[Value::Int(40)],
            group_fields.as_slice(),
            aggregate_exprs.as_slice(),
        );
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Aggregate(sum("rank"))),
            right: Box::new(Expr::Literal(Value::Int(2))),
        };

        let value = eval_expr_grouped(&expr, &grouped_row)
            .expect("grouped aggregate arithmetic projection should evaluate");
        assert_eq!(
            value.cmp_numeric(&Value::Int(42)),
            Some(Ordering::Equal),
            "grouped projections must evaluate aggregate+scalar arithmetic deterministically",
        );
    }

    #[test]
    fn grouped_projection_alias_wrapping_is_semantic_no_op() {
        let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
        let aggregate_exprs = [sum("rank")];
        let grouped_row = GroupedRowView::new(
            &[Value::Int(7)],
            &[Value::Int(40)],
            group_fields.as_slice(),
            aggregate_exprs.as_slice(),
        );
        let plain = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Aggregate(sum("rank"))),
            right: Box::new(Expr::Literal(Value::Int(2))),
        };
        let aliased = Expr::Alias {
            expr: Box::new(Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Aggregate(sum("rank"))),
                right: Box::new(Expr::Literal(Value::Int(2))),
            }),
            name: Alias::new("sum_plus_two"),
        };

        let plain_value =
            eval_expr_grouped(&plain, &grouped_row).expect("plain grouped expression should work");
        let alias_value = eval_expr_grouped(&aliased, &grouped_row)
            .expect("aliased grouped expression should work");
        assert_eq!(
            plain_value, alias_value,
            "grouped alias wrapping must not change expression values",
        );
    }

    #[test]
    fn grouped_projection_multiple_aggregate_expression_order_is_preserved() {
        let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
        let aggregate_exprs = [count(), sum("rank")];
        let grouped_row = GroupedRowView::new(
            &[Value::Int(7)],
            &[Value::Uint(3), Value::Int(40)],
            group_fields.as_slice(),
            aggregate_exprs.as_slice(),
        );
        let projection = ProjectionSpec::from_fields_for_test(vec![
            ProjectionField::Scalar {
                expr: Expr::Aggregate(sum("rank")),
                alias: Some(Alias::new("sum_rank")),
            },
            ProjectionField::Scalar {
                expr: Expr::Aggregate(count()),
                alias: Some(Alias::new("count_all")),
            },
            ProjectionField::Scalar {
                expr: Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Aggregate(count())),
                    right: Box::new(Expr::Literal(Value::Int(1))),
                },
                alias: Some(Alias::new("count_plus_one")),
            },
        ]);

        let values = evaluate_grouped_projection_values(&projection, &grouped_row)
            .expect("grouped projection vector should evaluate");

        assert_eq!(
            values.len(),
            3,
            "grouped projection must preserve declared field count",
        );
        assert_eq!(
            values[0].cmp_numeric(&Value::Int(40)),
            Some(Ordering::Equal),
            "first grouped projection output must follow projection declaration order",
        );
        assert_eq!(
            values[1].cmp_numeric(&Value::Uint(3)),
            Some(Ordering::Equal),
            "second grouped projection output must follow projection declaration order",
        );
        assert_eq!(
            values[2].cmp_numeric(&Value::Int(4)),
            Some(Ordering::Equal),
            "third grouped projection output must evaluate computed aggregate expression in order",
        );
    }

    #[test]
    fn grouped_projection_ordering_preserves_input_group_order() {
        let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
        let aggregate_exprs = [sum("rank")];
        let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Aggregate(sum("rank"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            },
            alias: Some(Alias::new("sum_plus_one")),
        }]);
        let grouped_inputs = vec![
            (vec![Value::Int(1)], vec![Value::Int(10)]),
            (vec![Value::Int(2)], vec![Value::Int(20)]),
            (vec![Value::Int(3)], vec![Value::Int(30)]),
        ];
        let mut observed = Vec::new();
        for (key_values, aggregate_values) in grouped_inputs {
            let row_view = GroupedRowView::new(
                key_values.as_slice(),
                aggregate_values.as_slice(),
                group_fields.as_slice(),
                aggregate_exprs.as_slice(),
            );
            let evaluated = evaluate_grouped_projection_values(&projection, &row_view)
                .expect("grouped projection should evaluate per-row");
            observed.push(evaluated[0].clone());
        }

        let expected = [Value::Int(11), Value::Int(21), Value::Int(31)];
        for (actual, expected_value) in observed.into_iter().zip(expected) {
            assert_eq!(
                actual.cmp_numeric(&expected_value),
                Some(Ordering::Equal),
                "grouped projection evaluation order must preserve grouped row order",
            );
        }
    }

    #[test]
    fn response_exposes_projected_rows_payload() {
        let row = row(6, 19, true);
        let projected = vec![ProjectedRow::new(row.0, vec![Value::Int(row.1.rank)])];
        let response = Response::from_rows_with_projection(vec![row.clone()], Some(projected));

        let projected_rows = response
            .projected_rows()
            .expect("response should expose projection payload when provided");
        assert_eq!(
            projected_rows.len(),
            1,
            "response projected payload should preserve row cardinality"
        );
        assert_eq!(
            projected_rows[0].id(),
            row.0,
            "response projected payload should preserve row identity"
        );
        assert_eq!(
            projected_rows[0].values(),
            &[Value::Int(19)],
            "response projected payload should preserve projection value ordering",
        );
    }
}
