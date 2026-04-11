//! Module: db::executor::projection::tests
//! Covers scalar and grouped projection evaluation behavior.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod grouped;
mod materialize;
mod scalar;

#[cfg(feature = "sql")]
use crate::db::response::ProjectedRow;
use crate::{
    db::query::{
        builder::aggregate::{count, sum},
        plan::{
            FieldSlot, GroupedAggregateExecutionSpec, GroupedAggregateProjectionSpec,
            expr::{Alias, BinaryOp, Expr, FieldId, ProjectionField, ProjectionSpec},
        },
    },
    db::{
        codec::serialize_row_payload,
        data::{
            CanonicalSlotReader, RawRow, SlotReader, StructuralSlotReader,
            encode_persisted_scalar_slot_payload,
        },
        executor::terminal::RowLayout,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::{
        field::{FieldKind, FieldModel},
        index::IndexModel,
    },
    serialize::serialize,
    traits::{EntitySchema, EntityValue, FieldProjection as _},
    types::Ulid,
    value::Value,
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[cfg(feature = "sql")]
use super::project_rows_from_projection;
use super::{
    GroupedRowView, ProjectionEvalError, compile_grouped_projection_expr,
    compile_grouped_projection_plan, compile_scalar_projection_expr,
    eval_canonical_scalar_projection_expr, eval_grouped_projection_expr,
    eval_scalar_projection_expr, evaluate_grouped_projection_values,
};

const EMPTY_INDEX_FIELDS: [&str; 0] = [];
const EMPTY_INDEX: IndexModel = IndexModel::generated(
    "query::executor::projection::idx_empty",
    "query::executor::projection::Store",
    &EMPTY_INDEX_FIELDS,
    false,
);

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
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
    entity_tag = crate::testing::PROJECTION_EVAL_ENTITY_TAG,
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

#[cfg(feature = "sql")]
pub(in crate::db::executor) fn projection_eval_row_layout_for_materialize_tests() -> RowLayout {
    RowLayout::from_model(ProjectionEvalEntity::MODEL)
}

fn resolve_field_slot_from_fields(fields: &[FieldModel], field_name: &str) -> Option<usize> {
    fields.iter().position(|field| field.name() == field_name)
}

/// Evaluate one projection expression through one runtime slot reader.
fn eval_expr_with_slot_reader(
    expr: &Expr,
    fields: &[FieldModel],
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> Result<Value, ProjectionEvalError> {
    match expr {
        Expr::Field(field_id) => {
            let field_name = field_id.as_str();
            let Some(field_index) = resolve_field_slot_from_fields(fields, field_name) else {
                return Err(ProjectionEvalError::UnknownField {
                    field: field_name.to_string(),
                });
            };
            let Some(value) = read_slot(field_index) else {
                return Err(ProjectionEvalError::MissingFieldValue {
                    field: field_name.to_string(),
                    index: field_index,
                });
            };

            Ok(value)
        }
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Unary { op, expr } => {
            let operand = eval_expr_with_slot_reader(expr.as_ref(), fields, read_slot)?;
            super::eval_unary_expr(*op, &operand)
        }
        Expr::Binary { op, left, right } => {
            let left_value = eval_expr_with_slot_reader(left.as_ref(), fields, read_slot)?;
            let right_value = eval_expr_with_slot_reader(right.as_ref(), fields, read_slot)?;

            super::eval_binary_expr(*op, &left_value, &right_value)
        }
        Expr::Aggregate(aggregate) => Err(ProjectionEvalError::AggregateNotEvaluable {
            kind: format!("{:?}", aggregate.kind()),
        }),
        Expr::Alias { expr, .. } => eval_expr_with_slot_reader(expr.as_ref(), fields, read_slot),
    }
}

/// Evaluate one projection expression through one required-value reader on the
/// canonical structural row path.
fn eval_expr_with_required_value_reader(
    expr: &Expr,
    fields: &[FieldModel],
    read_slot: &mut dyn FnMut(usize) -> Result<Value, InternalError>,
) -> Result<Value, InternalError> {
    match expr {
        Expr::Field(field_id) => {
            let field_name = field_id.as_str();
            let Some(field_index) = resolve_field_slot_from_fields(fields, field_name) else {
                return Err(ProjectionEvalError::UnknownField {
                    field: field_name.to_string(),
                }
                .into_invalid_logical_plan_internal_error());
            };

            read_slot(field_index)
        }
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Unary { op, expr } => {
            let operand = eval_expr_with_required_value_reader(expr.as_ref(), fields, read_slot)?;
            super::eval_unary_expr(*op, &operand)
                .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)
        }
        Expr::Binary { op, left, right } => {
            let left_value =
                eval_expr_with_required_value_reader(left.as_ref(), fields, read_slot)?;
            let right_value =
                eval_expr_with_required_value_reader(right.as_ref(), fields, read_slot)?;

            super::eval_binary_expr(*op, &left_value, &right_value)
                .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)
        }
        Expr::Aggregate(aggregate) => Err(ProjectionEvalError::AggregateNotEvaluable {
            kind: format!("{:?}", aggregate.kind()),
        }
        .into_invalid_logical_plan_internal_error()),
        Expr::Alias { expr, .. } => {
            eval_expr_with_required_value_reader(expr.as_ref(), fields, read_slot)
        }
    }
}

/// Evaluate one projection expression against one grouped output row view.
fn eval_expr_grouped(
    expr: &Expr,
    grouped_row: &GroupedRowView<'_>,
) -> Result<Value, ProjectionEvalError> {
    let compiled = compile_grouped_projection_expr(
        expr,
        grouped_row.group_fields,
        grouped_row.aggregate_execution_specs,
    )?;

    eval_grouped_projection_expr(&compiled, grouped_row)
}

fn eval_expr_for_row(
    expr: &Expr,
    row: &ProjectionEvalEntity,
) -> Result<Value, crate::db::executor::projection::ProjectionEvalError> {
    eval_expr_with_slot_reader(expr, ProjectionEvalEntity::MODEL.fields(), &mut |slot| {
        row.get_value_by_index(slot)
    })
}

fn eval_scalar_expr_for_row(
    expr: &Expr,
    row: &ProjectionEvalEntity,
) -> Result<Value, InternalError> {
    let compiled = compile_scalar_projection_expr(ProjectionEvalEntity::MODEL, expr)
        .expect("expression should compile onto scalar projection seam");
    let raw_row = RawRow::from_entity(row).expect("persisted row should encode");
    let mut row_fields = StructuralSlotReader::from_raw_row(&raw_row, ProjectionEvalEntity::MODEL)
        .expect("persisted row should decode structurally");

    eval_scalar_projection_expr(&compiled, &mut row_fields)
}

fn grouped_execution_specs<const N: usize>(
    aggregate_exprs: [crate::db::query::builder::aggregate::AggregateExpr; N],
) -> [GroupedAggregateExecutionSpec; N] {
    aggregate_exprs.map(|aggregate_expr| {
        GroupedAggregateExecutionSpec::from_projection_spec_with_model(
            ProjectionEvalEntity::MODEL,
            &GroupedAggregateProjectionSpec::from_aggregate_expr(&aggregate_expr),
        )
        .expect("grouped execution spec should lower from aggregate expression")
    })
}

///
/// ProjectionMissingDeclaredSlotReader
///
/// ProjectionMissingDeclaredSlotReader
///
/// ProjectionMissingDeclaredSlotReader simulates one canonical structural row
/// whose declared slots are absent so projection evaluators can prove they
/// preserve corruption diagnostics instead of flattening them into
/// invalid-logical-plan failures.
///

struct ProjectionMissingDeclaredSlotReader;

impl SlotReader for ProjectionMissingDeclaredSlotReader {
    fn model(&self) -> &'static crate::model::entity::EntityModel {
        ProjectionEvalEntity::MODEL
    }

    fn has(&self, _slot: usize) -> bool {
        false
    }

    fn get_bytes(&self, _slot: usize) -> Option<&[u8]> {
        None
    }

    fn get_scalar(
        &self,
        _slot: usize,
    ) -> Result<Option<crate::db::data::ScalarSlotValueRef<'_>>, InternalError> {
        Ok(None)
    }

    fn get_value(&mut self, _slot: usize) -> Result<Option<Value>, InternalError> {
        panic!("projection missing-slot test reader should not route through get_value")
    }
}

impl CanonicalSlotReader for ProjectionMissingDeclaredSlotReader {}
