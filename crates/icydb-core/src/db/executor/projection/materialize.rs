//! Module: db::executor::projection::materialize
//! Responsibility: module-local ownership and contracts for db::executor::projection::materialize.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::{ExecutablePlan, pipeline::contracts::LoadExecutor},
        query::plan::{
            AccessPlannedQuery,
            expr::{Expr, ProjectionField, ProjectionSpec},
        },
        response::{ProjectedRow, ProjectionResponse},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
    value::Value,
};

use crate::db::executor::projection::{
    eval::{ProjectionEvalError, eval_expr_grouped, eval_expr_with_slot_reader},
    grouped::GroupedRowView,
};

// Planner-owned projection strategy selected once before scalar row traversal.
// Keeps load execution materialization as a strategy call instead of policy
// branching at call sites.
enum ScalarProjectionExecutionStrategy<'a> {
    Identity,
    Materialized(&'a ProjectionSpec),
}

impl<'a> ScalarProjectionExecutionStrategy<'a> {
    fn for_projection<E>(projection: &'a ProjectionSpec) -> Self
    where
        E: EntityKind,
    {
        if projection_is_model_identity::<E>(projection) {
            Self::Identity
        } else {
            Self::Materialized(projection)
        }
    }

    fn materialize_rows<E>(
        self,
        rows: &[(Id<E>, E)],
    ) -> Result<Option<Vec<ProjectedRow<E>>>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match self {
            Self::Identity => Ok(None),
            Self::Materialized(projection) => {
                let projected = project_rows_from_projection::<E>(projection, rows)
                    .map_err(|err| crate::db::error::query_invalid_logical_plan(err.to_string()))?;
                Ok(Some(projected))
            }
        }
    }
}

///
/// ShapePreservingProjection
///
/// Marker trait for scalar projection contracts that must preserve one-to-one
/// row identity and ordering relative to post-access materialized rows.
///

pub(in crate::db::executor::projection) trait ShapePreservingProjection {
    /// Borrow canonical planner projection semantics.
    fn as_projection_spec(&self) -> &ProjectionSpec;
}

impl ShapePreservingProjection for ProjectionSpec {
    fn as_projection_spec(&self) -> &ProjectionSpec {
        self
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute one scalar load plan and return projection-shaped response rows.
    pub(in crate::db) fn execute_projection(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<ProjectionResponse<E>, InternalError> {
        // Phase 1: derive projection semantics from the planned query contract.
        let planned = plan.into_inner();
        let projection = planned.projection_spec(E::MODEL);

        // Phase 2: execute canonical scalar load to preserve existing route/runtime semantics.
        let rows = self.execute(ExecutablePlan::new(planned))?;
        let rows = rows
            .rows()
            .into_iter()
            .map(crate::db::response::Row::into_parts)
            .collect::<Vec<_>>();

        // Phase 3: materialize projection payloads in declaration order.
        let projected = project_rows_from_projection::<E>(&projection, rows.as_slice())
            .map_err(|err| crate::db::error::query_invalid_logical_plan(err.to_string()))?;

        Ok(ProjectionResponse::new(projected))
    }

    /// Evaluate scalar projection semantics over materialized rows when the
    /// projection is no longer identity (`SELECT *`).
    pub(in crate::db::executor) fn project_materialized_rows_if_needed(
        plan: &AccessPlannedQuery<E::Key>,
        rows: &[(Id<E>, E)],
    ) -> Result<Option<Vec<ProjectedRow<E>>>, InternalError> {
        let projection = plan.projection_spec(E::MODEL);
        let strategy = ScalarProjectionExecutionStrategy::for_projection::<E>(&projection);

        strategy.materialize_rows(rows)
    }
}

/// Evaluate one grouped projection spec into ordered projected values.
pub(in crate::db::executor) fn evaluate_grouped_projection_values(
    projection: &ProjectionSpec,
    grouped_row: &GroupedRowView<'_>,
) -> Result<Vec<Value>, ProjectionEvalError> {
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

pub(in crate::db::executor::projection) fn project_rows_from_projection<E>(
    projection: &impl ShapePreservingProjection,
    rows: &[(Id<E>, E)],
) -> Result<Vec<ProjectedRow<E>>, ProjectionEvalError>
where
    E: EntityKind + EntityValue,
{
    let projection = projection.as_projection_spec();
    let mut projected_rows = Vec::with_capacity(rows.len());
    for (id, entity) in rows {
        let mut values = Vec::with_capacity(projection.len());
        let mut read_slot = |slot| entity.get_value_by_index(slot);
        for field in projection.fields() {
            match field {
                ProjectionField::Scalar { expr, .. } => {
                    values.push(eval_expr_with_slot_reader(expr, E::MODEL, &mut read_slot)?);
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
