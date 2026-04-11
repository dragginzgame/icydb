//! Module: db::executor::projection::materialize
//! Responsibility: shared projection materialization helpers that are used by both structural and typed row flows.
//! Does not own: session-owned structural row shaping or expression evaluation semantics.
//! Boundary: keeps validation, grouped projection materialization, and shared row-walk helpers behind one executor-owned boundary.

#[cfg(test)]
use crate::{
    db::query::plan::expr::ProjectionField,
    db::response::ProjectedRow,
    traits::{EntityKind, EntityValue},
    types::Id,
};
use crate::{
    db::query::plan::{
        AccessPlannedQuery,
        expr::{ProjectionSpec, projection_field_direct_field_name},
    },
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;

#[cfg(test)]
use crate::db::executor::projection::eval::eval_scalar_projection_expr_with_value_reader;
use crate::db::executor::projection::eval::{
    ProjectionEvalError, ScalarProjectionExpr,
    eval_canonical_scalar_projection_expr_with_required_value_reader_cow,
    eval_scalar_projection_expr_with_value_ref_reader,
};
#[cfg(test)]
use crate::db::query::plan::expr::compile_scalar_projection_expr;

///
/// PreparedProjectionPlan
///
/// PreparedProjectionPlan is the executor-owned projection materialization plan
/// shared by typed row projection, slot-row validation, and higher-level
/// structural row shaping. Production paths consume only planner-compiled
/// scalar programs so projection execution no longer carries a generic
/// field-resolve fallback.
///

pub(in crate::db) enum PreparedProjectionPlan {
    Scalar(Vec<ScalarProjectionExpr>),
}

///
/// PreparedProjectionShape
///
/// PreparedProjectionShape is the executor-owned prepared projection contract
/// shared by slot-row validation and higher-level structural row shaping.
/// It freezes the canonical projection semantic spec plus the derived direct
/// slot layouts needed by compiled scalar projection flow.
///

pub(in crate::db) struct PreparedProjectionShape {
    projection: ProjectionSpec,
    prepared: PreparedProjectionPlan,
    projection_is_model_identity: bool,
    direct_projection_field_slots: Option<Vec<(String, usize)>>,
    #[cfg(any(test, feature = "perf-attribution"))]
    projected_slot_mask: Vec<bool>,
}

impl PreparedProjectionShape {
    #[must_use]
    pub(in crate::db) const fn projection(&self) -> &ProjectionSpec {
        &self.projection
    }

    #[must_use]
    pub(in crate::db) const fn prepared(&self) -> &PreparedProjectionPlan {
        &self.prepared
    }

    #[must_use]
    pub(in crate::db) const fn scalar_projection_exprs(&self) -> &[ScalarProjectionExpr] {
        let PreparedProjectionPlan::Scalar(compiled_fields) = self.prepared();

        compiled_fields.as_slice()
    }

    #[must_use]
    pub(in crate::db::executor) const fn projection_is_model_identity(&self) -> bool {
        self.projection_is_model_identity
    }

    #[must_use]
    pub(in crate::db) fn direct_projection_field_slots(&self) -> Option<&[(String, usize)]> {
        self.direct_projection_field_slots.as_deref()
    }

    #[cfg(any(test, feature = "perf-attribution"))]
    #[must_use]
    pub(in crate::db) const fn projected_slot_mask(&self) -> &[bool] {
        self.projected_slot_mask.as_slice()
    }

    /// Build one projection shape directly from test-owned prepared parts.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn from_test_parts(
        projection: ProjectionSpec,
        prepared: PreparedProjectionPlan,
        projection_is_model_identity: bool,
        direct_projection_field_slots: Option<Vec<(String, usize)>>,
        projected_slot_mask: Vec<bool>,
    ) -> Self {
        Self {
            projection,
            prepared,
            projection_is_model_identity,
            direct_projection_field_slots,
            projected_slot_mask,
        }
    }
}

///
/// PreparedSlotProjectionValidation
///
/// PreparedSlotProjectionValidation is the executor-owned slot-row projection
/// validation bundle reused by page kernels and retained-slot row shaping.
/// It freezes the canonical projection semantic spec plus the compiled
/// validation/evaluation shape so execute no longer recomputes that plan at
/// each slot-row validation boundary.
///

pub(in crate::db::executor) type PreparedSlotProjectionValidation = PreparedProjectionShape;

/// Build one executor-owned prepared projection shape from planner-frozen metadata.
#[must_use]
pub(in crate::db) fn prepare_projection_shape_from_plan(
    field_count: usize,
    plan: &AccessPlannedQuery,
) -> PreparedProjectionShape {
    let projection = plan.frozen_projection_spec().clone();
    let prepared = PreparedProjectionPlan::Scalar(
        plan.scalar_projection_plan()
            .expect(
                "scalar execution projection shapes must carry one planner-compiled scalar program",
            )
            .to_vec(),
    );
    let direct_projection_field_slots = direct_projection_field_slots_from_projection(
        &projection,
        plan.frozen_direct_projection_slots(),
    );
    #[cfg(any(test, feature = "perf-attribution"))]
    let projected_slot_mask =
        projected_slot_mask_from_slots(field_count, plan.projected_slot_mask());
    #[cfg(not(any(test, feature = "perf-attribution")))]
    let _ = field_count;

    PreparedProjectionShape {
        projection,
        prepared,
        projection_is_model_identity: plan.projection_is_model_identity(),
        direct_projection_field_slots,
        #[cfg(any(test, feature = "perf-attribution"))]
        projected_slot_mask,
    }
}

/// Validate projection expressions against one row-domain that can expose
/// borrowed slot values by field slot.
pub(in crate::db::executor) fn validate_prepared_projection_row<'a>(
    prepared_validation: &PreparedSlotProjectionValidation,
    read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
) -> Result<(), InternalError> {
    if prepared_validation.projection_is_model_identity() {
        return Ok(());
    }

    let PreparedProjectionPlan::Scalar(compiled_fields) = prepared_validation.prepared();
    for compiled in compiled_fields {
        let _ = eval_scalar_projection_expr_with_value_ref_reader(compiled, read_slot)
            .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)?;
    }

    Ok(())
}

fn direct_projection_field_slots_from_projection(
    projection: &ProjectionSpec,
    direct_projection_slots: Option<&[usize]>,
) -> Option<Vec<(String, usize)>> {
    let direct_projection_slots = direct_projection_slots?;
    let mut field_slots = Vec::with_capacity(direct_projection_slots.len());

    for (field, slot) in projection
        .fields()
        .zip(direct_projection_slots.iter().copied())
    {
        let field_name = projection_field_direct_field_name(field)?;
        field_slots.push((field_name.to_string(), slot));
    }

    Some(field_slots)
}

#[cfg(any(test, feature = "perf-attribution"))]
fn projected_slot_mask_from_slots(field_count: usize, projected_slots: &[bool]) -> Vec<bool> {
    let mut mask = vec![false; field_count];

    for (slot, projected) in projected_slots.iter().copied().enumerate() {
        if projected && let Some(entry) = mask.get_mut(slot) {
            *entry = true;
        }
    }

    mask
}

#[cfg(test)]
pub(in crate::db::executor::projection) fn project_rows_from_projection<E>(
    projection: &ProjectionSpec,
    rows: &[(Id<E>, E)],
) -> Result<Vec<ProjectedRow<E>>, ProjectionEvalError>
where
    E: EntityKind + EntityValue,
{
    let mut compiled_fields = Vec::with_capacity(projection.len());
    for field in projection.fields() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                let compiled = compile_scalar_projection_expr(E::MODEL, expr).expect(
                    "test projection materialization helpers require scalar-compilable expressions",
                );
                compiled_fields.push(compiled);
            }
        }
    }
    let prepared = PreparedProjectionPlan::Scalar(compiled_fields);
    let mut projected_rows = Vec::with_capacity(rows.len());
    for (id, entity) in rows {
        let mut values = Vec::with_capacity(projection.len());
        let mut read_slot = |slot| entity.get_value_by_index(slot);
        visit_prepared_projection_values_with_value_reader(
            &prepared,
            projection,
            &mut read_slot,
            &mut |value| values.push(value),
        )?;
        projected_rows.push(ProjectedRow::new(*id, values));
    }

    Ok(projected_rows)
}

#[cfg(test)]
pub(super) fn visit_prepared_projection_values_with_value_reader(
    prepared: &PreparedProjectionPlan,
    projection: &ProjectionSpec,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
    on_value: &mut dyn FnMut(Value),
) -> Result<(), ProjectionEvalError> {
    let _ = projection;

    let PreparedProjectionPlan::Scalar(compiled_fields) = prepared;
    for compiled in compiled_fields {
        on_value(eval_scalar_projection_expr_with_value_reader(
            compiled, read_slot,
        )?);
    }

    Ok(())
}

// Walk one prepared projection plan through one reader that can borrow slot
// values from retained structural rows until an expression needs ownership.
pub(in crate::db) fn visit_prepared_projection_values_with_required_value_reader_cow<'a>(
    prepared: &'a PreparedProjectionPlan,
    projection: &ProjectionSpec,
    read_slot: &mut dyn FnMut(usize) -> Result<Cow<'a, Value>, InternalError>,
    on_value: &mut dyn FnMut(Value),
) -> Result<(), InternalError> {
    let _ = projection;

    let PreparedProjectionPlan::Scalar(compiled_fields) = prepared;
    for compiled in compiled_fields {
        on_value(
            eval_canonical_scalar_projection_expr_with_required_value_reader_cow(
                compiled, read_slot,
            )?
            .into_owned(),
        );
    }

    Ok(())
}
