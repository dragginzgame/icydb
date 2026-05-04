//! Module: db::executor::projection::materialize::plan
//! Responsibility: prepared projection materialization contracts and validation.
//! Does not own: row loops, structural page dispatch, or DISTINCT execution.
//! Boundary: stores planner-derived projection shape for executor-owned consumers.

use crate::{
    db::{
        executor::projection::eval::{
            ProjectionEvalError, eval_compiled_expr_with_value_ref_reader,
        },
        query::plan::{
            AccessPlannedQuery,
            expr::{CompiledExpr, ProjectionSpec},
        },
    },
    error::InternalError,
    model::{
        entity::EntityModel,
        field::{LeafCodec, ScalarCodec},
    },
    value::Value,
};

///
/// PreparedProjectionPlan
///
/// PreparedProjectionPlan is the executor-owned projection materialization plan
/// shared by typed row projection, slot-row validation, and higher-level
/// structural row shaping. Production paths consume only planner-compiled
/// scalar programs so projection execution no longer carries a generic
/// field-resolve fallback.
///

#[derive(Debug)]
pub(in crate::db) enum PreparedProjectionPlan {
    Scalar(Vec<CompiledExpr>),
}

///
/// PreparedProjectionShape
///
/// PreparedProjectionShape is the executor-owned prepared projection contract
/// shared by slot-row validation and higher-level structural row shaping.
/// It freezes the canonical projection semantic spec plus the derived direct
/// slot layouts needed by compiled scalar projection flow.
///

#[derive(Debug)]
pub(in crate::db) struct PreparedProjectionShape {
    projection: ProjectionSpec,
    prepared: PreparedProjectionPlan,
    projection_is_model_identity: bool,
    retained_slot_direct_projection_field_slots: Option<Vec<(String, usize)>>,
    retained_slot_direct_octet_length_projection_slots: Vec<Option<usize>>,
    data_row_direct_projection_field_slots: Option<Vec<(String, usize)>>,
    #[cfg(any(test, feature = "diagnostics"))]
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
    pub(in crate::db) const fn scalar_projection_exprs(&self) -> &[CompiledExpr] {
        let PreparedProjectionPlan::Scalar(compiled_fields) = self.prepared();

        compiled_fields.as_slice()
    }

    #[must_use]
    pub(in crate::db::executor) const fn projection_is_model_identity(&self) -> bool {
        self.projection_is_model_identity
    }

    #[must_use]
    pub(in crate::db) fn retained_slot_direct_projection_field_slots(
        &self,
    ) -> Option<&[(String, usize)]> {
        self.retained_slot_direct_projection_field_slots.as_deref()
    }

    #[must_use]
    pub(in crate::db) const fn retained_slot_direct_octet_length_projection_slots(
        &self,
    ) -> &[Option<usize>] {
        self.retained_slot_direct_octet_length_projection_slots
            .as_slice()
    }

    #[must_use]
    pub(in crate::db) fn data_row_direct_projection_field_slots(
        &self,
    ) -> Option<&[(String, usize)]> {
        self.data_row_direct_projection_field_slots.as_deref()
    }

    #[cfg(any(test, feature = "diagnostics"))]
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
        retained_slot_direct_projection_field_slots: Option<Vec<(String, usize)>>,
        data_row_direct_projection_field_slots: Option<Vec<(String, usize)>>,
        projected_slot_mask: Vec<bool>,
    ) -> Self {
        Self {
            projection,
            prepared,
            projection_is_model_identity,
            retained_slot_direct_projection_field_slots,
            retained_slot_direct_octet_length_projection_slots: Vec::new(),
            data_row_direct_projection_field_slots,
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

///
/// ProjectionValidationRow
///
/// ProjectionValidationRow is the deliberately narrow row-read contract for
/// shared projection validation only.
/// This abstraction exists to keep retained-slot layout and row payload choice
/// as executor-local representation decisions rather than semantic
/// requirements of the validator itself.
/// It is intentionally not a generic executor row API for predicates,
/// ordering, projection materialization, or adapter rendering.
///

pub(in crate::db::executor) trait ProjectionValidationRow {
    /// Borrow one slot value for projection-expression validation.
    #[must_use]
    fn projection_validation_slot_value(&self, slot: usize) -> Option<&Value>;
}

/// Build one executor-owned prepared projection shape from planner-frozen metadata.
#[must_use]
pub(in crate::db) fn prepare_projection_shape_from_plan(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
) -> PreparedProjectionShape {
    let projection = plan.frozen_projection_spec().clone();
    let compiled_projection = plan
        .scalar_projection_plan()
        .expect("scalar execution projection shapes must carry one planner-compiled scalar program")
        .to_vec();
    let retained_slot_direct_projection_field_slots =
        retained_slot_direct_projection_field_slots_from_projection(
            &projection,
            plan.frozen_direct_projection_slots(),
        );
    let retained_slot_direct_octet_length_projection_slots =
        retained_slot_direct_octet_length_projection_slots_from_compiled(
            model,
            &compiled_projection,
        );
    let data_row_direct_projection_field_slots =
        data_row_direct_projection_field_slots_from_projection(
            &projection,
            plan.frozen_data_row_direct_projection_slots(),
        );
    #[cfg(any(test, feature = "diagnostics"))]
    let projected_slot_mask =
        projected_slot_mask_from_slots(model.fields().len(), plan.projected_slot_mask());

    PreparedProjectionShape {
        projection,
        prepared: PreparedProjectionPlan::Scalar(compiled_projection),
        projection_is_model_identity: plan.projection_is_model_identity(),
        retained_slot_direct_projection_field_slots,
        retained_slot_direct_octet_length_projection_slots,
        data_row_direct_projection_field_slots,
        #[cfg(any(test, feature = "diagnostics"))]
        projected_slot_mask,
    }
}

/// Validate projection expressions against one row-domain that can expose
/// borrowed slot values by field slot.
pub(in crate::db::executor) fn validate_prepared_projection_row(
    prepared_validation: &PreparedSlotProjectionValidation,
    row: &impl ProjectionValidationRow,
) -> Result<(), InternalError> {
    if prepared_validation.projection_is_model_identity() {
        return Ok(());
    }

    let PreparedProjectionPlan::Scalar(compiled_fields) = prepared_validation.prepared();
    for compiled in compiled_fields {
        let mut read_slot = |slot| row.projection_validation_slot_value(slot);
        eval_compiled_expr_with_value_ref_reader(compiled, &mut read_slot)
            .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)?;
    }

    Ok(())
}

// Validate slot availability for FieldPath-bearing expressions without
// evaluating the expression itself. Nested path evaluation requires raw
// persisted bytes and remains owned by the canonical projection executor.
fn retained_slot_direct_projection_field_slots_from_projection(
    projection: &ProjectionSpec,
    direct_projection_slots: Option<&[usize]>,
) -> Option<Vec<(String, usize)>> {
    let direct_projection_slots = direct_projection_slots?;
    let mut field_slots = Vec::with_capacity(direct_projection_slots.len());

    for (field, slot) in projection
        .fields()
        .zip(direct_projection_slots.iter().copied())
    {
        let field_name = field.direct_field_name()?;
        field_slots.push((field_name.to_string(), slot));
    }

    Some(field_slots)
}

fn data_row_direct_projection_field_slots_from_projection(
    projection: &ProjectionSpec,
    direct_projection_slots: Option<&[usize]>,
) -> Option<Vec<(String, usize)>> {
    let direct_projection_slots = direct_projection_slots?;
    let mut field_slots = Vec::with_capacity(direct_projection_slots.len());

    // Phase 1: preserve canonical output order exactly as declared, but allow
    // duplicate source slots because raw-row decoding can borrow the same slot
    // repeatedly without the retained-slot `take()` constraint.
    for (field, slot) in projection
        .fields()
        .zip(direct_projection_slots.iter().copied())
    {
        let field_name = field.direct_field_name()?;
        field_slots.push((field_name.to_string(), slot));
    }

    Some(field_slots)
}

fn retained_slot_direct_octet_length_projection_slots_from_compiled(
    model: &EntityModel,
    compiled_projection: &[CompiledExpr],
) -> Vec<Option<usize>> {
    let mut slots = Vec::with_capacity(compiled_projection.len());
    let mut has_direct_octet_length = false;

    for expr in compiled_projection {
        let slot = expr.direct_octet_length_slot().and_then(|(slot, _field)| {
            slot_uses_scalar_byte_length_codec(model, slot).then_some(slot)
        });
        has_direct_octet_length |= slot.is_some();
        slots.push(slot);
    }

    if has_direct_octet_length {
        slots
    } else {
        Vec::new()
    }
}

fn slot_uses_scalar_byte_length_codec(model: &EntityModel, slot: usize) -> bool {
    model.fields().get(slot).is_some_and(|field| {
        matches!(
            field.leaf_codec(),
            LeafCodec::Scalar(ScalarCodec::Blob | ScalarCodec::Text)
        )
    })
}

#[cfg(any(test, feature = "diagnostics"))]
fn projected_slot_mask_from_slots(field_count: usize, projected_slots: &[bool]) -> Vec<bool> {
    let mut mask = vec![false; field_count];

    for (slot, projected) in projected_slots.iter().copied().enumerate() {
        if projected && let Some(entry) = mask.get_mut(slot) {
            *entry = true;
        }
    }

    mask
}
