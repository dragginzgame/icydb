//! Module: db::executor::projection::materialize::plan
//! Responsibility: prepared projection materialization contracts and validation.
//! Does not own: row loops, structural page dispatch, or DISTINCT execution.
//! Boundary: stores planner-derived projection contract for executor-owned consumers.

use crate::db::executor::projection::materialize::contracts::ProjectionSpec;
use crate::db::schema::{LeafCodec, ScalarCodec};
use crate::{
    db::{
        executor::projection::eval::{
            ProjectionEvalError, eval_compiled_expr_with_value_ref_reader,
        },
        executor::projection::materialize::contracts::{AccessPlannedQuery, CompiledExpr},
        executor::terminal::RowLayout,
    },
    error::InternalError,
    value::Value,
};

#[derive(Debug)]
pub(in crate::db) struct PreparedDirectProjectionSlots {
    projections: Vec<PreparedDirectProjectionSlot>,
    has_repeated_source: bool,
}

#[derive(Debug)]
pub(in crate::db) struct PreparedDirectProjectionSlot {
    source_slot: usize,
    previous_projection_index: Option<usize>,
}

impl PreparedDirectProjectionSlots {
    #[must_use]
    fn from_slots(slots: Vec<usize>) -> Self {
        let mut projections: Vec<PreparedDirectProjectionSlot> = Vec::with_capacity(slots.len());
        let mut has_repeated_source = false;

        for source_slot in slots {
            let previous_projection_index = projections
                .iter()
                .position(|projection| projection.source_slot == source_slot);
            has_repeated_source |= previous_projection_index.is_some();
            projections.push(PreparedDirectProjectionSlot {
                source_slot,
                previous_projection_index,
            });
        }

        Self {
            projections,
            has_repeated_source,
        }
    }

    #[must_use]
    pub(in crate::db) const fn projections(&self) -> &[PreparedDirectProjectionSlot] {
        self.projections.as_slice()
    }

    #[must_use]
    pub(in crate::db) const fn len(&self) -> usize {
        self.projections.len()
    }

    #[must_use]
    pub(in crate::db) const fn has_repeated_source(&self) -> bool {
        self.has_repeated_source
    }
}

impl PreparedDirectProjectionSlot {
    #[must_use]
    pub(in crate::db) const fn source_slot(&self) -> usize {
        self.source_slot
    }

    #[must_use]
    pub(in crate::db) const fn previous_projection_index(&self) -> Option<usize> {
        self.previous_projection_index
    }
}

///
/// PreparedProjectionContract
///
/// PreparedProjectionContract is the executor-owned prepared projection contract
/// shared by slot-row validation and higher-level structural row shaping.
/// It freezes the canonical projection semantic spec plus the derived direct
/// slot layouts needed by compiled scalar projection flow.
///
#[derive(Debug)]
pub(in crate::db) struct PreparedProjectionContract {
    projection: ProjectionSpec,
    compiled_exprs: Vec<CompiledExpr>,
    projection_is_model_identity: bool,
    retained_slot_direct_projection_slots: Option<PreparedDirectProjectionSlots>,
    retained_slot_direct_octet_length_projection_slots: Vec<Option<usize>>,
    data_row_direct_projection_slots: Option<PreparedDirectProjectionSlots>,
    #[cfg(any(test, feature = "diagnostics"))]
    projected_slot_mask: Vec<bool>,
}

impl PreparedProjectionContract {
    #[must_use]
    pub(in crate::db) const fn projection(&self) -> &ProjectionSpec {
        &self.projection
    }

    #[must_use]
    pub(in crate::db) const fn compiled_exprs(&self) -> &[CompiledExpr] {
        self.compiled_exprs.as_slice()
    }

    #[must_use]
    pub(in crate::db::executor) fn scalar_projection_contains_field_path(&self) -> bool {
        self.compiled_exprs()
            .iter()
            .any(CompiledExpr::contains_field_path)
    }

    #[must_use]
    pub(in crate::db::executor) const fn projection_is_model_identity(&self) -> bool {
        self.projection_is_model_identity
    }

    #[must_use]
    pub(in crate::db) const fn retained_slot_direct_projection_slots(
        &self,
    ) -> Option<&PreparedDirectProjectionSlots> {
        self.retained_slot_direct_projection_slots.as_ref()
    }

    #[must_use]
    pub(in crate::db) const fn retained_slot_direct_octet_length_projection_slots(
        &self,
    ) -> &[Option<usize>] {
        self.retained_slot_direct_octet_length_projection_slots
            .as_slice()
    }

    #[must_use]
    pub(in crate::db) const fn data_row_direct_projection_slots(
        &self,
    ) -> Option<&PreparedDirectProjectionSlots> {
        self.data_row_direct_projection_slots.as_ref()
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[must_use]
    pub(in crate::db) const fn projected_slot_mask(&self) -> &[bool] {
        self.projected_slot_mask.as_slice()
    }
}

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

/// Build one executor-owned prepared projection contract from planner-frozen metadata.
pub(in crate::db) fn prepare_projection_contract_from_plan(
    row_layout: &RowLayout,
    plan: &AccessPlannedQuery,
) -> Result<PreparedProjectionContract, InternalError> {
    let projection = plan.frozen_projection_spec()?.clone();
    let compiled_projection = plan
        .scalar_projection_plan()
        .ok_or_else(InternalError::query_executor_invariant)?
        .to_vec();
    let retained_slot_direct_projection_slots =
        direct_projection_slots_from_projection(&projection, plan.frozen_direct_projection_slots());
    let retained_slot_direct_octet_length_projection_slots =
        retained_slot_direct_octet_length_projection_slots_from_compiled(
            row_layout,
            &compiled_projection,
        );
    let data_row_direct_projection_slots = direct_projection_slots_from_projection(
        &projection,
        plan.frozen_data_row_direct_projection_slots(),
    );
    #[cfg(any(test, feature = "diagnostics"))]
    let projected_slot_mask =
        projected_slot_mask_from_slots(row_layout.field_count(), plan.projected_slot_mask()?);
    Ok(PreparedProjectionContract {
        projection,
        compiled_exprs: compiled_projection,
        projection_is_model_identity: plan.projection_is_model_identity()?,
        retained_slot_direct_projection_slots,
        retained_slot_direct_octet_length_projection_slots,
        data_row_direct_projection_slots,
        #[cfg(any(test, feature = "diagnostics"))]
        projected_slot_mask,
    })
}

/// Validate projection expressions against one row-domain that can expose
/// borrowed slot values by field slot.
pub(in crate::db::executor) fn validate_prepared_projection_row(
    prepared_validation: &PreparedProjectionContract,
    row: &impl ProjectionValidationRow,
) -> Result<(), InternalError> {
    if prepared_validation.projection_is_model_identity() {
        return Ok(());
    }

    for compiled in prepared_validation.compiled_exprs() {
        let mut read_slot = |slot| row.projection_validation_slot_value(slot);
        eval_compiled_expr_with_value_ref_reader(compiled, &mut read_slot)
            .map_err(ProjectionEvalError::into_internal_error)?;
    }

    Ok(())
}

// Validate slot availability for FieldPath-bearing expressions without
// evaluating the expression itself. Nested path evaluation requires raw
// persisted bytes and remains owned by the canonical projection executor.
fn direct_projection_slots_from_projection(
    projection: &ProjectionSpec,
    direct_projection_slots: Option<&[usize]>,
) -> Option<PreparedDirectProjectionSlots> {
    let direct_projection_slots = direct_projection_slots?;
    let mut slots = Vec::with_capacity(direct_projection_slots.len());

    for (field, slot) in projection
        .fields()
        .zip(direct_projection_slots.iter().copied())
    {
        field.direct_field_name()?;
        slots.push(slot);
    }

    Some(PreparedDirectProjectionSlots::from_slots(slots))
}

fn retained_slot_direct_octet_length_projection_slots_from_compiled(
    row_layout: &RowLayout,
    compiled_projection: &[CompiledExpr],
) -> Vec<Option<usize>> {
    let mut slots = Vec::with_capacity(compiled_projection.len());
    let mut has_direct_octet_length = false;

    for expr in compiled_projection {
        let slot = expr.direct_octet_length_slot().and_then(|(slot, _field)| {
            slot_uses_scalar_byte_length_codec(row_layout, slot).then_some(slot)
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

fn slot_uses_scalar_byte_length_codec(row_layout: &RowLayout, slot: usize) -> bool {
    row_layout
        .contract()
        .field_leaf_codec(slot)
        .is_ok_and(|leaf_codec| {
            matches!(
                leaf_codec,
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
