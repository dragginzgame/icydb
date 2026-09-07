//! Module: db::executor::projection::materialize::plan
//! Responsibility: prepared projection materialization contracts.
//! Does not own: row loops, structural page dispatch, or DISTINCT execution.
//! Boundary: stores planner-derived projection contract for executor-owned consumers.

use crate::db::executor::projection::materialize::contracts::ProjectionSpec;
use crate::db::schema::{LeafCodec, ScalarCodec};
use crate::{
    db::{
        executor::projection::materialize::contracts::{AccessPlannedQuery, CompiledExpr},
        executor::terminal::RowLayout,
    },
    error::InternalError,
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
    /// Build source-slot metadata, retaining prior output indices for repeats.
    #[must_use]
    pub(super) fn from_slots(slots: Vec<usize>) -> Self {
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
/// consumed by scalar output shaping.
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
    Ok(PreparedProjectionContract {
        projection,
        compiled_exprs: compiled_projection,
        projection_is_model_identity: plan.projection_is_model_identity()?,
        retained_slot_direct_projection_slots,
        retained_slot_direct_octet_length_projection_slots,
        data_row_direct_projection_slots,
    })
}

// Reuse planner-frozen slots only when every projection is a direct field.
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

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(PreparedDirectProjectionSlot {
Self{source_slot,previous_projection_index} => [source_slot,previous_projection_index],
});
crate::retained::retained_fields!(PreparedDirectProjectionSlots {
Self{projections,has_repeated_source} => [projections,has_repeated_source],
});
crate::retained::retained_fields!(PreparedProjectionContract {
Self{projection,compiled_exprs,projection_is_model_identity,retained_slot_direct_projection_slots,retained_slot_direct_octet_length_projection_slots,data_row_direct_projection_slots} => [projection,compiled_exprs,projection_is_model_identity,retained_slot_direct_projection_slots,retained_slot_direct_octet_length_projection_slots,data_row_direct_projection_slots],
});
