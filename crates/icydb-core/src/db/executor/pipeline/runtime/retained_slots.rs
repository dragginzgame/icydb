//! Module: executor::pipeline::runtime::retained_slots
//! Responsibility: retained-slot layout derivation for scalar execution runtime.
//! Does not own: execution-input DTOs or retained-row storage behavior.
//! Boundary: compiles runtime slot requirements into terminal-owned retained layouts.

use crate::{
    db::{
        executor::{
            EntityAuthority,
            pipeline::contracts::{CursorEmissionMode, ProjectionMaterializationMode},
            route::access_order_satisfied_by_route_contract,
            terminal::{RetainedSlotLayout, RetainedSlotValueMode, RowLayout},
        },
        predicate::IndexCompileTarget,
        query::plan::AccessPlannedQuery,
    },
    model::field::{LeafCodec, ScalarCodec},
};

/// Compile the canonical retained-slot layout for one explicit scalar
/// projection and cursor-emission mode pair.
pub(in crate::db::executor) fn compile_retained_slot_layout_for_mode(
    authority: &EntityAuthority,
    plan: &AccessPlannedQuery,
    projection_materialization: ProjectionMaterializationMode,
    cursor_emission: CursorEmissionMode,
) -> Option<RetainedSlotLayout> {
    compile_retained_slot_layout_for_mode_with_extra_slots(
        authority,
        plan,
        projection_materialization,
        cursor_emission,
        &[],
    )
}

/// Compile the canonical retained-slot layout for one scalar runtime mode
/// while adding owner-supplied terminal slots that are not part of the cached
/// scalar projection shape.
pub(in crate::db::executor) fn compile_retained_slot_layout_for_mode_with_extra_slots(
    authority: &EntityAuthority,
    plan: &AccessPlannedQuery,
    projection_materialization: ProjectionMaterializationMode,
    cursor_emission: CursorEmissionMode,
    extra_slots: &[usize],
) -> Option<RetainedSlotLayout> {
    let projection_validation_enabled =
        projection_materialization.validate_projection() && !plan.projection_is_model_identity();
    let retain_slot_rows = projection_materialization.retain_slot_rows();

    compile_retained_slot_layout(
        authority,
        plan,
        projection_validation_enabled,
        retain_slot_rows,
        cursor_emission,
        extra_slots,
    )
}

// Compile the canonical retained-slot layout once per execution shape so
// shared scalar row materialization does not rebuild
// projection/predicate/order/cursor reachability ad hoc at each execution
// boundary.
fn compile_retained_slot_layout(
    authority: &EntityAuthority,
    plan: &AccessPlannedQuery,
    projection_validation_enabled: bool,
    retain_slot_rows: bool,
    cursor_emission: CursorEmissionMode,
    extra_slots: &[usize],
) -> Option<RetainedSlotLayout> {
    let row_layout = authority.row_layout_ref();
    let mut required_slots = RetainedSlotRequirements::new(row_layout.field_count());

    // Phase 1: projection validation needs complete values for diagnostics.
    // Retained-slot projection materialization can keep exact
    // `OCTET_LENGTH(text/blob)` expressions as length-only retained values.
    if projection_validation_enabled {
        required_slots.mark_slots(plan.projection_referenced_slots().iter().copied());
    } else if retain_slot_rows {
        mark_projection_retained_slots(row_layout, plan, &mut required_slots);
    }

    // Terminal-owned consumers such as scalar aggregate reduction can require
    // slots that are deliberately absent from the cached outward projection.
    // Keep those slots attached to this runtime layout only.
    required_slots.mark_slots(extra_slots.iter().copied());

    // Phase 2: residual filter semantics still run on retained slot rows
    // before the outer projection materializer consumes them.
    if let Some(filter_program) = plan.effective_runtime_filter_program() {
        filter_program.mark_referenced_slots(required_slots.flags_mut());
    }

    // Phase 3: ordering slots are needed for in-memory ordering and also for
    // cursor boundary assembly on route-ordered load paths.
    if plan.scalar_plan().order.as_ref().is_some()
        && let Some(order_slots) = plan.order_referenced_slots()
    {
        let route_needs_order_slots =
            !access_order_satisfied_by_route_contract(plan) || cursor_emission.enabled();

        if route_needs_order_slots {
            required_slots.mark_slots(order_slots.iter().copied());
        }
    }

    // Phase 4: index-range cursor anchors need the complete index key item
    // slot set, not only the outward order slots. Model-identity projections
    // no longer force shared validation state, so keep these slots explicit
    // for cursor-emitting index-range paths.
    if cursor_emission.enabled()
        && plan.access.as_index_range_path().is_some()
        && let Some(index_compile_targets) = plan.index_compile_targets()
    {
        required_slots.mark_index_compile_target_slots(index_compile_targets);
    }

    let (required_slots, value_modes) = required_slots.into_slots_and_value_modes();

    if required_slots.is_empty() && !retain_slot_rows {
        return None;
    }

    Some(RetainedSlotLayout::compile_with_value_modes(
        row_layout.field_count(),
        required_slots,
        value_modes,
    ))
}

// Mark projection-driven retained slots while preserving byte-length-only
// scalar blob/text fields as length values instead of full blob/text values.
// Non-direct expressions keep normal value materialization so diagnostics and
// fallback expression semantics stay unchanged.
fn mark_projection_retained_slots(
    row_layout: &RowLayout,
    plan: &AccessPlannedQuery,
    required_slots: &mut RetainedSlotRequirements,
) {
    let Some(compiled_projection) = plan.scalar_projection_plan() else {
        required_slots.mark_slots(plan.projection_referenced_slots().iter().copied());
        return;
    };

    for expr in compiled_projection {
        let Some((slot, _field)) = expr.direct_octet_length_slot() else {
            expr.for_each_referenced_slot(&mut |slot| required_slots.mark_slot(slot));
            continue;
        };

        if slot_uses_scalar_byte_length_codec(row_layout, slot) {
            required_slots.mark_slot_octet_length(slot);
        } else {
            required_slots.mark_slot(slot);
        }
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

///
/// RetainedSlotRequirements
///
/// RetainedSlotRequirements collects the canonical retained-slot requirement
/// set for one scalar execution shape.
/// It exists so projection, predicate, ordering, and index-anchor slot needs
/// can all contribute through one owner-local boundary instead of mutating the
/// raw bitset directly in several separate loops.
///

struct RetainedSlotRequirements {
    flags: Vec<bool>,
    octet_length_flags: Vec<bool>,
}

impl RetainedSlotRequirements {
    // Build one empty retained-slot requirement set sized to the model field
    // count for the current execution shape.
    fn new(field_count: usize) -> Self {
        Self {
            flags: vec![false; field_count],
            octet_length_flags: vec![false; field_count],
        }
    }

    // Borrow the raw bitset when an existing helper already knows how to mark
    // referenced slots in place.
    const fn flags_mut(&mut self) -> &mut [bool] {
        self.flags.as_mut_slice()
    }

    // Mark one iterator of already-resolved field slots as required.
    fn mark_slots(&mut self, slots: impl IntoIterator<Item = usize>) {
        for slot in slots {
            self.mark_slot(slot);
        }
    }

    // Mark one slot as requiring normal value materialization. Normal wins
    // over length-only materialization when another phase needs the real
    // scalar value for filtering, ordering, cursor emission, or validation.
    fn mark_slot(&mut self, slot: usize) {
        if let Some(required) = self.flags.get_mut(slot) {
            *required = true;
        }
        if let Some(octet_length) = self.octet_length_flags.get_mut(slot) {
            *octet_length = false;
        }
    }

    // Mark one slot as requiring only scalar byte length unless another phase
    // has already requested normal value materialization.
    fn mark_slot_octet_length(&mut self, slot: usize) {
        let Some(required) = self.flags.get(slot) else {
            return;
        };
        if *required {
            return;
        }
        if let Some(octet_length) = self.octet_length_flags.get_mut(slot) {
            *octet_length = true;
        }
    }

    // Mark the slots needed to reconstruct index-range cursor anchors from the
    // planner-frozen key-item compile targets instead of reopening generated
    // model field-slot resolution during retained-layout compilation.
    fn mark_index_compile_target_slots(&mut self, targets: &[IndexCompileTarget]) {
        for target in targets {
            self.mark_slot(target.field_slot);
        }
    }

    // Consume the requirement set into the final sorted retained-slot vector
    // used by the compiled layout contract.
    fn into_slots_and_value_modes(self) -> (Vec<usize>, Vec<RetainedSlotValueMode>) {
        let mut slots = Vec::new();
        let mut value_modes = Vec::new();

        for slot in 0..self.flags.len() {
            if self.flags[slot] {
                slots.push(slot);
                value_modes.push(RetainedSlotValueMode::Normal);
            } else if self.octet_length_flags[slot] {
                slots.push(slot);
                value_modes.push(RetainedSlotValueMode::ScalarOctetLength);
            }
        }

        (slots, value_modes)
    }
}
