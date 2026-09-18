//! Module: executor::pipeline::runtime::retained_slots
//! Responsibility: retained-slot layout derivation for scalar execution runtime.
//! Does not own: execution-input DTOs or retained-row storage behavior.
//! Boundary: compiles runtime slot requirements into terminal-owned retained layouts.

use crate::{
    db::{
        executor::{
            EntityAuthority,
            pipeline::contracts::{CursorEmissionMode, ProjectionMaterializationMode},
            route::access_order_satisfied_by_route_mode,
            terminal::{RetainedSlotLayout, RetainedSlotValueMode, RowLayout},
        },
        predicate::IndexCompileTarget,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
};

/// Compile the canonical retained-slot layout for one explicit scalar
/// projection and cursor-emission mode pair.
pub(in crate::db::executor) fn compile_retained_slot_layout_for_mode(
    authority: &EntityAuthority,
    plan: &AccessPlannedQuery,
    projection_materialization: ProjectionMaterializationMode,
    cursor_emission: CursorEmissionMode,
) -> Result<Option<RetainedSlotLayout>, InternalError> {
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
) -> Result<Option<RetainedSlotLayout>, InternalError> {
    let retain_slot_rows = projection_materialization.retain_slot_rows();

    compile_retained_slot_layout(
        authority,
        plan,
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
    retain_slot_rows: bool,
    cursor_emission: CursorEmissionMode,
    extra_slots: &[usize],
) -> Result<Option<RetainedSlotLayout>, InternalError> {
    let row_layout = authority.row_layout_ref();
    let mut required_slots = RetainedSlotRequirements::new(row_layout.field_count());

    // Phase 1: retain projection inputs, including scalar byte-length values.
    if retain_slot_rows {
        mark_projection_retained_slots(row_layout, plan, &mut required_slots)?;
    }

    // Terminal-owned consumers such as scalar aggregate reduction can require
    // slots that are deliberately absent from the cached outward projection.
    // Keep those slots attached to this runtime layout only.
    required_slots.mark_slots(extra_slots.iter().copied());

    // Phase 2: ordering slots are needed for in-memory ordering and also for
    // cursor boundary assembly on route-ordered load paths.
    if plan.scalar_plan().order.as_ref().is_some()
        && let Some(order_slots) = plan.order_referenced_slots()
    {
        let route_needs_order_slots =
            !access_order_satisfied_by_route_mode(plan) || cursor_emission.enabled();

        if route_needs_order_slots {
            required_slots.mark_slots(order_slots.iter().copied());
        }
    }

    // Phase 3: index-range cursor anchors need the complete index key item
    // slot set, not only the outward order slots. Keep these slots explicit
    // for cursor-emitting index-range paths, including identity projections.
    if cursor_emission.enabled()
        && plan
            .access
            .shape_facts()
            .has_single_path_index_range_access_path()
        && let Some(index_compile_targets) = plan.index_compile_targets()
    {
        required_slots.mark_index_compile_target_slots(index_compile_targets);
    }

    let (required_slots, value_modes) = required_slots.into_slots_and_value_modes();

    if required_slots.is_empty() && !retain_slot_rows {
        return Ok(None);
    }

    Ok(Some(RetainedSlotLayout::compile_with_value_modes(
        row_layout.field_count(),
        required_slots,
        value_modes,
    )))
}

// Mark projection-driven retained slots while preserving byte-length-only
// scalar blob/text fields as length values instead of full blob/text values.
// Non-direct expressions keep normal value materialization so diagnostics and
// fallback expression semantics stay unchanged.
fn mark_projection_retained_slots(
    row_layout: &RowLayout,
    plan: &AccessPlannedQuery,
    required_slots: &mut RetainedSlotRequirements,
) -> Result<(), InternalError> {
    let Some(compiled_projection) = plan.scalar_projection_plan() else {
        required_slots.mark_slots(plan.projection_referenced_slots()?.iter().copied());
        return Ok(());
    };

    for expr in compiled_projection {
        let Some((slot, _field)) = expr.direct_octet_length_slot() else {
            expr.for_each_referenced_slot(&mut |slot| required_slots.mark_slot(slot));
            continue;
        };

        if row_layout.slot_uses_scalar_byte_length_codec(slot) {
            required_slots.mark_slot_octet_length(slot);
        } else {
            required_slots.mark_slot(slot);
        }
    }

    Ok(())
}

///
/// RetainedSlotRequirements
///
/// RetainedSlotRequirements collects the canonical retained-slot requirement
/// set for one scalar execution shape.
/// It exists so projection, predicate, ordering, and index-anchor slot needs
/// can all contribute through one owner-local boundary instead of mutating the
/// raw slot state directly in several separate loops.
///

struct RetainedSlotRequirements {
    modes: Vec<Option<RetainedSlotValueMode>>,
}

impl RetainedSlotRequirements {
    // Build one empty retained-slot requirement set sized to the model field
    // count for the current execution shape.
    fn new(field_count: usize) -> Self {
        Self {
            modes: vec![None; field_count],
        }
    }

    // Mark one iterator of already-resolved field slots as required.
    fn mark_slots(&mut self, slots: impl IntoIterator<Item = usize>) {
        for slot in slots {
            self.mark_slot(slot);
        }
    }

    // Mark one slot as requiring normal value materialization. Normal wins
    // over length-only materialization when another phase needs the real
    // scalar value for projection, ordering, cursor emission, or validation.
    fn mark_slot(&mut self, slot: usize) {
        if let Some(mode) = self.modes.get_mut(slot) {
            *mode = Some(RetainedSlotValueMode::Normal);
        }
    }

    // Mark one slot as requiring only scalar byte length unless another phase
    // has already requested normal value materialization.
    fn mark_slot_octet_length(&mut self, slot: usize) {
        if let Some(mode) = self.modes.get_mut(slot) {
            mode.get_or_insert(RetainedSlotValueMode::ScalarOctetLength);
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

        for (slot, mode) in self.modes.into_iter().enumerate() {
            let Some(mode) = mode else { continue };
            // Empty modes mean all-normal. Backfill only when the first length
            // override appears, then retain alignment with every selected slot.
            if mode != RetainedSlotValueMode::Normal && value_modes.is_empty() {
                value_modes.resize(slots.len(), RetainedSlotValueMode::Normal);
            }
            if mode != RetainedSlotValueMode::Normal || !value_modes.is_empty() {
                value_modes.push(mode);
            }
            slots.push(slot);
        }

        (slots, value_modes)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{RetainedSlotRequirements, RetainedSlotValueMode};

    #[test]
    fn full_value_requirements_win_in_either_marking_order() {
        for length_first in [false, true] {
            let mut requirements = RetainedSlotRequirements::new(4);
            if length_first {
                requirements.mark_slot_octet_length(2);
            }
            requirements.mark_slots([2, 0]);
            requirements.mark_slot_octet_length(2);
            requirements.mark_slot(99);
            requirements.mark_slot_octet_length(99);
            let (slots, modes) = requirements.into_slots_and_value_modes();
            assert_eq!(slots, [0, 2]);
            assert!(modes.is_empty());
            assert_eq!(modes.capacity(), 0);
        }
    }

    #[test]
    fn length_override_modes_align_with_sparse_sorted_slots() {
        use RetainedSlotValueMode::{Normal, ScalarOctetLength};

        for length_slot in [0, 2, 4] {
            let mut requirements = RetainedSlotRequirements::new(6);
            for slot in [4, 0, 2] {
                if slot == length_slot {
                    requirements.mark_slot_octet_length(slot);
                    requirements.mark_slot_octet_length(slot);
                } else {
                    requirements.mark_slot(slot);
                }
            }
            let (slots, modes) = requirements.into_slots_and_value_modes();
            assert_eq!(slots, [0, 2, 4]);
            assert_eq!(modes.len(), slots.len());
            for (slot, mode) in slots.into_iter().zip(modes) {
                assert_eq!(
                    mode,
                    if slot == length_slot {
                        ScalarOctetLength
                    } else {
                        Normal
                    }
                );
            }
        }
        let (slots, modes) = RetainedSlotRequirements::new(0).into_slots_and_value_modes();
        assert!(slots.is_empty());
        assert_eq!(modes.capacity(), 0);
    }
}
