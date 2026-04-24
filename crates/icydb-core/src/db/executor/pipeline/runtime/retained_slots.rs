//! Module: executor::pipeline::runtime::retained_slots
//! Responsibility: retained-slot layout derivation for scalar execution runtime.
//! Does not own: execution-input DTOs or retained-row storage behavior.
//! Boundary: compiles runtime slot requirements into terminal-owned retained layouts.

use crate::{
    db::{
        executor::{
            pipeline::contracts::{CursorEmissionMode, ProjectionMaterializationMode},
            route::access_order_satisfied_by_route_contract,
            terminal::RetainedSlotLayout,
        },
        predicate::PredicateProgram,
        query::plan::{AccessPlannedQuery, expr::ScalarProjectionExpr},
    },
    model::{entity::EntityModel, index::IndexKeyItemsRef},
};

/// Compile the canonical retained-slot layout for one explicit scalar
/// projection and cursor-emission mode pair.
pub(in crate::db::executor) fn compile_retained_slot_layout_for_mode(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    projection_materialization: ProjectionMaterializationMode,
    cursor_emission: CursorEmissionMode,
) -> Option<RetainedSlotLayout> {
    let projection_validation_enabled =
        projection_materialization.validate_projection() && !plan.projection_is_model_identity();
    let retain_slot_rows = projection_materialization.retain_slot_rows();

    compile_retained_slot_layout(
        model,
        plan,
        plan.effective_runtime_compiled_predicate(),
        projection_validation_enabled,
        retain_slot_rows,
        cursor_emission,
    )
}

// Compile the canonical retained-slot layout once per execution shape so
// shared scalar row materialization does not rebuild
// projection/predicate/order/cursor reachability ad hoc at each execution
// boundary.
fn compile_retained_slot_layout(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    compiled_predicate: Option<&PredicateProgram>,
    projection_validation_enabled: bool,
    retain_slot_rows: bool,
    cursor_emission: CursorEmissionMode,
) -> Option<RetainedSlotLayout> {
    let mut required_slots = RetainedSlotRequirements::new(model.fields().len());

    // Phase 1: projection validation and retained-slot materialization both
    // need one stable slot set for later structural slot reads.
    if projection_validation_enabled || retain_slot_rows {
        required_slots.mark_slots(plan.projection_referenced_slots().iter().copied());
    }

    // Phase 2: residual filter semantics still run on retained slot rows
    // before the outer projection materializer consumes them.
    if plan.effective_runtime_filter_program().is_some() {
        if let Some(predicate_program) = compiled_predicate {
            predicate_program.mark_referenced_slots(required_slots.flags_mut());
        }
        if let Some(filter_expr) = plan.effective_runtime_compiled_filter_expr() {
            required_slots.mark_slots_for_scalar_projection_expr(filter_expr);
        }
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
        && let Some(spec) = plan.access.as_index_range_path()
    {
        required_slots.mark_index_key_item_slots(model, spec.index().key_items());
    }

    let required_slots = required_slots.into_slots();

    if required_slots.is_empty() && !retain_slot_rows {
        return None;
    }

    Some(RetainedSlotLayout::compile(
        model.fields().len(),
        required_slots,
    ))
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
}

impl RetainedSlotRequirements {
    // Build one empty retained-slot requirement set sized to the model field
    // count for the current execution shape.
    fn new(field_count: usize) -> Self {
        Self {
            flags: vec![false; field_count],
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
            self.flags[slot] = true;
        }
    }

    // Mark every slot referenced by one compiled scalar filter expression.
    fn mark_slots_for_scalar_projection_expr(&mut self, expr: &ScalarProjectionExpr) {
        expr.mark_referenced_slots(self.flags.as_mut_slice());
    }

    // Mark the slots needed to reconstruct index-range cursor anchors from the
    // full index key item set instead of only the outward order fields.
    fn mark_index_key_item_slots(&mut self, model: &EntityModel, key_items: IndexKeyItemsRef) {
        match key_items {
            IndexKeyItemsRef::Fields(fields) => {
                for field in fields {
                    if let Some(slot) = model.resolve_field_slot(field) {
                        self.flags[slot] = true;
                    }
                }
            }
            IndexKeyItemsRef::Items(items) => {
                for key_item in items {
                    if let Some(slot) = model.resolve_field_slot(key_item.field()) {
                        self.flags[slot] = true;
                    }
                }
            }
        }
    }

    // Consume the requirement set into the final sorted retained-slot vector
    // used by the compiled layout contract.
    fn into_slots(self) -> Vec<usize> {
        self.flags
            .into_iter()
            .enumerate()
            .filter_map(|(slot, required)| required.then_some(slot))
            .collect()
    }
}
