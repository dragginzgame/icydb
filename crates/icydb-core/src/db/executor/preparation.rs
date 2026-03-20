//! Module: db::executor::preparation
//! Responsibility: build reusable executor-side predicate/index compilation state.
//! Does not own: access planning or runtime route policy.
//! Boundary: one-time preparation object consumed by execution paths.

use crate::{
    db::{
        executor::ExecutableAccessPlan,
        index::{IndexCompilePolicy, IndexPredicateProgram, compile_index_program},
        predicate::PredicateProgram,
        query::plan::AccessPlannedQuery,
    },
    model::entity::{EntityModel, resolve_field_slot},
    traits::EntityKind,
};

///
/// ExecutionPreparation
///
/// Canonical one-shot predicate/index compilation bundle derived from one plan.
/// Build once at the execution boundary and reuse across route/load/delete/aggregate paths.
///

#[derive(Clone)]
pub(in crate::db::executor) struct ExecutionPreparation {
    compiled_predicate: Option<PredicateProgram>,
    slot_map: Option<Vec<usize>>,
    strict_mode: Option<IndexPredicateProgram>,
}

impl ExecutionPreparation {
    /// Build execution preparation once for one validated access-planned query.
    #[must_use]
    pub(in crate::db::executor) fn from_plan(
        model: &'static EntityModel,
        plan: &AccessPlannedQuery,
        slot_map: Option<Vec<usize>>,
    ) -> Self {
        // Phase 1: Compile the row-predicate once from logical plan semantics.
        let compiled_predicate = plan
            .scalar_plan()
            .predicate
            .as_ref()
            .map(|predicate| PredicateProgram::compile_with_model(model, predicate));

        // Phase 2: Build strict index predicate program only when both inputs exist.
        let strict_mode = match (compiled_predicate.as_ref(), slot_map.as_deref()) {
            (Some(compiled_predicate), Some(slot_map)) => compile_index_program(
                compiled_predicate.resolved(),
                slot_map,
                IndexCompilePolicy::StrictAllOrNone,
            ),
            (Some(_) | None, None) | (None, Some(_)) => None,
        };

        Self {
            compiled_predicate,
            slot_map,
            strict_mode,
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn compiled_predicate(&self) -> Option<&PredicateProgram> {
        self.compiled_predicate.as_ref()
    }

    #[must_use]
    pub(in crate::db::executor) fn slot_map(&self) -> Option<&[usize]> {
        self.slot_map.as_deref()
    }

    #[must_use]
    pub(in crate::db::executor) const fn strict_mode(&self) -> Option<&IndexPredicateProgram> {
        self.strict_mode.as_ref()
    }
}

/// Resolve index field slots from a single-path index access shape using structural model data.
pub(in crate::db::executor) fn resolved_index_slots_for_access_path<K>(
    model: &'static EntityModel,
    access: &ExecutableAccessPlan<'_, K>,
) -> Option<Vec<usize>> {
    let path = access.as_path()?;
    let path_capabilities = path.capabilities();
    let index_fields = path_capabilities.index_fields_for_slot_map()?;

    let mut slots = Vec::with_capacity(index_fields.len());
    for field_name in index_fields {
        let slot = resolve_field_slot(model, field_name)?;
        slots.push(slot);
    }

    Some(slots)
}

/// Resolve one structural slot map for one access-planned query using structural model data.
pub(in crate::db::executor) fn slot_map_for_model_plan(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
) -> Option<Vec<usize>> {
    resolved_index_slots_for_access_path(model, plan.access.resolve_strategy().executable())
}

/// Resolve one structural slot map for one entity-bound plan at the execution boundary.
pub(in crate::db::executor) fn slot_map_for_entity_plan<E>(
    plan: &AccessPlannedQuery,
) -> Option<Vec<usize>>
where
    E: EntityKind,
{
    slot_map_for_model_plan(E::MODEL, plan)
}
