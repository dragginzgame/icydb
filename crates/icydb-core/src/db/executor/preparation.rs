//! Module: db::executor::preparation
//! Responsibility: build reusable executor-side predicate/index compilation state.
//! Does not own: access planning or runtime route policy.
//! Boundary: one-time preparation object consumed by execution paths.

use crate::{
    db::{
        executor::ExecutableAccessPlan,
        index::{
            IndexCompilePolicy, IndexPredicateProgram, compile_index_program,
            compile_index_program_for_targets,
        },
        predicate::{
            IndexCompileTarget, PredicateCapabilityContext, PredicateCapabilityProfile,
            PredicateProgram, classify_predicate_capabilities,
            classify_predicate_capabilities_for_targets,
        },
        query::plan::AccessPlannedQuery,
    },
    model::{
        entity::{EntityModel, resolve_field_slot},
        index::IndexKeyItemsRef,
    },
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
    compile_targets: Option<Vec<IndexCompileTarget>>,
    conservative_mode: Option<IndexPredicateProgram>,
    predicate_capability_profile: Option<PredicateCapabilityProfile>,
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
        let effective_predicate = plan.execution_preparation_predicate();
        let compiled_predicate = effective_predicate
            .as_ref()
            .map(|predicate| PredicateProgram::compile_with_model(model, predicate));

        let compile_targets = index_compile_targets_for_model_plan(model, plan);

        // Phase 2: Derive canonical predicate capability once for runtime/explain consumers.
        let predicate_capability_profile = match (
            compiled_predicate.as_ref(),
            compile_targets.as_deref(),
            slot_map.as_deref(),
        ) {
            (Some(compiled_predicate), Some(compile_targets), _) => {
                Some(classify_predicate_capabilities_for_targets(
                    compiled_predicate.executable(),
                    compile_targets,
                ))
            }
            (Some(compiled_predicate), None, Some(slot_map)) => {
                Some(classify_predicate_capabilities(
                    compiled_predicate.executable(),
                    PredicateCapabilityContext::index_compile(slot_map),
                ))
            }
            (Some(_) | None, None, None) | (None, Some(_), _) | (None, None, Some(_)) => None,
        };

        // Phase 3: Build strict index predicate program only when strict full pushdown is valid.
        let strict_mode = match (
            compiled_predicate.as_ref(),
            compile_targets.as_deref(),
            slot_map.as_deref(),
        ) {
            (Some(compiled_predicate), Some(compile_targets), _) => {
                compile_index_program_for_targets(
                    compiled_predicate.executable(),
                    compile_targets,
                    IndexCompilePolicy::StrictAllOrNone,
                )
            }
            (Some(compiled_predicate), None, Some(slot_map)) => compile_index_program(
                compiled_predicate.executable(),
                slot_map,
                IndexCompilePolicy::StrictAllOrNone,
            ),
            (Some(_) | None, None, None) | (None, Some(_), _) | (None, None, Some(_)) => None,
        };

        Self {
            compiled_predicate,
            compile_targets,
            conservative_mode: None,
            predicate_capability_profile,
            slot_map,
            strict_mode,
        }
    }

    /// Build the lighter runtime execution preparation needed by shared scalar
    /// load execution.
    ///
    /// This path keeps only the compiled row predicate plus slot-map data used
    /// by runtime filtering and conservative index-predicate compilation. It
    /// intentionally skips explain/aggregate-only capability snapshots and the
    /// additional strict predicate program that the scalar load runtime never
    /// consumes after route planning has already completed.
    #[must_use]
    pub(in crate::db::executor) fn from_runtime_plan(
        model: &'static EntityModel,
        plan: &AccessPlannedQuery,
        slot_map: Option<Vec<usize>>,
    ) -> Self {
        let effective_predicate = plan.effective_execution_predicate();
        let compiled_predicate = effective_predicate
            .as_ref()
            .map(|predicate| PredicateProgram::compile_with_model(model, predicate));
        let compile_targets = index_compile_targets_for_model_plan(model, plan);
        let conservative_mode = match (
            compiled_predicate.as_ref(),
            compile_targets.as_deref(),
            slot_map.as_deref(),
        ) {
            (Some(compiled_predicate), Some(compile_targets), _) => {
                compile_index_program_for_targets(
                    compiled_predicate.executable(),
                    compile_targets,
                    IndexCompilePolicy::ConservativeSubset,
                )
            }
            (Some(compiled_predicate), None, Some(slot_map)) => compile_index_program(
                compiled_predicate.executable(),
                slot_map,
                IndexCompilePolicy::ConservativeSubset,
            ),
            (Some(_) | None, None, None) | (None, Some(_), _) | (None, None, Some(_)) => None,
        };

        Self {
            compiled_predicate,
            compile_targets,
            conservative_mode,
            predicate_capability_profile: None,
            slot_map,
            strict_mode: None,
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn compiled_predicate(&self) -> Option<&PredicateProgram> {
        self.compiled_predicate.as_ref()
    }

    #[must_use]
    pub(in crate::db::executor) const fn conservative_mode(
        &self,
    ) -> Option<&IndexPredicateProgram> {
        self.conservative_mode.as_ref()
    }

    #[must_use]
    pub(in crate::db::executor) fn slot_map(&self) -> Option<&[usize]> {
        self.slot_map.as_deref()
    }

    #[must_use]
    pub(in crate::db::executor) fn compile_targets(&self) -> Option<&[IndexCompileTarget]> {
        self.compile_targets.as_deref()
    }

    #[must_use]
    pub(in crate::db::executor) const fn predicate_capability_profile(
        &self,
    ) -> Option<PredicateCapabilityProfile> {
        // This is a read-only capability snapshot for planner/explain consumers.
        // Predicate interpretation and capability meaning stay owned by
        // `db::predicate::capability`.
        self.predicate_capability_profile
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

// Resolve one structural key-item-aware compile target list for one
// access-planned query using structural model data.
fn index_compile_targets_for_model_plan(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
) -> Option<Vec<IndexCompileTarget>> {
    let index = plan.access.as_path()?.selected_index_model()?;
    let mut targets = Vec::new();

    match index.key_items() {
        IndexKeyItemsRef::Fields(fields) => {
            for (component_index, &field_name) in fields.iter().enumerate() {
                let field_slot = resolve_field_slot(model, field_name)?;
                targets.push(IndexCompileTarget {
                    component_index,
                    field_slot,
                    key_item: crate::model::index::IndexKeyItem::Field(field_name),
                });
            }
        }
        IndexKeyItemsRef::Items(items) => {
            for (component_index, &key_item) in items.iter().enumerate() {
                let field_slot = resolve_field_slot(model, key_item.field())?;
                targets.push(IndexCompileTarget {
                    component_index,
                    field_slot,
                    key_item,
                });
            }
        }
    }

    Some(targets)
}
