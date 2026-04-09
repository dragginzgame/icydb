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
            PredicateExecutionModel, PredicateProgram, classify_predicate_capabilities,
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

// Selects which planner-owned predicate view should seed executor preparation.
#[derive(Clone, Copy)]
enum PreparationPredicateSource {
    ExecutionPreparation,
    EffectiveRuntime,
}

impl PreparationPredicateSource {
    fn predicate_for_plan(self, plan: &AccessPlannedQuery) -> Option<PredicateExecutionModel> {
        match self {
            Self::ExecutionPreparation => plan.execution_preparation_predicate(),
            Self::EffectiveRuntime => plan.effective_execution_predicate(),
        }
    }
}

// Build-time toggles for the canonical preparation builder.
#[derive(Clone, Copy)]
struct PreparationBuildConfig {
    predicate_source: PreparationPredicateSource,
    include_predicate_capability_profile: bool,
    strict_policy: Option<IndexCompilePolicy>,
    conservative_policy: Option<IndexCompilePolicy>,
}

impl ExecutionPreparation {
    /// Build execution preparation once for one validated access-planned query.
    #[must_use]
    pub(in crate::db::executor) fn from_plan(
        model: &'static EntityModel,
        plan: &AccessPlannedQuery,
        slot_map: Option<Vec<usize>>,
    ) -> Self {
        Self::build(
            model,
            plan,
            slot_map,
            PreparationBuildConfig {
                predicate_source: PreparationPredicateSource::ExecutionPreparation,
                include_predicate_capability_profile: true,
                strict_policy: Some(IndexCompilePolicy::StrictAllOrNone),
                conservative_policy: None,
            },
        )
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
        Self::build(
            model,
            plan,
            slot_map,
            PreparationBuildConfig {
                predicate_source: PreparationPredicateSource::EffectiveRuntime,
                include_predicate_capability_profile: false,
                strict_policy: None,
                conservative_policy: Some(IndexCompilePolicy::ConservativeSubset),
            },
        )
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

    // Build the canonical preparation bundle once from one planner predicate
    // source plus the caller's requested index-program/capability outputs.
    fn build(
        model: &'static EntityModel,
        plan: &AccessPlannedQuery,
        slot_map: Option<Vec<usize>>,
        config: PreparationBuildConfig,
    ) -> Self {
        // Phase 1: compile the chosen planner predicate projection once.
        let predicate = config.predicate_source.predicate_for_plan(plan);
        let compiled_predicate = predicate
            .as_ref()
            .map(|predicate| PredicateProgram::compile_with_model(model, predicate));
        let compile_targets = index_compile_targets_for_model_plan(model, plan);

        // Phase 2: derive the optional planner/explain capability snapshot.
        let predicate_capability_profile = if config.include_predicate_capability_profile {
            predicate_capability_profile_for_preparation(
                compiled_predicate.as_ref(),
                compile_targets.as_deref(),
                slot_map.as_deref(),
            )
        } else {
            None
        };

        // Phase 3: compile whichever index-predicate programs this boundary needs.
        let strict_mode = config.strict_policy.and_then(|policy| {
            compile_index_program_for_preparation(
                compiled_predicate.as_ref(),
                compile_targets.as_deref(),
                slot_map.as_deref(),
                policy,
            )
        });
        let conservative_mode = config.conservative_policy.and_then(|policy| {
            compile_index_program_for_preparation(
                compiled_predicate.as_ref(),
                compile_targets.as_deref(),
                slot_map.as_deref(),
                policy,
            )
        });

        Self {
            compiled_predicate,
            compile_targets,
            conservative_mode,
            predicate_capability_profile,
            slot_map,
            strict_mode,
        }
    }
}

// Derive one optional predicate capability snapshot from the compiled
// predicate plus whichever slot-target projection this access path exposes.
fn predicate_capability_profile_for_preparation(
    compiled_predicate: Option<&PredicateProgram>,
    compile_targets: Option<&[IndexCompileTarget]>,
    slot_map: Option<&[usize]>,
) -> Option<PredicateCapabilityProfile> {
    match (compiled_predicate, compile_targets, slot_map) {
        (Some(compiled_predicate), Some(compile_targets), _) => {
            Some(classify_predicate_capabilities_for_targets(
                compiled_predicate.executable(),
                compile_targets,
            ))
        }
        (Some(compiled_predicate), None, Some(slot_map)) => Some(classify_predicate_capabilities(
            compiled_predicate.executable(),
            PredicateCapabilityContext::index_compile(slot_map),
        )),
        (Some(_) | None, None, None) | (None, Some(_), _) | (None, None, Some(_)) => None,
    }
}

// Compile one index predicate program for the requested pushdown policy using
// either key-item-aware compile targets or the structural slot-map fallback.
fn compile_index_program_for_preparation(
    compiled_predicate: Option<&PredicateProgram>,
    compile_targets: Option<&[IndexCompileTarget]>,
    slot_map: Option<&[usize]>,
    policy: IndexCompilePolicy,
) -> Option<IndexPredicateProgram> {
    match (compiled_predicate, compile_targets, slot_map) {
        (Some(compiled_predicate), Some(compile_targets), _) => compile_index_program_for_targets(
            compiled_predicate.executable(),
            compile_targets,
            policy,
        ),
        (Some(compiled_predicate), None, Some(slot_map)) => {
            compile_index_program(compiled_predicate.executable(), slot_map, policy)
        }
        (Some(_) | None, None, None) | (None, Some(_), _) | (None, None, Some(_)) => None,
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
