//! Module: db::executor::planning::preparation
//! Responsibility: build reusable executor-side predicate/index compilation state.
//! Does not own: access planning or runtime route policy.
//! Boundary: one-time preparation object consumed by execution paths.

#[cfg(test)]
mod tests;

use crate::db::{
    index::{
        IndexCompilePolicy, IndexPredicateProgram, compile_index_program,
        compile_index_program_for_targets,
    },
    predicate::{
        IndexCompileTarget, PredicateCapabilityContext, PredicateCapabilityProfile,
        PredicateProgram, classify_predicate_capabilities,
        classify_predicate_capabilities_for_targets,
    },
    query::plan::{
        AccessPlannedQuery, EffectiveRuntimeFilterProgram, covering_strict_predicate_compatible,
    },
};
use std::borrow::Cow;

///
/// ExecutionPreparation
///
/// Canonical one-shot predicate/index compilation bundle derived from one plan.
/// Build once at the execution boundary and reuse across route/load/delete/aggregate paths.
///

#[derive(Clone)]
pub(in crate::db::executor) struct ExecutionPreparation {
    compiled_predicate: Option<PredicateProgram>,
    effective_runtime_filter_program: Option<EffectiveRuntimeFilterProgram>,
    compile_targets: Option<Vec<IndexCompileTarget>>,
    index_program: Option<PreparedIndexProgram>,
    predicate_capability_profile: Option<PredicateCapabilityProfile>,
    slot_map: Option<Vec<usize>>,
}

/// One completed compiler result, including a successful unsupported result.
/// Absence of this record means no policy was requested, not failed compilation.
#[derive(Clone)]
struct PreparedIndexProgram {
    policy: IndexCompilePolicy,
    program: Option<IndexPredicateProgram>,
}

impl PreparedIndexProgram {
    // Only the same policy may reuse absence. A different policy still needs
    // its own compilation; supported results are borrowed without cloning.
    fn resolve(
        prepared: Option<&Self>,
        policy: IndexCompilePolicy,
        compile: impl FnOnce() -> Option<IndexPredicateProgram>,
    ) -> Option<Cow<'_, IndexPredicateProgram>> {
        match prepared {
            Some(prepared) if prepared.policy == policy => {
                prepared.program.as_ref().map(Cow::Borrowed)
            }
            Some(_) | None => compile().map(Cow::Owned),
        }
    }
}

// Selects which planner-owned predicate view should seed executor preparation.
#[derive(Clone, Copy)]
enum PreparationPredicateSource {
    ExecutionPreparation,
    EffectiveRuntime,
}

// Build-time toggles for the canonical preparation builder.
#[derive(Clone, Copy)]
struct PreparationBuildConfig {
    predicate_source: PreparationPredicateSource,
    include_predicate_capability_profile: bool,
    index_policy: Option<IndexCompilePolicy>,
}

impl ExecutionPreparation {
    /// Build execution preparation once for one validated access-planned query.
    #[must_use]
    pub(in crate::db::executor) fn from_plan(
        plan: &AccessPlannedQuery,
        slot_map: Option<Vec<usize>>,
    ) -> Self {
        Self::build(
            plan,
            slot_map,
            PreparationBuildConfig {
                predicate_source: PreparationPredicateSource::ExecutionPreparation,
                include_predicate_capability_profile: true,
                index_policy: Some(IndexCompilePolicy::StrictAllOrNone),
            },
        )
    }

    /// Build the lighter planner preparation needed by scalar covering-route
    /// admission during load route derivation.
    ///
    /// This path keeps the execution-preparation predicate view plus the
    /// indexability capability snapshot used by covering-read eligibility, but
    /// it intentionally skips strict/conservative index predicate program
    /// compilation because scalar load route planning does not consume them.
    #[must_use]
    pub(in crate::db::executor) fn from_covering_route_plan(
        plan: &AccessPlannedQuery,
        slot_map: Option<Vec<usize>>,
    ) -> Self {
        Self::build(
            plan,
            slot_map,
            PreparationBuildConfig {
                predicate_source: PreparationPredicateSource::ExecutionPreparation,
                include_predicate_capability_profile: true,
                index_policy: None,
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
        plan: &AccessPlannedQuery,
        slot_map: Option<Vec<usize>>,
    ) -> Self {
        Self::build(
            plan,
            slot_map,
            PreparationBuildConfig {
                predicate_source: PreparationPredicateSource::EffectiveRuntime,
                include_predicate_capability_profile: false,
                index_policy: Some(IndexCompilePolicy::ConservativeSubset),
            },
        )
    }

    #[must_use]
    pub(in crate::db::executor) const fn compiled_predicate(&self) -> Option<&PredicateProgram> {
        self.compiled_predicate.as_ref()
    }

    #[must_use]
    pub(in crate::db::executor) const fn effective_runtime_filter_program(
        &self,
    ) -> Option<&EffectiveRuntimeFilterProgram> {
        self.effective_runtime_filter_program.as_ref()
    }

    #[must_use]
    pub(in crate::db::executor) fn prepared_index_program(
        &self,
        policy: IndexCompilePolicy,
    ) -> Option<&IndexPredicateProgram> {
        self.index_program
            .as_ref()
            .filter(|prepared| prepared.policy == policy)
            .and_then(|prepared| prepared.program.as_ref())
    }

    /// Reuse a completed result, or compile an unprepared policy at the same
    /// owner used by eager preparation. `None` never triggers a second compile
    /// when that policy already completed successfully without a program.
    #[must_use]
    pub(in crate::db::executor) fn resolve_index_program(
        &self,
        policy: IndexCompilePolicy,
    ) -> Option<Cow<'_, IndexPredicateProgram>> {
        PreparedIndexProgram::resolve(self.index_program.as_ref(), policy, || {
            compile_index_program_for_preparation(
                self.compiled_predicate.as_ref(),
                self.compile_targets.as_deref(),
                self.slot_map.as_deref(),
                policy,
            )
        })
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

    // Build the canonical preparation bundle once from one planner predicate
    // source plus the caller's requested index-program/capability outputs.
    fn build(
        plan: &AccessPlannedQuery,
        slot_map: Option<Vec<usize>>,
        config: PreparationBuildConfig,
    ) -> Self {
        // Phase 1: borrow the planner-compiled predicate projection once.
        let compiled_predicate = match config.predicate_source {
            PreparationPredicateSource::ExecutionPreparation => {
                plan.execution_preparation_compiled_predicate().cloned()
            }
            PreparationPredicateSource::EffectiveRuntime => {
                plan.effective_runtime_compiled_predicate().cloned()
            }
        };
        let effective_runtime_filter_program = matches!(
            config.predicate_source,
            PreparationPredicateSource::EffectiveRuntime
        )
        .then(|| plan.effective_runtime_filter_program().cloned())
        .flatten();
        let compile_targets = index_compile_targets_for_model_plan(plan);

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

        // Phase 3: retain completion for the one policy this boundary needs,
        // including an unsupported result. Lightweight route preparation asks
        // for no program and leaves on-demand compilation available.
        let index_program = config.index_policy.map(|policy| PreparedIndexProgram {
            policy,
            program: compile_index_program_for_preparation(
                compiled_predicate.as_ref(),
                compile_targets.as_deref(),
                slot_map.as_deref(),
                policy,
            ),
        });

        Self {
            compiled_predicate,
            effective_runtime_filter_program,
            compile_targets,
            index_program,
            predicate_capability_profile,
            slot_map,
        }
    }
}

impl std::fmt::Debug for ExecutionPreparation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ExecutionPreparation(..)")
    }
}

/// Resolve covering strict-predicate compatibility without building predicate
/// capability state when the residual-filter contract already decides it.
#[must_use]
pub(in crate::db::executor) fn covering_strict_predicate_compatible_for_plan(
    plan: &AccessPlannedQuery,
) -> bool {
    if !plan.has_residual_filter_predicate() {
        return covering_strict_predicate_compatible(plan, None);
    }

    let execution_preparation =
        ExecutionPreparation::from_covering_route_plan(plan, slot_map_for_model_plan(plan));
    covering_strict_predicate_compatible(
        plan,
        execution_preparation
            .predicate_capability_profile()
            .map(PredicateCapabilityProfile::index),
    )
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

/// Project one planner-frozen structural slot map from one access-planned query.
pub(in crate::db::executor) fn slot_map_for_model_plan(
    plan: &AccessPlannedQuery,
) -> Option<Vec<usize>> {
    plan.slot_map().map(<[usize]>::to_vec)
}

// Project one planner-frozen key-item-aware compile target list from the plan.
fn index_compile_targets_for_model_plan(
    plan: &AccessPlannedQuery,
) -> Option<Vec<IndexCompileTarget>> {
    plan.index_compile_targets()
        .map(<[IndexCompileTarget]>::to_vec)
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(ExecutionPreparation {
Self{compiled_predicate,effective_runtime_filter_program,compile_targets,index_program,predicate_capability_profile,slot_map} => [compiled_predicate,effective_runtime_filter_program,compile_targets,index_program,predicate_capability_profile,slot_map],
});
crate::retained::retained_fields!(PreparedIndexProgram {
Self{policy: _,program} => [program],
});
