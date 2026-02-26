use crate::{
    db::{
        access::IndexPredicateProgram,
        executor::{ExecutionKernel, IndexPredicateCompileMode, load::LoadExecutor},
        plan::AccessPlannedQuery,
        query::predicate::PredicateFieldSlots,
    },
    traits::{EntityKind, EntityValue},
};

///
/// ExecutionPreparation
///
/// Canonical one-shot predicate/index compilation bundle derived from one plan.
/// Build once at the execution boundary and reuse across route/load/delete/aggregate paths.
///

#[derive(Clone)]
pub(in crate::db::executor) struct ExecutionPreparation {
    compiled_predicate: Option<PredicateFieldSlots>,
    slot_map: Option<Vec<usize>>,
    strict_mode: Option<IndexPredicateProgram>,
    index_coverage: bool,
}

impl ExecutionPreparation {
    /// Build execution preparation once for one validated access-planned query.
    #[must_use]
    pub(in crate::db::executor) fn for_plan<E>(plan: &AccessPlannedQuery<E::Key>) -> Self
    where
        E: EntityKind + EntityValue,
    {
        let compiled_predicate = plan
            .predicate
            .as_ref()
            .map(PredicateFieldSlots::resolve::<E>);
        let slot_map = LoadExecutor::<E>::resolved_index_slots_for_access_path(&plan.access);
        let strict_mode = match (compiled_predicate.as_ref(), slot_map.as_deref()) {
            (Some(compiled_predicate), Some(slot_map)) => {
                ExecutionKernel::compile_index_predicate_program_from_slots(
                    compiled_predicate,
                    slot_map,
                    IndexPredicateCompileMode::StrictAllOrNone,
                )
            }
            (Some(_) | None, None) | (None, Some(_)) => None,
        };
        let index_coverage = LoadExecutor::<E>::predicate_slots_fully_covered_by_index_slots(
            compiled_predicate.as_ref(),
            slot_map.as_deref(),
        );

        Self {
            compiled_predicate,
            slot_map,
            strict_mode,
            index_coverage,
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn compiled_predicate(&self) -> Option<&PredicateFieldSlots> {
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

    #[must_use]
    pub(in crate::db::executor) const fn index_coverage(&self) -> bool {
        self.index_coverage
    }
}
