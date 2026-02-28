use crate::{
    db::{
        executor::load::LoadExecutor,
        index::{IndexCompilePolicy, IndexPredicateProgram, compile_index_program},
        query::plan::AccessPlannedQuery,
        query::predicate::runtime::PredicateProgram,
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
    compiled_predicate: Option<PredicateProgram>,
    slot_map: Option<Vec<usize>>,
    strict_mode: Option<IndexPredicateProgram>,
}

impl ExecutionPreparation {
    /// Build execution preparation once for one validated access-planned query.
    #[must_use]
    pub(in crate::db::executor) fn for_plan<E>(plan: &AccessPlannedQuery<E::Key>) -> Self
    where
        E: EntityKind + EntityValue,
    {
        let compiled_predicate = plan
            .scalar_plan()
            .predicate
            .as_ref()
            .map(PredicateProgram::compile::<E>);
        let slot_map = LoadExecutor::<E>::resolved_index_slots_for_access_path(&plan.access);
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
