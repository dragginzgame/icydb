use crate::{
    db::{
        executor::{
            IndexPredicateCompileMode, compile_index_predicate_program_from_slots,
            load::LoadExecutor, predicate_runtime::PredicateFieldSlots,
        },
        index::IndexPredicateProgram,
        query::plan::AccessPlannedQuery,
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
                compile_index_predicate_program_from_slots(
                    compiled_predicate,
                    slot_map,
                    IndexPredicateCompileMode::StrictAllOrNone,
                )
            }
            (Some(_) | None, None) | (None, Some(_)) => None,
        };
        Self {
            compiled_predicate,
            slot_map,
            strict_mode,
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
}
