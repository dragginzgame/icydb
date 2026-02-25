use crate::db::{
    executor::ExecutionKernel, index::predicate::IndexPredicateProgram,
    query::predicate::PredicateFieldSlots,
};

///
/// IndexPredicateCompileMode
///
/// Predicate compile policy for index-only prefilter programs.
/// `ConservativeSubset` keeps load behavior by compiling safe AND-subsets.
/// `StrictAllOrNone` compiles only when every predicate node is supported.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum IndexPredicateCompileMode {
    ConservativeSubset,
    StrictAllOrNone,
}

impl ExecutionKernel {
    // Compile one optional index-only predicate program from pre-resolved slots.
    // This is the single compile-mode switch boundary for subset vs strict policy.
    pub(in crate::db::executor) fn compile_index_predicate_program_from_slots(
        predicate_slots: &PredicateFieldSlots,
        index_slots: &[usize],
        mode: IndexPredicateCompileMode,
    ) -> Option<IndexPredicateProgram> {
        match mode {
            IndexPredicateCompileMode::ConservativeSubset => {
                predicate_slots.compile_index_program(index_slots)
            }
            IndexPredicateCompileMode::StrictAllOrNone => {
                predicate_slots.compile_index_program_strict(index_slots)
            }
        }
    }
}
