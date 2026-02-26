use crate::db::{
    executor::{
        ExecutionKernel, IndexPredicateCompileMode, PredicateFieldSlots,
        compile_index_predicate_program_from_slots as compile_index_program_from_slots,
    },
    index::IndexPredicateProgram,
};

impl ExecutionKernel {
    // Compile one optional index-only predicate program from pre-resolved slots.
    // This keeps the kernel compile entrypoint stable while the compile logic
    // is owned by the shared access compile contract.
    pub(in crate::db::executor) fn compile_index_predicate_program_from_slots(
        predicate_slots: &PredicateFieldSlots,
        index_slots: &[usize],
        mode: IndexPredicateCompileMode,
    ) -> Option<IndexPredicateProgram> {
        compile_index_program_from_slots(predicate_slots, index_slots, mode)
    }
}
