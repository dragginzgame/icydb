//! Module: index
//! Responsibility: index key encoding, index entry modeling, and index-store access.
//! Does not own: query planning policy, commit orchestration, or relation semantics.
//! Boundary: executor/query/commit may depend on index; index depends on data primitives.

mod entry;
pub(in crate::db) mod envelope;
mod key;
mod pk_equivalence;
mod plan;
pub(in crate::db) mod predicate;
mod range;
mod scan;
mod store;

pub(in crate::db) use entry::{
    IndexEntry, IndexEntryCorruption, IndexEntryEncodeError, MAX_INDEX_ENTRY_BYTES, RawIndexEntry,
};
pub(in crate::db) use envelope::{
    KeyEnvelope, continuation_advanced, key_within_envelope, resume_bounds_from_refs,
    validate_index_scan_continuation_advancement, validate_index_scan_continuation_envelope,
};
pub(in crate::db) use key::{
    EncodedValue, IndexId, IndexKey, IndexKeyKind, RawIndexKey, derive_index_expression_value,
};
pub(in crate::db) use pk_equivalence::{PrimaryKeyEquivalenceError, primary_key_matches_value};
pub(in crate::db) use plan::{
    IndexEntryReader, IndexMutationPlan, PrimaryRowReader, SealedIndexEntryReader,
    SealedPrimaryRowReader, SealedStructuralIndexEntryReader, StructuralIndexEntryReader,
    compile_index_membership_predicate, index_key_for_slot_reader_with_membership,
    plan_index_mutation_for_slot_reader,
};
pub(in crate::db) use predicate::{
    IndexCompareOp, IndexCompilePolicy, IndexLiteral, IndexPredicateProgram,
    canonical_index_predicate, compile_index_program,
};
pub(in crate::db) use range::{
    IndexRangeBoundEncodeError, envelope_is_empty, raw_bounds_for_semantic_index_component_range,
    raw_keys_for_encoded_prefix, raw_keys_for_encoded_prefix_with_kind,
};
pub use store::IndexStore;
