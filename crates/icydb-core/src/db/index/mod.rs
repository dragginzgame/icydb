//! Module: index
//! Responsibility: index key encoding, index entry modeling, and index-store access.
//! Does not own: query planning policy, commit orchestration, or relation semantics.
//! Boundary: executor/query/commit may depend on index; index depends on data primitives.

mod contracts;
mod entry;
pub(in crate::db) mod envelope;
mod key;
mod plan;
pub(in crate::db) mod predicate;
mod range;
mod scan;
mod store;

pub(in crate::db) use contracts::{PrimaryKeyEquivalenceError, primary_key_matches_value};
pub(in crate::db) use entry::{
    IndexEntry, IndexEntryCorruption, IndexEntryEncodeError, MAX_INDEX_ENTRY_BYTES, RawIndexEntry,
};
pub(in crate::db) use envelope::{
    KeyEnvelope, anchor_within_envelope, continuation_advanced,
    continuation_advances_from_ordering, resume_bounds_from_refs,
};
pub(in crate::db) use key::{EncodedValue, IndexId, IndexKey, IndexKeyKind, RawIndexKey};
pub(in crate::db) use plan::{IndexEntryReader, PrimaryRowReader, plan_index_mutation_for_entity};
pub(in crate::db) use predicate::{
    IndexCompareOp, IndexCompilePolicy, IndexLiteral, IndexPredicateProgram, compile_index_program,
};
pub(in crate::db) use range::{
    IndexRangeBoundEncodeError, envelope_is_empty, raw_bounds_for_semantic_index_component_range,
    raw_keys_for_encoded_prefix, raw_keys_for_encoded_prefix_with_kind,
};
pub use store::IndexStore;
