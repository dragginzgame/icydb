mod contracts;
mod entry;
pub(in crate::db) mod envelope;
mod fingerprint;
mod key;
mod plan;
pub(in crate::db) mod predicate;
mod range;
mod store;

pub(in crate::db) use crate::db::direction::Direction;
pub(in crate::db) use contracts::{PrimaryKeyEquivalenceError, primary_key_matches_value};
pub(in crate::db) use entry::{
    IndexEntry, IndexEntryCorruption, IndexEntryEncodeError, MAX_INDEX_ENTRY_BYTES, RawIndexEntry,
};
pub(in crate::db) use envelope::{KeyEnvelope, continuation_advances_from_ordering};
pub(crate) use fingerprint::hash_value;
pub(in crate::db) use key::{EncodedValue, IndexId, IndexKey, IndexKeyKind, RawIndexKey};
pub(in crate::db) use plan::plan_index_mutation_for_entity;
pub(in crate::db) use range::{
    IndexRangeNotIndexableReasonScope, continuation_advanced, envelope_is_empty,
    map_index_range_not_indexable_reason, raw_bounds_for_semantic_index_component_range,
    raw_keys_for_encoded_prefix, raw_keys_for_encoded_prefix_with_kind, resume_bounds_from_refs,
};
pub use store::IndexStore;
