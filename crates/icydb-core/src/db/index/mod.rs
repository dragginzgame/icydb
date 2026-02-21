mod entry;
mod fingerprint;
mod key;
mod plan;
mod range;
mod store;

pub(in crate::db) use entry::{
    IndexEntry, IndexEntryCorruption, IndexEntryEncodeError, MAX_INDEX_ENTRY_BYTES, RawIndexEntry,
};
pub(crate) use fingerprint::hash_value;
#[cfg(test)]
pub(in crate::db) use key::encode_canonical_index_component;
pub(in crate::db) use key::{EncodedValue, IndexId, IndexKey, IndexKeyKind, RawIndexKey};
pub(in crate::db) use plan::plan_index_mutation_for_entity;
pub(crate) use range::Direction;
pub(in crate::db) use range::{
    continuation_advanced, envelope_is_empty, map_bound_encode_error,
    raw_bounds_for_encoded_index_component_range, raw_bounds_for_index_component_range,
    resume_bounds,
};
pub use store::IndexStore;
