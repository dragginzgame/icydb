pub mod entry;
pub mod fingerprint;
pub mod key;
pub mod plan;
pub mod store;

#[cfg(test)]
mod tests;

pub use entry::{
    IndexEntry, IndexEntryCorruption, IndexEntryEncodeError, MAX_INDEX_ENTRY_BYTES,
    MAX_INDEX_ENTRY_KEYS, RawIndexEntry,
};
pub use key::{IndexId, IndexKey, RawIndexKey};
pub use store::{
    IndexInsertError, IndexInsertOutcome, IndexRemoveOutcome, IndexStore, IndexStoreRegistry,
};
