mod key;
mod row;
mod storage_key;
mod store;

// re-exports (Tier-3 → Tier-2 boundary)
pub(crate) use key::{DataKey, RawDataKey};
pub(crate) use row::{DataRow, MAX_ROW_BYTES, RawRow};
pub(crate) use storage_key::{StorageKey, StorageKeyEncodeError};
pub use store::DataStore;
