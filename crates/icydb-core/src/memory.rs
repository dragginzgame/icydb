//! Module: memory
//! Responsibility: resolve physical IDs from committed logical allocation authority.
//! Does not own: allocation, caching, placement, or schema authority.
//! Boundary: runtime identity consumers to the shared ic-memory capability.

use ic_memory::{RuntimeOpenError, StableKey, committed_allocations};

pub(crate) fn committed_memory_id(key: &str) -> Result<u8, RuntimeOpenError> {
    let allocations = committed_allocations()?;
    let key = StableKey::parse(key)?;
    let slot = allocations
        .slot_for(&key)
        .ok_or_else(|| RuntimeOpenError::StableKeyNotCommitted(key.to_string()))?;
    slot.memory_manager_id().map_err(Into::into)
}
