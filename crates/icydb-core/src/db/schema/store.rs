//! Module: db::schema::store
//! Responsibility: stable-memory owner for persisted schema metadata.
//! Does not own: reconciliation policy or generated schema proposal construction.
//! Boundary: provides the third per-store stable memory alongside row and index stores.

use canic_cdk::structures::{DefaultMemoryImpl, memory::VirtualMemory};

///
/// SchemaStore
///
/// Stable-memory handle reserved for one store's persisted schema metadata.
/// The concrete `__icydb_schema` map is introduced by reconciliation work; this
/// type establishes the generated memory boundary without changing row/index IO.
///

pub struct SchemaStore {
    memory: VirtualMemory<DefaultMemoryImpl>,
}

impl SchemaStore {
    /// Initialize the schema store with the provided backing memory.
    #[must_use]
    pub const fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self { memory }
    }

    /// Borrow the backing stable memory reserved for schema metadata.
    #[must_use]
    pub const fn memory(&self) -> &VirtualMemory<DefaultMemoryImpl> {
        &self.memory
    }
}
