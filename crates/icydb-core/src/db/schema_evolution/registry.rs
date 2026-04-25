//! Module: db::schema_evolution::registry
//! Responsibility: completed schema-migration registry contract.
//! Does not own: durable commit layout or in-progress migration cursor storage.
//! Boundary: schema-evolution guard state consumed before migration execution.

use crate::db::identity::EntityName;
use std::collections::BTreeSet;

///
/// MigrationRegistryKey
///
/// MigrationRegistryKey names one completed schema migration by canonical
/// migration identity plus monotonic version.
/// The registry uses this key to reject repeat execution before the lower
/// migration engine is invoked.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MigrationRegistryKey {
    migration_id: EntityName,
    version: u64,
}

impl MigrationRegistryKey {
    /// Build one registry key from canonical migration identity.
    #[must_use]
    pub const fn new(migration_id: EntityName, version: u64) -> Self {
        Self {
            migration_id,
            version,
        }
    }

    /// Return the canonical migration identity.
    #[must_use]
    pub const fn migration_id(self) -> EntityName {
        self.migration_id
    }

    /// Return the monotonic migration version.
    #[must_use]
    pub const fn version(self) -> u64 {
        self.version
    }
}

///
/// MigrationRegistry
///
/// MigrationRegistry tracks schema migrations that have completed above the
/// lower `db::migration` execution engine.
/// This first implementation is an explicit owner object so no commit/storage
/// layout changes are required by the schema-evolution slice.
///

#[derive(Clone, Debug, Default)]
pub struct MigrationRegistry {
    completed: BTreeSet<MigrationRegistryKey>,
}

impl MigrationRegistry {
    /// Build one empty completed-migration registry.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            completed: BTreeSet::new(),
        }
    }

    /// Return whether the migration id/version is already recorded as complete.
    #[must_use]
    pub fn is_applied(&self, migration_id: EntityName, version: u64) -> bool {
        self.completed
            .contains(&MigrationRegistryKey::new(migration_id, version))
    }

    /// Record one migration id/version as complete.
    pub fn record_applied(&mut self, migration_id: EntityName, version: u64) {
        self.completed
            .insert(MigrationRegistryKey::new(migration_id, version));
    }

    /// Return the number of completed migration keys tracked by this registry.
    #[must_use]
    pub fn len(&self) -> usize {
        self.completed.len()
    }

    /// Return whether this registry currently has no completed migration keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.completed.is_empty()
    }
}
