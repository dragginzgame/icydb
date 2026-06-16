//! Module: catalog::snapshot
//! Responsibility: owner-local accepted schema snapshot representation.
//! Does not own: generated entity models or physical row layout.
//! Boundary: records accepted runtime authority after admission validation.

///
/// AcceptedSchemaSnapshot
///
/// Owner-local accepted schema fact stored by the catalog module.
/// This type stays `pub(crate)` so callers must go through admission reports
/// and catalog queries instead of depending on storage internals.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AcceptedSchemaSnapshot {
    entity_name: String,
    version: u32,
}

impl AcceptedSchemaSnapshot {
    /// Build one accepted snapshot from already-validated admission input.
    #[must_use]
    pub(crate) fn new(entity_name: &str, version: u32) -> Self {
        Self {
            entity_name: entity_name.to_owned(),
            version,
        }
    }

    /// Return the entity name covered by this accepted snapshot.
    #[must_use]
    pub(crate) fn entity_name(&self) -> &str {
        &self.entity_name
    }

    /// Return the accepted schema version.
    #[must_use]
    pub(crate) const fn version(&self) -> u32 {
        self.version
    }
}
