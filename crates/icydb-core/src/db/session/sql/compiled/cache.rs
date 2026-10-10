//! Compiled SQL schema fingerprint identity.
//! Does not own: compiled command variants or execution context handoff.

use crate::db::{commit::CommitSchemaFingerprint, session::AcceptedSchemaCatalogContext};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct SqlCompiledSchemaFingerprint {
    method_version: u8,
    fingerprint: CommitSchemaFingerprint,
}

impl SqlCompiledSchemaFingerprint {
    #[must_use]
    pub(in crate::db) const fn new(
        method_version: u8,
        fingerprint: CommitSchemaFingerprint,
    ) -> Self {
        Self {
            method_version,
            fingerprint,
        }
    }

    #[must_use]
    pub(in crate::db) fn from_catalog(catalog: &AcceptedSchemaCatalogContext) -> Self {
        Self::new(catalog.fingerprint_method_version(), catalog.fingerprint())
    }
}
