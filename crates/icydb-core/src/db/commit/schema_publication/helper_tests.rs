//! Test-only helpers for the parent module.

use super::*;

#[cfg(feature = "sql")]
pub(in crate::db) fn interrupt_next_schema_publication_for_tests(
    interruption: SchemaPublicationInterruption,
) {
    NEXT_SCHEMA_PUBLICATION_INTERRUPTION.with(|next| next.set(Some(interruption)));
}

#[cfg(feature = "sql")]
pub(super) fn interrupt_schema_publication_at(
    interruption: SchemaPublicationInterruption,
) -> Result<(), InternalError> {
    NEXT_SCHEMA_PUBLICATION_INTERRUPTION.with(|next| {
        if next.get() == Some(interruption) {
            next.set(None);
            Err(InternalError::executor_invariant())
        } else {
            Ok(())
        }
    })
}

/// Test-only returned-error cuts through the real index publication path.
#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaPublicationInterruption {
    MarkerPersisted,
    IndexDeleted,
    IndexInserted,
}

#[cfg(feature = "sql")]
thread_local! {
    pub(super) static NEXT_SCHEMA_PUBLICATION_INTERRUPTION: std::cell::Cell<Option<SchemaPublicationInterruption>> =
        const { std::cell::Cell::new(None) };
}
