//! Test-only constructors and adapters for the parent module.

use super::*;

impl IndexId {
    /// Build one runtime index identity from data-only identity components.
    #[must_use]
    pub(crate) const fn new(entity_tag: EntityTag, ordinal: u16) -> Self {
        Self {
            entity_tag,
            ordinal,
            generation: 0,
        }
    }
}
