//! Test-only constructors and adapters for the parent module.

use super::*;

impl SchemaRowLayout {
    /// Build the sole initial physical layout for a new accepted entity.
    #[must_use]
    pub(in crate::db) const fn initial(field_to_slot: Vec<(FieldId, SchemaFieldSlot)>) -> Self {
        Self::single_version(RowLayoutVersion::INITIAL, field_to_slot)
    }
}
