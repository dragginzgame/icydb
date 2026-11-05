//! Test-only constructors and adapters for the parent module.

use super::*;

impl<'a> AcceptedFieldPersistenceContract<'a> {
    /// Build an explicit paired contract for focused data-layer tests.
    pub(in crate::db) fn new_for_tests(
        value_catalog: &'a AcceptedValueCatalogHandle,
        field: AcceptedFieldDecodeContract<'a>,
    ) -> Result<Self, EnumCatalogBuildError> {
        Self::new(value_catalog, field)
    }
}
