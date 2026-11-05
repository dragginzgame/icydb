//! Test-only constructors and adapters for the parent module.

use super::*;

impl AcceptedCompositeCatalog {
    #[must_use]
    pub(in crate::db) const fn empty() -> Self {
        Self {
            by_id: BTreeMap::new(),
            id_by_path: BTreeMap::new(),
        }
    }

    #[must_use]
    pub(in crate::db) fn type_id(&self, path: &str) -> Option<CompositeTypeId> {
        self.id_by_path.get(path).copied()
    }
}
