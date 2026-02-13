use crate::db::{index::store::IndexStore, store::StoreRegistry};
use derive_more::{Deref, DerefMut};

///
/// IndexStoreRegistry
///

#[derive(Deref, DerefMut)]
pub struct IndexStoreRegistry(StoreRegistry<IndexStore>);

impl IndexStoreRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self(StoreRegistry::new())
    }
}

impl Default for IndexStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}
