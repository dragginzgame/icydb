//! Module: db::executor::authority
//! Responsibility: structural entity authority bundles for executor/runtime boundaries.
//! Does not own: query semantics, store access, or typed API entrypoints.
//! Boundary: replaces ad hoc `E::MODEL` / `E::ENTITY_TAG` / `E::PATH` threading in execution prep.

use crate::{
    model::entity::EntityModel,
    traits::{EntityKind, Path},
    types::EntityTag,
};

///
/// EntityAuthority
///
/// EntityAuthority is the canonical structural entity-identity bundle used by
/// executor runtime preparation once typed API boundaries have resolved the
/// concrete entity type.
/// It keeps model, entity-tag, and store path authority aligned while deriving
/// the entity path from the model itself so execution-core code does not pass
/// duplicated metadata independently.
///

#[derive(Clone, Copy, Debug)]
pub struct EntityAuthority {
    model: &'static EntityModel,
    entity_tag: EntityTag,
    store_path: &'static str,
}

impl EntityAuthority {
    /// Build structural authority from explicit runtime metadata.
    #[must_use]
    pub const fn new(
        model: &'static EntityModel,
        entity_tag: EntityTag,
        store_path: &'static str,
    ) -> Self {
        Self {
            model,
            entity_tag,
            store_path,
        }
    }

    /// Build structural authority from one resolved entity type.
    #[must_use]
    pub const fn for_type<E: EntityKind>() -> Self {
        Self::new(E::MODEL, E::ENTITY_TAG, E::Store::PATH)
    }

    /// Borrow structural entity model authority.
    #[must_use]
    pub const fn model(&self) -> &'static EntityModel {
        self.model
    }

    /// Borrow structural entity-tag authority.
    #[must_use]
    pub const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }

    /// Borrow structural entity-path authority.
    #[must_use]
    pub const fn entity_path(&self) -> &'static str {
        self.model.path()
    }

    /// Borrow structural store-path authority.
    #[must_use]
    pub const fn store_path(&self) -> &'static str {
        self.store_path
    }
}
