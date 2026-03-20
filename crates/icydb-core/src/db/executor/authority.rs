//! Module: db::executor::authority
//! Responsibility: structural entity authority bundles for executor/runtime boundaries.
//! Does not own: query semantics, store access, or typed API entrypoints.
//! Boundary: replaces ad hoc `E::MODEL` / `E::ENTITY_TAG` / `E::PATH` threading in execution prep.

use crate::{model::entity::EntityModel, traits::EntityKind, types::EntityTag};

///
/// EntityAuthority
///
/// EntityAuthority is the canonical structural entity-identity bundle used by
/// executor runtime preparation once typed API boundaries have resolved the
/// concrete entity type.
/// It keeps model, entity-tag, and path authority aligned so execution-core
/// code does not pass those pieces independently.
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db::executor) struct EntityAuthority {
    model: &'static EntityModel,
    entity_tag: EntityTag,
    entity_path: &'static str,
}

impl EntityAuthority {
    /// Build structural authority from one resolved entity type.
    #[must_use]
    pub(in crate::db::executor) const fn for_type<E: EntityKind>() -> Self {
        Self {
            model: E::MODEL,
            entity_tag: E::ENTITY_TAG,
            entity_path: E::PATH,
        }
    }

    /// Borrow structural entity model authority.
    #[must_use]
    pub(in crate::db::executor) const fn model(&self) -> &'static EntityModel {
        self.model
    }

    /// Borrow structural entity-tag authority.
    #[must_use]
    pub(in crate::db::executor) const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }

    /// Borrow structural entity-path authority.
    #[must_use]
    pub(in crate::db::executor) const fn entity_path(&self) -> &'static str {
        self.entity_path
    }
}
