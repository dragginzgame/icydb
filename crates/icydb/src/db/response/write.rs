use crate::{
    traits::{EntityKind, EntityValue},
    types::Id,
};
use icydb_core::db::WriteBatchResponse as CoreWriteBatchResponse;

///
/// WriteResponse
///
/// Facade over a single write result with explicit accessors.
/// Returned IDs are public identifiers used for correlation, reporting, and lookup.
///

#[derive(Debug)]
pub struct WriteResponse<E: EntityKind> {
    entity: E,
}

impl<E: EntityKind> WriteResponse<E> {
    /// Construct a facade write response from a stored entity.
    #[must_use]
    pub const fn new(entity: E) -> Self {
        Self { entity }
    }

    /// Return the stored entity.
    #[must_use]
    pub fn entity(self) -> E {
        self.entity
    }

    /// Return the stored entity's primary identity
    #[must_use]
    pub fn id(&self) -> Id<E>
    where
        E: EntityValue,
    {
        self.entity.id()
    }
}

///
/// WriteBatchResponse
///
/// Facade over batch write results with explicit accessors.
///

#[derive(Debug)]
pub struct WriteBatchResponse<E: EntityKind> {
    entities: Vec<E>,
}

impl<E: EntityKind> WriteBatchResponse<E> {
    /// Construct a facade batch response from stored entities.
    #[must_use]
    pub const fn new(entities: Vec<E>) -> Self {
        Self { entities }
    }

    /// Construct a facade batch response from the core response.
    #[must_use]
    pub fn from_core(inner: CoreWriteBatchResponse<E>) -> Self {
        Self {
            entities: inner.into_iter().collect(),
        }
    }

    /// Return the number of entries.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.entities.len()
    }

    /// Returns `true` if the batch is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entities.is_empty()
    }

    /// Return all stored entities.
    #[must_use]
    pub fn entities(self) -> Vec<E> {
        self.entities
    }

    /// Borrow an iterator over primary keys for correlation, reporting, and lookup.
    pub fn ids(&self) -> impl Iterator<Item = Id<E>> + '_
    where
        E: EntityValue,
    {
        self.entities.iter().map(EntityValue::id)
    }
}

impl<E: EntityKind> WriteBatchResponse<E> {
    pub fn iter(&self) -> std::slice::Iter<'_, E> {
        self.entities.iter()
    }
}

impl<'a, E: EntityKind> IntoIterator for &'a WriteBatchResponse<E> {
    type Item = &'a E;
    type IntoIter = std::slice::Iter<'a, E>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
