use crate::{
    traits::{AsView, EntityValue},
    types::Id,
    view::View as EntityView,
};

///
/// WriteResponse
///
/// Result of a single write operation.
/// Provides explicit access to the stored entity and its identifier.
///

#[derive(Debug)]
pub struct WriteResponse<E> {
    entity: E,
}

impl<E> WriteResponse<E> {
    /// Construct a write response from the stored entity.
    #[must_use]
    pub const fn new(entity: E) -> Self {
        Self { entity }
    }

    /// Return the stored entity.
    #[must_use]
    pub fn entity(self) -> E {
        self.entity
    }

    /// Return the stored entity's primary key.
    ///
    /// Returned IDs are public correlation/reporting/lookup values, not authority-bearing tokens.
    #[must_use]
    pub fn key(&self) -> Id<E>
    where
        E: EntityValue,
    {
        self.entity.id()
    }

    /// Return the stored entity as its view type.
    #[must_use]
    pub fn view(&self) -> EntityView<E>
    where
        E: AsView,
    {
        self.entity.as_view()
    }
}

///
/// WriteBatchResponse
///
/// Result of a batch write operation.
/// Provides explicit access to stored entities and their identifiers.
///

#[derive(Debug)]
pub struct WriteBatchResponse<E> {
    entries: Vec<WriteResponse<E>>,
}

impl<E> WriteBatchResponse<E> {
    /// Construct a batch response from stored entities.
    #[must_use]
    pub fn new(entities: Vec<E>) -> Self {
        Self {
            entries: entities.into_iter().map(WriteResponse::new).collect(),
        }
    }

    /// Return all write responses.
    #[must_use]
    pub fn entries(&self) -> &[WriteResponse<E>] {
        &self.entries
    }

    /// Return the number of entries.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the batch is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Return all stored entities.
    #[must_use]
    pub fn entities(self) -> Vec<E> {
        self.entries
            .into_iter()
            .map(WriteResponse::entity)
            .collect()
    }

    /// Return all primary keys for correlation, reporting, and lookup.
    #[must_use]
    pub fn ids(&self) -> Vec<Id<E>>
    where
        E: EntityValue,
    {
        self.entries.iter().map(WriteResponse::key).collect()
    }

    /// Return all views.
    #[must_use]
    pub fn views(&self) -> Vec<EntityView<E>>
    where
        E: AsView,
    {
        self.entries.iter().map(WriteResponse::view).collect()
    }
}

impl<E> IntoIterator for WriteBatchResponse<E> {
    type Item = WriteResponse<E>;
    type IntoIter = std::vec::IntoIter<WriteResponse<E>>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl<E> WriteBatchResponse<E> {
    pub fn iter(&self) -> std::slice::Iter<'_, WriteResponse<E>> {
        self.entries.iter()
    }
}

impl<'a, E> IntoIterator for &'a WriteBatchResponse<E> {
    type Item = &'a WriteResponse<E>;
    type IntoIter = std::slice::Iter<'a, WriteResponse<E>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
