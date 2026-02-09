use crate::{
    traits::{AsView, EntityKind, EntityValue},
    types::Id,
};
use icydb_core::db::response::{
    WriteBatchResponse as CoreWriteBatchResponse, WriteResponse as CoreWriteResponse,
};

///
/// WriteResponse
///
/// Facade over a single write result with explicit accessors.
/// Returned IDs are public identifiers used for correlation, reporting, and lookup.
///

#[derive(Debug)]
pub struct WriteResponse<E: EntityKind> {
    inner: CoreWriteResponse<E>,
}

impl<E: EntityKind> WriteResponse<E> {
    /// Construct a facade write response from a stored entity.
    #[must_use]
    pub const fn new(entity: E) -> Self {
        Self::from_core(CoreWriteResponse::new(entity))
    }

    /// Construct a facade write response from the core response.
    #[must_use]
    pub const fn from_core(inner: CoreWriteResponse<E>) -> Self {
        Self { inner }
    }

    /// Return the stored entity.
    #[must_use]
    pub fn entity(self) -> E {
        self.inner.entity()
    }

    /// Return the stored entity's primary identity
    #[must_use]
    pub fn id(&self) -> Id<E>
    where
        E: EntityValue,
    {
        self.inner.id()
    }

    /// Return the stored entity as its view type.
    #[must_use]
    pub fn view(&self) -> <E as AsView>::ViewType
    where
        E: AsView,
    {
        self.inner.view()
    }
}

///
/// WriteBatchResponse
///
/// Facade over batch write results with explicit accessors.
///

#[derive(Debug)]
pub struct WriteBatchResponse<E: EntityKind> {
    entries: Vec<WriteResponse<E>>,
}

impl<E: EntityKind> WriteBatchResponse<E> {
    /// Construct a facade batch response from stored entities.
    #[must_use]
    pub fn new(entities: Vec<E>) -> Self {
        Self {
            entries: entities.into_iter().map(WriteResponse::new).collect(),
        }
    }

    /// Construct a facade batch response from the core response.
    #[must_use]
    pub fn from_core(inner: CoreWriteBatchResponse<E>) -> Self {
        Self {
            entries: inner.into_iter().map(WriteResponse::from_core).collect(),
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
        self.entries.iter().map(WriteResponse::id).collect()
    }

    /// Return all views.
    #[must_use]
    pub fn views(&self) -> Vec<<E as AsView>::ViewType>
    where
        E: AsView,
    {
        self.entries.iter().map(WriteResponse::view).collect()
    }
}

impl<E: EntityKind> WriteBatchResponse<E> {
    pub fn iter(&self) -> std::slice::Iter<'_, WriteResponse<E>> {
        self.entries.iter()
    }
}

impl<'a, E: EntityKind> IntoIterator for &'a WriteBatchResponse<E> {
    type Item = &'a WriteResponse<E>;
    type IntoIter = std::slice::Iter<'a, WriteResponse<E>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
