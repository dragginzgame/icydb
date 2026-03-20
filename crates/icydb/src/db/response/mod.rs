mod paged;
mod write;

use crate::{error::Error, traits::EntityKind, types::Id};
use icydb_core::db::{
    EntityResponse as CoreEntityResponse, ProjectedRow as CoreProjectedRow,
    ProjectionResponse as CoreProjectionResponse,
    ResponseCardinalityExt as CoreResponseCardinalityExt,
};

// re-exports
pub use paged::{PagedGroupedResponse, PagedResponse};
pub use write::*;

///
/// Response
///
/// Public facade over a materialized query result.
/// Wraps the core response and exposes only safe, policy-aware operations.
/// Any returned `Id<E>` values are public identifiers for correlation, reporting, and lookup only.
///

#[derive(Debug)]
pub struct Response<E: EntityKind> {
    inner: CoreEntityResponse<E>,
}

impl<E: EntityKind> Response<E> {
    /// Construct a facade response from a core response.
    #[must_use]
    pub const fn from_core(inner: CoreEntityResponse<E>) -> Self {
        Self { inner }
    }

    #[must_use]
    pub const fn count(&self) -> u32 {
        self.inner.count()
    }

    #[must_use]
    pub const fn exists(&self) -> bool {
        !self.inner.is_empty()
    }

    // ------------------------------------------------------------------
    // Cardinality
    // ------------------------------------------------------------------

    /// Require exactly one row.
    pub fn require_one(&self) -> Result<(), Error> {
        CoreResponseCardinalityExt::require_one(&self.inner).map_err(Into::into)
    }

    /// Require at least one row.
    pub fn require_some(&self) -> Result<(), Error> {
        CoreResponseCardinalityExt::require_some(&self.inner).map_err(Into::into)
    }

    // ------------------------------------------------------------------
    // Entities
    // ------------------------------------------------------------------

    /// Return the single entity.
    pub fn entity(self) -> Result<E, Error> {
        CoreResponseCardinalityExt::entity(self.inner).map_err(Into::into)
    }

    /// Return zero or one entity.
    pub fn try_entity(self) -> Result<Option<E>, Error> {
        CoreResponseCardinalityExt::try_entity(self.inner).map_err(Into::into)
    }

    /// Return all entities.
    #[must_use]
    pub fn entities(self) -> Vec<E> {
        self.inner.entities()
    }

    // Identity (facade-friendly naming)
    // ------------------------------------------------------------------

    /// Return the single identity.
    ///
    /// This key is a public identifier and does not grant access or authority.
    pub fn require_id(self) -> Result<Id<E>, Error> {
        CoreResponseCardinalityExt::require_id(self.inner).map_err(Into::into)
    }

    /// Return zero or one primary key.
    ///
    /// IDs are safe to transport and log; verification is always explicit and contextual.
    pub fn try_id(self) -> Result<Option<Id<E>>, Error> {
        CoreResponseCardinalityExt::try_row(self.inner)
            .map(|row| row.map(|entry| entry.id()))
            .map_err(Into::into)
    }

    /// Borrow an iterator over primary keys for correlation, reporting, and lookup.
    pub fn ids(&self) -> impl Iterator<Item = Id<E>> + '_ {
        self.inner.ids()
    }

    /// Check whether the response contains the given primary key.
    pub fn contains_id(&self, id: &Id<E>) -> bool {
        self.inner.contains_id(id)
    }
}

///
/// ProjectionResponse
///
/// Public facade over projection-shaped SQL query results.
/// Wraps the core projection response and exposes projection-row iteration.
///

#[derive(Debug)]
pub struct ProjectionResponse<E: EntityKind> {
    inner: CoreProjectionResponse<E>,
}

impl<E: EntityKind> ProjectionResponse<E> {
    /// Construct a facade projection response from a core projection response.
    #[must_use]
    pub const fn from_core(inner: CoreProjectionResponse<E>) -> Self {
        Self { inner }
    }

    /// Return the number of projected rows.
    #[must_use]
    pub const fn count(&self) -> u32 {
        self.inner.count()
    }

    /// Return whether at least one projected row exists.
    #[must_use]
    pub const fn exists(&self) -> bool {
        !self.inner.is_empty()
    }

    /// Consume and return projected rows in response order.
    #[must_use]
    pub fn rows(self) -> Vec<CoreProjectedRow<E>> {
        self.inner.rows()
    }

    /// Borrow an iterator over projected rows in response order.
    pub fn iter(&self) -> std::slice::Iter<'_, CoreProjectedRow<E>> {
        self.inner.iter()
    }
}

impl<'a, E: EntityKind> IntoIterator for &'a ProjectionResponse<E> {
    type Item = &'a CoreProjectedRow<E>;
    type IntoIter = std::slice::Iter<'a, CoreProjectedRow<E>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<E: EntityKind> IntoIterator for ProjectionResponse<E> {
    type Item = CoreProjectedRow<E>;
    type IntoIter = std::vec::IntoIter<CoreProjectedRow<E>>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows().into_iter()
    }
}
