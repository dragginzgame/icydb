mod write;

use crate::{
    error::{Error, ErrorClass, ErrorOrigin},
    traits::EntityKind,
    types::Id,
    view::View,
};
use icydb_core::db::response::{Response as CoreResponse, ResponseError};

// re-exports
pub use write::*;

///
/// Response
///
/// Public facade over a materialized query result.
/// Wraps the core response and exposes only safe, policy-aware operations.
///

#[derive(Debug)]
pub struct Response<E: EntityKind> {
    inner: CoreResponse<E>,
}

impl<E: EntityKind> Response<E> {
    /// Construct a facade response from a core response.
    #[must_use]
    pub const fn from_core(inner: CoreResponse<E>) -> Self {
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
        self.inner.require_one().map_err(map_response_error)
    }

    /// Require at least one row.
    pub fn require_some(&self) -> Result<(), Error> {
        self.inner.require_some().map_err(map_response_error)
    }

    // ------------------------------------------------------------------
    // Entities
    // ------------------------------------------------------------------

    /// Return the single entity.
    pub fn entity(self) -> Result<E, Error> {
        self.inner.entity().map_err(map_response_error)
    }

    /// Return zero or one entity.
    pub fn try_entity(self) -> Result<Option<E>, Error> {
        self.inner.try_entity().map_err(map_response_error)
    }

    /// Return all entities.
    #[must_use]
    pub fn entities(self) -> Vec<E> {
        self.inner.entities()
    }

    // ------------------------------------------------------------------
    // Views
    // ------------------------------------------------------------------

    /// Return the single view.
    pub fn view(&self) -> Result<View<E>, Error> {
        self.inner.view().map_err(map_response_error)
    }

    /// Return zero or one view.
    pub fn view_opt(&self) -> Result<Option<View<E>>, Error> {
        self.inner.view_opt().map_err(map_response_error)
    }

    /// Return all views.
    #[must_use]
    pub fn views(&self) -> Vec<View<E>> {
        self.inner.views()
    }

    // ------------------------------------------------------------------
    // Identity (facade-friendly naming)
    // ------------------------------------------------------------------

    /// Return the single key.
    pub fn key(self) -> Result<Id<E>, Error> {
        self.inner.require_id().map_err(map_response_error)
    }

    /// Return zero or one primary key.
    pub fn try_key(self) -> Result<Option<Id<E>>, Error> {
        self.inner
            .try_row()
            .map(|row| row.map(|(id, _)| id))
            .map_err(map_response_error)
    }

    /// Return all primary keys.
    #[must_use]
    pub fn keys(&self) -> Vec<Id<E>> {
        self.inner.ids()
    }

    /// Check whether the response contains the given primary key.
    pub fn contains_key(&self, id: &Id<E>) -> bool {
        self.inner.contains_id(id)
    }
}

// ----------------------------------------------------------------------
// Error mapping
// ----------------------------------------------------------------------

/// Map core response cardinality errors to public errors.
pub(crate) fn map_response_error(err: ResponseError) -> Error {
    match err {
        ResponseError::NotFound { .. } => {
            Error::new(ErrorClass::NotFound, ErrorOrigin::Response, err.to_string())
        }
        ResponseError::NotUnique { .. } => {
            Error::new(ErrorClass::Conflict, ErrorOrigin::Response, err.to_string())
        }
    }
}
