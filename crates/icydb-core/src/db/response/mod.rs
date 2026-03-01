//! Module: response
//! Responsibility: materialized query/write response payload contracts.
//! Does not own: execution routing, planning policy, or cursor token protocol.
//! Boundary: Tier-2 db API DTO surface returned by session execution.
mod paged;

use crate::{
    prelude::*,
    traits::{AsView, EntityValue},
    types::Id,
};
use thiserror::Error as ThisError;

// re-exports
pub use paged::{PagedLoadExecution, PagedLoadExecutionWithTrace};

///
/// Row
///

pub type Row<E> = (Id<E>, E);

///
/// ResponseError
///

#[derive(Debug, ThisError)]
pub enum ResponseError {
    #[error("expected exactly one row, found 0 (entity {entity})")]
    NotFound { entity: &'static str },

    #[error("expected exactly one row, found {count} (entity {entity})")]
    NotUnique { entity: &'static str, count: u32 },
}

impl ResponseError {
    const fn not_found<E: EntityKind>() -> Self {
        Self::NotFound { entity: E::PATH }
    }

    const fn not_unique<E: EntityKind>(count: u32) -> Self {
        Self::NotUnique {
            entity: E::PATH,
            count,
        }
    }
}

///
/// Response
///
/// Materialized query result: ordered `(Id, Entity)` pairs.
/// IDs are returned for correlation, reporting, and lookup; they are public values and do not imply
/// authorization, ownership, or row existence outside this response payload.
///

#[derive(Debug)]
pub struct Response<E: EntityKind>(pub Vec<Row<E>>);

impl<E: EntityKind> Response<E> {
    // ------------------------------------------------------------------
    // Introspection
    // ------------------------------------------------------------------

    /// Return the number of rows as a u32 API contract count.
    #[must_use]
    #[expect(clippy::cast_possible_truncation)]
    pub const fn count(&self) -> u32 {
        self.0.len() as u32
    }

    /// Return whether this response has no rows.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    // ------------------------------------------------------------------
    // Cardinality enforcement
    // ------------------------------------------------------------------

    /// Require exactly one row in this response.
    pub const fn require_one(&self) -> Result<(), ResponseError> {
        match self.count() {
            1 => Ok(()),
            0 => Err(ResponseError::not_found::<E>()),
            n => Err(ResponseError::not_unique::<E>(n)),
        }
    }

    /// Require at least one row in this response.
    pub const fn require_some(&self) -> Result<(), ResponseError> {
        if self.is_empty() {
            Err(ResponseError::not_found::<E>())
        } else {
            Ok(())
        }
    }

    // ------------------------------------------------------------------
    // Rows
    // ------------------------------------------------------------------

    /// Consume and return `None` for empty, `Some(row)` for one row, or error for many rows.
    #[expect(clippy::cast_possible_truncation)]
    pub fn try_row(self) -> Result<Option<Row<E>>, ResponseError> {
        match self.0.len() {
            0 => Ok(None),
            1 => Ok(Some(self.0.into_iter().next().unwrap())),
            n => Err(ResponseError::not_unique::<E>(n as u32)),
        }
    }

    /// Consume and return the single row, or fail on zero/many rows.
    pub fn row(self) -> Result<Row<E>, ResponseError> {
        self.try_row()?.ok_or_else(ResponseError::not_found::<E>)
    }

    /// Consume and return all rows in response order.
    #[must_use]
    pub fn rows(self) -> Vec<Row<E>> {
        self.0
    }

    // ------------------------------------------------------------------
    // Entities
    // ------------------------------------------------------------------

    /// Consume and return the single entity or `None`, failing on many rows.
    pub fn try_entity(self) -> Result<Option<E>, ResponseError> {
        Ok(self.try_row()?.map(|(_, e)| e))
    }

    /// Consume and return the single entity, failing on zero/many rows.
    pub fn entity(self) -> Result<E, ResponseError> {
        self.row().map(|(_, e)| e)
    }

    /// Consume and return all entities in response order.
    #[must_use]
    pub fn entities(self) -> Vec<E> {
        self.0.into_iter().map(|(_, e)| e).collect()
    }

    // ------------------------------------------------------------------
    // Ids (identity-level)
    // ------------------------------------------------------------------

    /// Return the first row identifier, if present.
    ///
    /// This identifier is a public value for correlation, reporting, and lookup only.
    #[must_use]
    pub fn id(&self) -> Option<Id<E>> {
        self.0.first().map(|(id, _)| *id)
    }

    /// Require exactly one row and return its identifier.
    pub fn require_id(self) -> Result<Id<E>, ResponseError> {
        self.row().map(|(id, _)| id)
    }

    /// Return all row identifiers in response order for correlation/reporting/lookup.
    #[must_use]
    pub fn ids(&self) -> Vec<Id<E>> {
        self.0.iter().map(|(id, _)| *id).collect()
    }

    /// Check whether the response contains the provided identifier.
    pub fn contains_id(&self, id: &Id<E>) -> bool {
        self.0.iter().any(|(k, _)| k == id)
    }

    // ------------------------------------------------------------------
    // Views
    // ------------------------------------------------------------------

    /// Return the single-row view, failing on zero/many rows.
    pub fn view(&self) -> Result<<E as AsView>::ViewType, ResponseError> {
        self.require_one()?;
        Ok(self.0[0].1.as_view())
    }

    /// Return an optional single-row view, failing on many rows.
    pub fn view_opt(&self) -> Result<Option<<E as AsView>::ViewType>, ResponseError> {
        match self.count() {
            0 => Ok(None),
            1 => Ok(Some(self.0[0].1.as_view())),
            n => Err(ResponseError::not_unique::<E>(n)),
        }
    }

    /// Return all row views in response order.
    #[must_use]
    pub fn views(&self) -> Vec<<E as AsView>::ViewType> {
        self.0.iter().map(|(_, e)| e.as_view()).collect()
    }
}

impl<E: EntityKind> IntoIterator for Response<E> {
    type Item = Row<E>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

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

    /// Return the stored entity's identity
    #[must_use]
    pub fn id(&self) -> Id<E>
    where
        E: EntityValue,
    {
        self.entity.id()
    }

    /// Return the stored entity as its view type.
    #[must_use]
    pub fn view(&self) -> <E as AsView>::ViewType
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

impl<E> IntoIterator for WriteBatchResponse<E> {
    type Item = WriteResponse<E>;
    type IntoIter = std::vec::IntoIter<WriteResponse<E>>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl<E> WriteBatchResponse<E> {
    /// Borrow an iterator over write entries in stable batch order.
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
