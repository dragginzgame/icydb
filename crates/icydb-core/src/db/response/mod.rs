//! Module: response
//! Responsibility: materialized query/write response payload contracts.
//! Does not own: execution routing, planning policy, or cursor token protocol.
//! Boundary: Tier-2 db API DTO surface returned by session execution.
//! Architecture: `Response<R>` is transport-only and row-shape-agnostic.
//! Query semantics (for example cardinality checks) must live in query/session
//! extension traits rather than inherent response DTO methods.

mod grouped;
mod paged;

use crate::{prelude::*, traits::EntityValue, types::Id, value::Value};
use thiserror::Error as ThisError;

mod private {
    ///
    /// Sealed
    ///
    /// Internal marker used to seal response row-shape marker implementations.
    ///

    pub trait Sealed {}
}

pub(in crate::db) use grouped::GroupedTextCursorPageWithTrace;
pub use grouped::{GroupedRow, PagedGroupedExecution, PagedGroupedExecutionWithTrace};
pub use paged::{PagedLoadExecution, PagedLoadExecutionWithTrace};

///
/// ResponseRow
///
/// Marker trait for row-shape DTOs that are valid payloads for `Response<R>`.
/// This trait is sealed to keep row-shape admission local to the response layer.
///

pub trait ResponseRow: private::Sealed {}

impl ResponseRow for GroupedRow {}

impl private::Sealed for GroupedRow {}

///
/// Row
///
/// Materialized entity row with explicit identity and payload accessors.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Row<E: EntityKind> {
    id: Id<E>,
    entity: E,
}

impl<E: EntityKind> Row<E> {
    /// Construct one row from identity and entity payload.
    #[must_use]
    pub const fn new(id: Id<E>, entity: E) -> Self {
        Self { id, entity }
    }

    /// Borrow this row's identity.
    #[must_use]
    pub const fn id(&self) -> Id<E> {
        self.id
    }

    /// Consume and return this row's entity payload.
    #[must_use]
    pub fn entity(self) -> E {
        self.entity
    }

    /// Borrow this row's entity payload.
    #[must_use]
    pub const fn entity_ref(&self) -> &E {
        &self.entity
    }

    /// Consume and return `(id, entity)` parts.
    #[must_use]
    pub fn into_parts(self) -> (Id<E>, E) {
        (self.id, self.entity)
    }
}

impl<E: EntityKind> From<(Id<E>, E)> for Row<E> {
    fn from(value: (Id<E>, E)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl<E: EntityKind> private::Sealed for Row<E> {}

impl<E: EntityKind> ResponseRow for Row<E> {}

///
/// ProjectedRow
///
/// One scalar projection output row emitted in planner declaration order.
/// `values` carries evaluated expression outputs for this row.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedRow<E: EntityKind> {
    id: Id<E>,
    values: Vec<Value>,
}

impl<E: EntityKind> ProjectedRow<E> {
    /// Construct one projected scalar row.
    #[must_use]
    pub const fn new(id: Id<E>, values: Vec<Value>) -> Self {
        Self { id, values }
    }

    /// Borrow the source row identifier.
    #[must_use]
    pub const fn id(&self) -> Id<E> {
        self.id
    }

    /// Borrow projected scalar values in declaration order.
    #[must_use]
    pub const fn values(&self) -> &[Value] {
        self.values.as_slice()
    }

    /// Consume and return `(id, projected_values)`.
    #[must_use]
    pub fn into_parts(self) -> (Id<E>, Vec<Value>) {
        (self.id, self.values)
    }
}

impl<E: EntityKind> private::Sealed for ProjectedRow<E> {}

impl<E: EntityKind> ResponseRow for ProjectedRow<E> {}

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
    /// Construct one response not-found cardinality error.
    #[must_use]
    pub const fn not_found(entity: &'static str) -> Self {
        Self::NotFound { entity }
    }

    /// Construct one response not-unique cardinality error.
    #[must_use]
    pub const fn not_unique(entity: &'static str, count: u32) -> Self {
        Self::NotUnique { entity, count }
    }
}

///
/// Response
///
/// Generic response transport container for one row shape `R`.
///

#[derive(Debug)]
pub struct Response<R: ResponseRow>(Vec<R>);

///
/// EntityResponse
///
/// Entity-row response transport alias.
///

pub type EntityResponse<E> = Response<Row<E>>;

///
/// ProjectionResponse
///
/// Scalar projection response transport alias.
///

pub type ProjectionResponse<E> = Response<ProjectedRow<E>>;

impl<R: ResponseRow> Response<R> {
    /// Construct one response from ordered rows.
    #[must_use]
    pub const fn new(rows: Vec<R>) -> Self {
        Self(rows)
    }

    /// Construct one response from rows convertible into `R`.
    #[must_use]
    pub fn from_rows<T>(rows: Vec<T>) -> Self
    where
        T: Into<R>,
    {
        Self(rows.into_iter().map(Into::into).collect())
    }

    /// Return the number of rows.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

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

    /// Consume and return all rows in response order.
    #[must_use]
    pub fn rows(self) -> Vec<R> {
        self.0
    }

    /// Borrow an iterator over rows in response order.
    pub fn iter(&self) -> std::slice::Iter<'_, R> {
        self.0.iter()
    }
}

impl<R: ResponseRow> AsRef<[R]> for Response<R> {
    fn as_ref(&self) -> &[R] {
        self.0.as_slice()
    }
}

impl<R: ResponseRow> std::ops::Deref for Response<R> {
    type Target = [R];

    fn deref(&self) -> &Self::Target {
        self.0.as_slice()
    }
}

impl<E: EntityKind> Response<Row<E>> {
    /// Return the first row identifier, if present.
    #[must_use]
    pub fn id(&self) -> Option<Id<E>> {
        self.0.first().map(Row::id)
    }

    /// Consume and return all entities in response order.
    #[must_use]
    pub fn entities(self) -> Vec<E> {
        self.0.into_iter().map(Row::entity).collect()
    }

    /// Borrow an iterator over row identifiers in response order.
    pub fn ids(&self) -> impl Iterator<Item = Id<E>> + '_ {
        self.0.iter().map(Row::id)
    }

    /// Check whether the response contains the provided identifier.
    pub fn contains_id(&self, id: &Id<E>) -> bool {
        self.0.iter().any(|row| row.id() == *id)
    }
}

impl<R: ResponseRow> IntoIterator for Response<R> {
    type Item = R;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, R: ResponseRow> IntoIterator for &'a Response<R> {
    type Item = &'a R;
    type IntoIter = std::slice::Iter<'a, R>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
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
    entities: Vec<E>,
}

impl<E> WriteBatchResponse<E> {
    /// Construct a batch response from stored entities.
    #[must_use]
    pub const fn new(entities: Vec<E>) -> Self {
        Self { entities }
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

    /// Borrow an iterator over primary keys in stable batch order.
    pub fn ids(&self) -> impl Iterator<Item = Id<E>> + '_
    where
        E: EntityValue,
    {
        self.entities.iter().map(EntityValue::id)
    }

    /// Borrow an iterator over write entries in stable batch order.
    pub fn iter(&self) -> std::slice::Iter<'_, E> {
        self.entities.iter()
    }
}

impl<E> IntoIterator for WriteBatchResponse<E> {
    type Item = E;
    type IntoIter = std::vec::IntoIter<E>;

    fn into_iter(self) -> Self::IntoIter {
        self.entities.into_iter()
    }
}

impl<'a, E> IntoIterator for &'a WriteBatchResponse<E> {
    type Item = &'a E;
    type IntoIter = std::slice::Iter<'a, E>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
