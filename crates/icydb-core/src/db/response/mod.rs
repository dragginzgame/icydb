mod write;

use crate::{prelude::*, traits::AsView, types::Id};
use thiserror::Error as ThisError;

// re-exports
pub use write::*;

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

    #[must_use]
    #[expect(clippy::cast_possible_truncation)]
    pub const fn count(&self) -> u32 {
        self.0.len() as u32
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    // ------------------------------------------------------------------
    // Cardinality enforcement
    // ------------------------------------------------------------------

    pub const fn require_one(&self) -> Result<(), ResponseError> {
        match self.count() {
            1 => Ok(()),
            0 => Err(ResponseError::not_found::<E>()),
            n => Err(ResponseError::not_unique::<E>(n)),
        }
    }

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

    #[expect(clippy::cast_possible_truncation)]
    pub fn try_row(self) -> Result<Option<Row<E>>, ResponseError> {
        match self.0.len() {
            0 => Ok(None),
            1 => Ok(Some(self.0.into_iter().next().unwrap())),
            n => Err(ResponseError::not_unique::<E>(n as u32)),
        }
    }

    pub fn row(self) -> Result<Row<E>, ResponseError> {
        self.try_row()?.ok_or_else(ResponseError::not_found::<E>)
    }

    #[must_use]
    pub fn rows(self) -> Vec<Row<E>> {
        self.0
    }

    // ------------------------------------------------------------------
    // Entities
    // ------------------------------------------------------------------

    pub fn try_entity(self) -> Result<Option<E>, ResponseError> {
        Ok(self.try_row()?.map(|(_, e)| e))
    }

    pub fn entity(self) -> Result<E, ResponseError> {
        self.row().map(|(_, e)| e)
    }

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

    pub fn view(&self) -> Result<<E as AsView>::ViewType, ResponseError> {
        self.require_one()?;
        Ok(self.0[0].1.as_view())
    }

    pub fn view_opt(&self) -> Result<Option<<E as AsView>::ViewType>, ResponseError> {
        match self.count() {
            0 => Ok(None),
            1 => Ok(Some(self.0[0].1.as_view())),
            n => Err(ResponseError::not_unique::<E>(n)),
        }
    }

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
