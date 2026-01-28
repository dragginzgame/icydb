use crate::{
    error::{ErrorClass, ErrorOrigin, InternalError},
    prelude::*,
    view::View,
};
use thiserror::Error as ThisError;

///
/// Row
///

pub type Row<E> = (Key, E);

///
/// ResponseError
/// Errors related to interpreting a materialized response.
///

#[derive(Debug, ThisError)]
pub enum ResponseError {
    #[error("expected exactly one row, found 0 (entity {entity})")]
    NotFound { entity: &'static str },

    #[error("expected exactly one row, found {count} (entity {entity})")]
    NotUnique { entity: &'static str, count: u32 },
}

impl ResponseError {
    pub(crate) const fn class(&self) -> ErrorClass {
        match self {
            Self::NotFound { .. } => ErrorClass::NotFound,
            Self::NotUnique { .. } => ErrorClass::Conflict,
        }
    }
}

impl From<ResponseError> for InternalError {
    fn from(err: ResponseError) -> Self {
        Self::new(err.class(), ErrorOrigin::Response, err.to_string())
    }
}

///
/// Response
/// Materialized query result: ordered `(Key, Entity)` pairs.
///
/// Fully materialized executor output. Pagination, ordering, and
/// filtering are expressed at the intent layer.
///
#[derive(Debug)]
pub struct Response<E: EntityKind>(pub Vec<Row<E>>);

impl<E: EntityKind> Response<E> {
    // ------------------------------------------------------------------
    // Introspection
    // ------------------------------------------------------------------

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
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

    pub fn require_one(&self) -> Result<(), InternalError> {
        match self.count() {
            1 => Ok(()),
            0 => Err(ResponseError::NotFound { entity: E::PATH }.into()),
            n => Err(ResponseError::NotUnique {
                entity: E::PATH,
                count: n,
            }
            .into()),
        }
    }

    pub fn require_some(&self) -> Result<(), InternalError> {
        if self.is_empty() {
            Err(ResponseError::NotFound { entity: E::PATH }.into())
        } else {
            Ok(())
        }
    }

    // ------------------------------------------------------------------
    // Rows
    // ------------------------------------------------------------------

    pub fn row(self) -> Result<Row<E>, InternalError> {
        self.require_one()?;
        Ok(self.0.into_iter().next().unwrap())
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn try_row(self) -> Result<Option<Row<E>>, InternalError> {
        match self.0.len() {
            0 => Ok(None),
            1 => Ok(Some(self.0.into_iter().next().unwrap())),
            n => Err(ResponseError::NotUnique {
                entity: E::PATH,
                count: n as u32,
            }
            .into()),
        }
    }

    #[must_use]
    pub fn rows(self) -> Vec<Row<E>> {
        self.0
    }

    // ------------------------------------------------------------------
    // Entities (primary ergonomic surface)
    // ------------------------------------------------------------------

    pub fn entity(self) -> Result<E, InternalError> {
        self.row().map(|(_, e)| e)
    }

    pub fn try_entity(self) -> Result<Option<E>, InternalError> {
        Ok(self.try_row()?.map(|(_, e)| e))
    }

    #[must_use]
    pub fn entities(self) -> Vec<E> {
        self.0.into_iter().map(|(_, e)| e).collect()
    }

    // ------------------------------------------------------------------
    // Keys
    // ------------------------------------------------------------------

    #[must_use]
    pub fn key(&self) -> Option<Key> {
        self.0.first().map(|(k, _)| *k)
    }

    pub fn key_strict(self) -> Result<Key, InternalError> {
        self.row().map(|(k, _)| k)
    }

    pub fn try_key(self) -> Result<Option<Key>, InternalError> {
        Ok(self.try_row()?.map(|(k, _)| k))
    }

    #[must_use]
    pub fn keys(&self) -> Vec<Key> {
        self.0.iter().map(|(k, _)| *k).collect()
    }

    #[must_use]
    pub fn contains_key(&self, key: &Key) -> bool {
        self.0.iter().any(|(k, _)| k == key)
    }

    // ------------------------------------------------------------------
    // Views
    // ------------------------------------------------------------------

    /// Require exactly one result and return it as a view.
    pub fn view(&self) -> Result<View<E>, InternalError> {
        self.require_one()?;
        let view = self
            .0
            .first()
            .map(|(_, entity)| entity.to_view())
            .expect("require_one ensures one row");

        Ok(view)
    }

    /// Return zero or one result as a view.
    #[allow(clippy::cast_possible_truncation)]
    pub fn view_opt(&self) -> Result<Option<View<E>>, InternalError> {
        match self.0.len() {
            0 => Ok(None),
            1 => Ok(self.0.first().map(|(_, entity)| entity.to_view())),
            n => Err(ResponseError::NotUnique {
                entity: E::PATH,
                count: n as u32,
            }
            .into()),
        }
    }

    /// Return zero or one result as a view.
    pub fn try_view(&self) -> Result<Option<View<E>>, InternalError> {
        self.view_opt()
    }

    /// Return all results as views.
    #[must_use]
    pub fn views(&self) -> Vec<View<E>> {
        self.0.iter().map(|(_, entity)| entity.to_view()).collect()
    }

    // ------------------------------------------------------------------
    // Non-strict access (explicitly unsafe)
    // ------------------------------------------------------------------

    #[must_use]
    pub fn first(self) -> Option<Row<E>> {
        self.0.into_iter().next()
    }

    #[must_use]
    pub fn first_entity(self) -> Option<E> {
        self.first().map(|(_, e)| e)
    }

    #[must_use]
    pub fn first_pk(self) -> Option<E::PrimaryKey> {
        self.first_entity().map(|e| e.primary_key())
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
/// ResponseExt
/// Ergonomic helpers for `Result<Response<E>, InternalError>`.
///
/// This trait exists solely to avoid repetitive `?` when
/// working with executor results. It intentionally exposes
/// a *minimal* surface.
///
pub trait ResponseExt<E: EntityKind> {
    // --- entities (primary use case) ---

    fn entities(self) -> Result<Vec<E>, InternalError>;
    fn entity(self) -> Result<E, InternalError>;
    fn try_entity(self) -> Result<Option<E>, InternalError>;

    // --- introspection ---

    fn count(self) -> Result<u32, InternalError>;
}

impl<E: EntityKind> ResponseExt<E> for Result<Response<E>, InternalError> {
    fn entities(self) -> Result<Vec<E>, InternalError> {
        Ok(self?.entities())
    }

    fn entity(self) -> Result<E, InternalError> {
        self?.entity()
    }

    fn try_entity(self) -> Result<Option<E>, InternalError> {
        self?.try_entity()
    }

    fn count(self) -> Result<u32, InternalError> {
        Ok(self?.count())
    }
}
