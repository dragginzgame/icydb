mod ext;

pub use ext::*;

use crate::{
    error::{ErrorClass, ErrorOrigin, InternalError},
    prelude::*,
};
use thiserror::Error as ThisError;

///
/// Page
///

pub struct Page<T> {
    pub items: Vec<T>,
    pub has_more: bool,
}

impl<T> Page<T> {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.items.len()
    }
}

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
            Self::NotUnique { .. } => ErrorClass::Unsupported,
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

#[derive(Debug)]
pub struct Response<E: EntityKind>(pub Vec<Row<E>>);

impl<E: EntityKind> Response<E> {
    // ======================================================================
    // Cardinality (introspection only)
    // ======================================================================

    /// Number of rows in the response, truncated to `u32`.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub const fn count(&self) -> u32 {
        self.0.len() as u32
    }

    /// True when no rows were returned.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    // ======================================================================
    // Cardinality guards (non-consuming)
    // ======================================================================

    /// Require exactly one row.
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

    /// Require at least one row.
    pub fn require_some(&self) -> Result<(), InternalError> {
        match self.count() {
            0 => Err(ResponseError::NotFound { entity: E::PATH }.into()),
            _ => Ok(()),
        }
    }

    /// Require exactly `expected` rows.
    pub fn require_len(&self, expected: u32) -> Result<(), InternalError> {
        let actual = self.count();
        if actual == expected {
            Ok(())
        } else if actual == 0 {
            Err(ResponseError::NotFound { entity: E::PATH }.into())
        } else {
            Err(ResponseError::NotUnique {
                entity: E::PATH,
                count: actual,
            }
            .into())
        }
    }

    // ======================================================================
    // Row extractors (consume self)
    // ======================================================================

    /// Require exactly one row and return it.
    pub fn one(self) -> Result<Row<E>, InternalError> {
        self.require_one()?;
        Ok(self.0.into_iter().next().unwrap())
    }

    /// Require at most one row and return it.
    #[allow(clippy::cast_possible_truncation)]
    pub fn one_opt(self) -> Result<Option<Row<E>>, InternalError> {
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

    /// Convert the response into a page of entities with a `has_more` indicator.
    ///
    /// This consumes at most `limit + 1` rows to determine whether more results
    /// exist. Ordering is preserved.
    ///
    /// NOTE:
    /// - `has_more` only indicates the existence of additional rows
    /// - Page boundaries are not stable unless the underlying query ordering is stable
    #[must_use]
    pub fn into_page(self, limit: usize) -> Page<E> {
        let mut iter = self.0.into_iter();

        let mut items = Vec::with_capacity(limit);
        for _ in 0..limit {
            if let Some((_, entity)) = iter.next() {
                items.push(entity);
            } else {
                return Page {
                    items,
                    has_more: false,
                };
            }
        }

        Page {
            items,
            has_more: iter.next().is_some(),
        }
    }

    // ======================================================================
    // Key extractors
    // ======================================================================

    /// First key in the response, if present.
    #[must_use]
    pub fn key(&self) -> Option<Key> {
        self.0.first().map(|(k, _)| *k)
    }

    /// Collect all keys in order.
    #[must_use]
    pub fn keys(&self) -> Vec<Key> {
        self.0.iter().map(|(k, _)| *k).collect()
    }

    /// Require exactly one row and return its key.
    pub fn one_key(self) -> Result<Key, InternalError> {
        self.one().map(|(k, _)| k)
    }

    /// Require at most one row and return its key.
    pub fn one_opt_key(self) -> Result<Option<Key>, InternalError> {
        Ok(self.one_opt()?.map(|(k, _)| k))
    }

    #[must_use]
    pub fn contains_key(&self, key: &Key) -> bool {
        self.0.iter().any(|(k, _)| k == key)
    }

    // ======================================================================
    // Entity extractors
    // ======================================================================

    /// Consume the response and return the first entity, if any.
    #[must_use]
    pub fn entity(self) -> Option<E> {
        self.0.into_iter().next().map(|(_, e)| e)
    }

    /// Consume the response and collect all entities.
    #[must_use]
    pub fn entities(self) -> Vec<E> {
        self.0.into_iter().map(|(_, e)| e).collect()
    }

    /// Require exactly one entity.
    pub fn one_entity(self) -> Result<E, InternalError> {
        self.one().map(|(_, e)| e)
    }

    /// Require at most one entity.
    pub fn one_opt_entity(self) -> Result<Option<E>, InternalError> {
        Ok(self.one_opt()?.map(|(_, e)| e))
    }

    // ======================================================================
    // Primary key extractors
    // ======================================================================

    /// First primary key in the response, if present.
    #[must_use]
    pub fn pk(&self) -> Option<E::PrimaryKey> {
        self.0.first().map(|(_, e)| e.primary_key())
    }

    /// Collect all primary keys in order.
    #[must_use]
    pub fn pks(&self) -> Vec<E::PrimaryKey> {
        self.0.iter().map(|(_, e)| e.primary_key()).collect()
    }

    /// Require exactly one primary key.
    pub fn one_pk(self) -> Result<E::PrimaryKey, InternalError> {
        self.one_entity().map(|e| e.primary_key())
    }

    /// Require at most one primary key.
    pub fn one_opt_pk(self) -> Result<Option<E::PrimaryKey>, InternalError> {
        Ok(self.one_opt_entity()?.map(|e| e.primary_key()))
    }

    // ======================================================================
    // View extractors
    // ======================================================================

    /// Convert the first entity to its view type, if present.
    #[must_use]
    pub fn view(self) -> Option<E::ViewType> {
        self.entity().map(|e| e.to_view())
    }

    /// Require exactly one view.
    pub fn one_view(self) -> Result<E::ViewType, InternalError> {
        self.one_entity().map(|e| e.to_view())
    }

    /// Require at most one view.
    pub fn one_opt_view(self) -> Result<Option<E::ViewType>, InternalError> {
        Ok(self.one_opt_entity()?.map(|e| e.to_view()))
    }

    /// Convert all entities to their view types.
    #[must_use]
    pub fn views(self) -> Vec<E::ViewType> {
        self.entities().into_iter().map(|e| e.to_view()).collect()
    }

    // ======================================================================
    // Arbitrary row access (no cardinality guarantees)
    // ======================================================================

    /// Return the first row in the response, if any.
    ///
    /// This does NOT enforce cardinality. Use only when row order is
    /// well-defined and uniqueness is irrelevant.
    #[must_use]
    pub fn first(self) -> Option<Row<E>> {
        self.0.into_iter().next()
    }

    /// Return the first entity in the response, if any.
    ///
    /// This does NOT enforce cardinality. Use only when row order is
    /// well-defined and uniqueness is irrelevant.
    #[must_use]
    pub fn first_entity(self) -> Option<E> {
        self.first().map(|(_, e)| e)
    }

    /// Return the first primary key in the response, if any.
    ///
    /// This does NOT enforce cardinality.
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
