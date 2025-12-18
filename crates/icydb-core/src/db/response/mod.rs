mod ext;

pub use ext::*;

use crate::{Error, Key, ThisError, db::DbError, traits::EntityKind};

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

impl From<ResponseError> for Error {
    fn from(err: ResponseError) -> Self {
        DbError::from(err).into()
    }
}

///
/// Response
/// Materialized query result: ordered `(Key, Entity)` pairs.
///

#[derive(Debug)]
pub struct Response<E: EntityKind>(pub Vec<(Key, E)>);

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
    pub fn require_one(&self) -> Result<(), Error> {
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

    // ======================================================================
    // Row extractors (consume self)
    // ======================================================================

    /// Require exactly one row and return it.
    pub fn one(self) -> Result<(Key, E), Error> {
        self.require_one()?;
        Ok(self.0.into_iter().next().unwrap())
    }

    /// Require at most one row and return it.
    pub fn one_opt(self) -> Result<Option<(Key, E)>, Error> {
        match self.count() {
            0 => Ok(None),
            1 => Ok(Some(self.0.into_iter().next().unwrap())),
            n => Err(ResponseError::NotUnique {
                entity: E::PATH,
                count: n,
            }
            .into()),
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
    pub fn one_key(self) -> Result<Key, Error> {
        self.one().map(|(k, _)| k)
    }

    /// Require at most one row and return its key.
    pub fn one_opt_key(self) -> Result<Option<Key>, Error> {
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
    pub fn one_entity(self) -> Result<E, Error> {
        self.one().map(|(_, e)| e)
    }

    /// Require at most one entity.
    pub fn one_opt_entity(self) -> Result<Option<E>, Error> {
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
    pub fn one_pk(self) -> Result<E::PrimaryKey, Error> {
        self.one_entity().map(|e| e.primary_key())
    }

    /// Require at most one primary key.
    pub fn one_opt_pk(self) -> Result<Option<E::PrimaryKey>, Error> {
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
    pub fn one_view(self) -> Result<E::ViewType, Error> {
        self.one_entity().map(|e| e.to_view())
    }

    /// Require at most one view.
    pub fn one_opt_view(self) -> Result<Option<E::ViewType>, Error> {
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
    pub fn first(self) -> Option<(Key, E)> {
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
    type Item = (Key, E);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
