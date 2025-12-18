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
    //
    // Cardinality
    //

    #[must_use]
    /// Number of rows in the response, truncated to `u32`.
    #[allow(clippy::cast_possible_truncation)]
    pub const fn count(&self) -> u32 {
        self.0.len() as u32
    }

    #[must_use]
    /// True when no rows were returned.
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    //
    // Exact cardinality helpers
    //

    /// Require exactly one row.
    pub fn one(self) -> Result<(Key, E), Error> {
        let count = self.count();

        match count {
            0 => Err(ResponseError::NotFound { entity: E::PATH }.into()),
            1 => Ok(self.0.into_iter().next().unwrap()),
            _ => Err(ResponseError::NotUnique {
                entity: E::PATH,
                count,
            }
            .into()),
        }
    }

    /// Require exactly one entity.
    pub fn one_entity(self) -> Result<E, Error> {
        self.one().map(|(_, e)| e)
    }

    /// Require at most one row.
    pub fn one_opt(self) -> Result<Option<(Key, E)>, Error> {
        let count = self.count();

        match count {
            0 => Ok(None),
            1 => Ok(Some(self.0.into_iter().next().unwrap())),
            _ => Err(ResponseError::NotUnique {
                entity: E::PATH,
                count,
            }
            .into()),
        }
    }

    /// Require at most one entity.
    pub fn one_opt_entity(self) -> Result<Option<E>, Error> {
        Ok(self.one_opt()?.map(|(_, e)| e))
    }

    //
    // Keys
    //

    #[must_use]
    /// First key in the response, if present.
    pub fn key(&self) -> Option<Key> {
        self.0.first().map(|(key, _)| *key)
    }

    #[must_use]
    /// Collect all keys in order.
    pub fn keys(&self) -> Vec<Key> {
        self.0.iter().map(|(key, _)| *key).collect()
    }

    /// Iterate keys without cloning entities.
    pub fn keys_iter(self) -> impl Iterator<Item = Key> {
        self.0.into_iter().map(|(key, _)| key)
    }

    /// Require exactly one row and return its key.
    pub fn one_key(self) -> Result<Key, Error> {
        self.one().map(|(key, _)| key)
    }

    /// Require at most one row and return its key.
    pub fn one_opt_key(self) -> Result<Option<Key>, Error> {
        Ok(self.one_opt()?.map(|(key, _)| key))
    }

    #[must_use]
    pub fn contains_key(&self, key: &Key) -> bool {
        self.0.iter().any(|(k, _)| k == key)
    }

    //
    // Primary keys
    //

    #[must_use]
    /// First primary key in the response, if present.
    pub fn pk(&self) -> Option<E::PrimaryKey> {
        self.0.first().map(|(_, e)| e.primary_key())
    }

    #[must_use]
    /// Collect all primary keys in order.
    pub fn pks(&self) -> Vec<E::PrimaryKey> {
        self.0.iter().map(|(_, e)| e.primary_key()).collect()
    }

    /// Iterate primary keys without cloning entities.
    pub fn pks_iter(self) -> impl Iterator<Item = E::PrimaryKey> {
        self.0.into_iter().map(|(_, e)| e.primary_key())
    }

    pub fn one_pk(self) -> Result<E::PrimaryKey, Error> {
        self.one_entity().map(|e| e.primary_key())
    }

    pub fn one_opt_pk(self) -> Result<Option<E::PrimaryKey>, Error> {
        Ok(self.one_opt_entity()?.map(|e| e.primary_key()))
    }

    //
    // Entities
    //

    #[must_use]
    /// Consume the response and return the first entity, if any.
    pub fn entity(self) -> Option<E> {
        self.0.into_iter().next().map(|(_, e)| e)
    }

    #[must_use]
    /// Consume the response and collect all entities.
    pub fn entities(self) -> Vec<E> {
        self.0.into_iter().map(|(_, e)| e).collect()
    }

    /// Iterate entities without materializing a `Vec`.
    pub fn entities_iter(self) -> impl Iterator<Item = E> {
        self.0.into_iter().map(|(_, e)| e)
    }

    //
    // Views
    //

    #[must_use]
    /// Convert the first entity to its view type, if present.
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

    #[must_use]
    /// Convert all entities to their view types and collect them.
    pub fn views(self) -> Vec<E::ViewType> {
        self.entities().into_iter().map(|e| e.to_view()).collect()
    }

    /// Iterate over view types without cloning entities.
    pub fn views_iter(self) -> impl Iterator<Item = E::ViewType> {
        self.entities().into_iter().map(|e| e.to_view())
    }
}

impl<E: EntityKind> IntoIterator for Response<E> {
    type Item = (Key, E);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
