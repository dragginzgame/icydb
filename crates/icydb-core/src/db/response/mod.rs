use crate::{Error, Key, ThisError, db::DbError, traits::EntityKind};

///
/// ResponseError
///

#[derive(Debug, ThisError)]
pub enum ResponseError {
    #[error("expected one or more rows, found 0 (entity {0})")]
    NoRowsFound(String),
}

impl From<ResponseError> for Error {
    fn from(err: ResponseError) -> Self {
        DbError::from(err).into()
    }
}

///
/// Response
///

#[derive(Debug)]
pub struct Response<E: EntityKind>(pub Vec<(Key, E)>);

impl<E> Response<E>
where
    E: EntityKind,
{
    // count
    // not len, as it returns a u32 so could get confusing
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    /// Number of rows in the response, truncated to `u32`.
    pub const fn count(&self) -> u32 {
        self.0.len() as u32
    }

    #[must_use]
    /// True when no rows were returned.
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    ///
    /// Key
    ///

    #[must_use]
    /// First key in the response, if present.
    pub fn key(&self) -> Option<Key> {
        self.0.first().map(|(key, _)| *key)
    }

    /// Return the first key or an error if no rows were returned.
    pub fn try_key(&self) -> Result<Key, Error> {
        let key = self
            .key()
            .ok_or_else(|| ResponseError::NoRowsFound(E::PATH.to_string()))?;

        Ok(key)
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

    ///
    /// Pk
    ///

    #[must_use]
    /// First primary key in the response, if present.
    pub fn pk(&self) -> Option<E::PrimaryKey> {
        self.0.first().map(|(_, e)| e.primary_key())
    }

    /// Return the first primary key or an error if no rows were returned.
    pub fn try_pk(&self) -> Result<E::PrimaryKey, Error> {
        let pk = self
            .pk()
            .ok_or_else(|| ResponseError::NoRowsFound(E::PATH.to_string()))?;

        Ok(pk)
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

    ///
    /// Entity
    ///

    #[must_use]
    /// Consume the response and return the first entity.
    pub fn entity(self) -> Option<E> {
        self.0.into_iter().next().map(|(_, e)| e)
    }

    /// Return the first entity or an error if no rows were returned.
    pub fn try_entity(self) -> Result<E, Error> {
        let res = self
            .entity()
            .ok_or_else(|| ResponseError::NoRowsFound(E::PATH.to_string()))?;

        Ok(res)
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

    ///
    /// View
    ///

    #[must_use]
    /// Convert the first entity to its view type.
    pub fn view(self) -> Option<E::ViewType> {
        self.entity().map(|e| e.to_view())
    }

    /// Convert the first entity to its view type or error if empty.
    pub fn try_view(self) -> Result<E::ViewType, Error> {
        self.try_entity().map(|e| e.to_view())
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
