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
    pub const fn count(&self) -> u32 {
        self.0.len() as u32
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    ///
    /// Key
    ///

    #[must_use]
    pub fn key(&self) -> Option<Key> {
        self.0.first().map(|(key, _)| *key)
    }

    pub fn try_key(&self) -> Result<Key, Error> {
        let key = self
            .key()
            .ok_or_else(|| ResponseError::NoRowsFound(E::PATH.to_string()))?;

        Ok(key)
    }

    #[must_use]
    pub fn keys(&self) -> Vec<Key> {
        self.0.iter().map(|(key, _)| *key).collect()
    }

    pub fn keys_iter(self) -> impl Iterator<Item = Key> {
        self.0.into_iter().map(|(key, _)| key)
    }

    ///
    /// Pk
    ///

    #[must_use]
    pub fn pk(&self) -> Option<E::PrimaryKey> {
        self.0.first().map(|(_, e)| e.primary_key())
    }

    pub fn try_pk(&self) -> Result<E::PrimaryKey, Error> {
        let pk = self
            .pk()
            .ok_or_else(|| ResponseError::NoRowsFound(E::PATH.to_string()))?;

        Ok(pk)
    }

    #[must_use]
    pub fn pks(&self) -> Vec<E::PrimaryKey> {
        self.0.iter().map(|(_, e)| e.primary_key()).collect()
    }

    pub fn pks_iter(self) -> impl Iterator<Item = E::PrimaryKey> {
        self.0.into_iter().map(|(_, e)| e.primary_key())
    }

    ///
    /// Entity
    ///

    #[must_use]
    pub fn entity(self) -> Option<E> {
        self.0.into_iter().next().map(|(_, e)| e)
    }

    pub fn try_entity(self) -> Result<E, Error> {
        let res = self
            .entity()
            .ok_or_else(|| ResponseError::NoRowsFound(E::PATH.to_string()))?;

        Ok(res)
    }

    #[must_use]
    pub fn entities(self) -> Vec<E> {
        self.0.into_iter().map(|(_, e)| e).collect()
    }

    pub fn entities_iter(self) -> impl Iterator<Item = E> {
        self.0.into_iter().map(|(_, e)| e)
    }

    ///
    /// View
    ///

    #[must_use]
    pub fn view(self) -> Option<E::ViewType> {
        self.entity().map(|e| e.to_view())
    }

    pub fn try_view(self) -> Result<E::ViewType, Error> {
        self.try_entity().map(|e| e.to_view())
    }

    #[must_use]
    pub fn views(self) -> Vec<E::ViewType> {
        self.entities().into_iter().map(|e| e.to_view()).collect()
    }

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
