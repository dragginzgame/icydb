use crate::{Error, db::response::Response, traits::EntityKind};

///
/// ResponseExt
/// Ergonomic helpers for interpreting `Result<Response<E>, Error>`.
///
/// This composes query execution with explicit cardinality handling
/// without collapsing the semantic boundary.
///

pub trait ResponseExt<E: EntityKind> {
    /// Require exactly one row and return the entity.
    fn one_entity(self) -> Result<E, Error>;

    /// Require at most one row and return the entity.
    fn one_opt_entity(self) -> Result<Option<E>, Error>;

    /// Consume the response and return all entities.
    fn entities(self) -> Result<Vec<E>, Error>;

    /// Require exactly one row and return its primary key.
    fn one_pk(self) -> Result<E::PrimaryKey, Error>;

    /// Require at most one row and return its primary key.
    fn one_opt_pk(self) -> Result<Option<E::PrimaryKey>, Error>;
}

impl<E, Err> ResponseExt<E> for Result<Response<E>, Err>
where
    E: EntityKind,
    Err: Into<Error>,
{
    #[inline]
    fn one_entity(self) -> Result<E, Error> {
        self.map_err(Into::into)?.one_entity()
    }

    #[inline]
    fn one_opt_entity(self) -> Result<Option<E>, Error> {
        self.map_err(Into::into)?.one_opt_entity()
    }

    #[inline]
    fn entities(self) -> Result<Vec<E>, Error> {
        Ok(self.map_err(Into::into)?.entities())
    }

    #[inline]
    fn one_pk(self) -> Result<E::PrimaryKey, Error> {
        self.map_err(Into::into)?.one_pk()
    }

    #[inline]
    fn one_opt_pk(self) -> Result<Option<E::PrimaryKey>, Error> {
        self.map_err(Into::into)?.one_opt_pk()
    }
}
