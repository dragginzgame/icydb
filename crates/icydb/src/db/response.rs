use crate::{error::Error, traits::EntityKind};

pub use icydb_core::db::response::{Response, Row};

///
/// ResponseExt
/// Facade helpers for `Result<Response<E>, Error>`.
/// Keeps public APIs returning `icydb::Error`.
///

pub trait ResponseExt<E: EntityKind> {
    /// Extract all entities from a successful response.
    fn entities(self) -> Result<Vec<E>, Error>;

    /// Extract exactly one entity or return an error.
    fn entity(self) -> Result<E, Error>;

    /// Extract zero or one entity or return an error.
    fn try_entity(self) -> Result<Option<E>, Error>;

    /// Return the row count of a successful response.
    fn count(self) -> Result<u32, Error>;
}

impl<E: EntityKind> ResponseExt<E> for Result<Response<E>, Error> {
    fn entities(self) -> Result<Vec<E>, Error> {
        Ok(self?.entities())
    }

    fn entity(self) -> Result<E, Error> {
        self?.entity().map_err(Error::from)
    }

    fn try_entity(self) -> Result<Option<E>, Error> {
        self?.try_entity().map_err(Error::from)
    }

    fn count(self) -> Result<u32, Error> {
        Ok(self?.count())
    }
}
