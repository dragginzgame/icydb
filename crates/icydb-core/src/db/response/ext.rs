use crate::{Error, Key, db::response::Response, traits::EntityKind};

///
/// ResponseExt
/// Ergonomic helpers for interpreting `Result<Response<E>, Error>`.
///
pub trait ResponseExt<E: EntityKind> {
    // --- entities ---

    fn entities(self) -> Result<Vec<E>, Error>;
    fn one_entity(self) -> Result<E, Error>;
    fn one_opt_entity(self) -> Result<Option<E>, Error>;

    // --- primary keys ---

    fn pks(self) -> Result<Vec<E::PrimaryKey>, Error>;
    fn one_pk(self) -> Result<E::PrimaryKey, Error>;
    fn one_opt_pk(self) -> Result<Option<E::PrimaryKey>, Error>;

    // --- keys ---

    fn keys(self) -> Result<Vec<Key>, Error>;
    fn one_key(self) -> Result<Key, Error>;
    fn one_opt_key(self) -> Result<Option<Key>, Error>;

    // --- views ---

    fn views(self) -> Result<Vec<E::ViewType>, Error>;
    fn one_view(self) -> Result<E::ViewType, Error>;
    fn one_opt_view(self) -> Result<Option<E::ViewType>, Error>;

    // --- introspection ---

    fn count(self) -> Result<u32, Error>;
}

impl<E: EntityKind> ResponseExt<E> for Result<Response<E>, Error> {
    // --- entities ---
    fn entities(self) -> Result<Vec<E>, Error> {
        Ok(self?.entities())
    }

    fn one_entity(self) -> Result<E, Error> {
        self?.one_entity()
    }

    fn one_opt_entity(self) -> Result<Option<E>, Error> {
        self?.one_opt_entity()
    }

    // --- primary keys ---

    fn pks(self) -> Result<Vec<E::PrimaryKey>, Error> {
        Ok(self?.pks())
    }

    fn one_pk(self) -> Result<E::PrimaryKey, Error> {
        self?.one_pk()
    }

    fn one_opt_pk(self) -> Result<Option<E::PrimaryKey>, Error> {
        self?.one_opt_pk()
    }

    // keys

    fn keys(self) -> Result<Vec<Key>, Error> {
        Ok(self?.keys())
    }

    fn one_key(self) -> Result<Key, Error> {
        self?.one_key()
    }

    fn one_opt_key(self) -> Result<Option<Key>, Error> {
        self?.one_opt_key()
    }

    // --- views ---

    fn views(self) -> Result<Vec<E::ViewType>, Error> {
        Ok(self?.views())
    }

    fn one_view(self) -> Result<E::ViewType, Error> {
        self?.one_view()
    }

    fn one_opt_view(self) -> Result<Option<E::ViewType>, Error> {
        self?.one_opt_view()
    }

    // --- introspection ---

    fn count(self) -> Result<u32, Error> {
        Ok(self?.count())
    }
}
