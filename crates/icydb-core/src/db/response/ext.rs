use crate::{Key, db::response::Response, runtime_error::RuntimeError, traits::EntityKind};

///
/// ResponseExt
/// Ergonomic helpers for interpreting `Result<Response<E>, RuntimeError>`.
///
pub trait ResponseExt<E: EntityKind> {
    // --- entities ---

    fn entities(self) -> Result<Vec<E>, RuntimeError>;
    fn one_entity(self) -> Result<E, RuntimeError>;
    fn one_opt_entity(self) -> Result<Option<E>, RuntimeError>;

    // --- primary keys ---

    fn pks(self) -> Result<Vec<E::PrimaryKey>, RuntimeError>;
    fn one_pk(self) -> Result<E::PrimaryKey, RuntimeError>;
    fn one_opt_pk(self) -> Result<Option<E::PrimaryKey>, RuntimeError>;

    // --- keys ---

    fn keys(self) -> Result<Vec<Key>, RuntimeError>;
    fn one_key(self) -> Result<Key, RuntimeError>;
    fn one_opt_key(self) -> Result<Option<Key>, RuntimeError>;

    // --- views ---

    fn views(self) -> Result<Vec<E::ViewType>, RuntimeError>;
    fn one_view(self) -> Result<E::ViewType, RuntimeError>;
    fn one_opt_view(self) -> Result<Option<E::ViewType>, RuntimeError>;

    // --- introspection ---

    fn count(self) -> Result<u32, RuntimeError>;
}

impl<E: EntityKind> ResponseExt<E> for Result<Response<E>, RuntimeError> {
    // --- entities ---
    fn entities(self) -> Result<Vec<E>, RuntimeError> {
        Ok(self?.entities())
    }

    fn one_entity(self) -> Result<E, RuntimeError> {
        self?.one_entity()
    }

    fn one_opt_entity(self) -> Result<Option<E>, RuntimeError> {
        self?.one_opt_entity()
    }

    // --- primary keys ---

    fn pks(self) -> Result<Vec<E::PrimaryKey>, RuntimeError> {
        Ok(self?.pks())
    }

    fn one_pk(self) -> Result<E::PrimaryKey, RuntimeError> {
        self?.one_pk()
    }

    fn one_opt_pk(self) -> Result<Option<E::PrimaryKey>, RuntimeError> {
        self?.one_opt_pk()
    }

    // keys

    fn keys(self) -> Result<Vec<Key>, RuntimeError> {
        Ok(self?.keys())
    }

    fn one_key(self) -> Result<Key, RuntimeError> {
        self?.one_key()
    }

    fn one_opt_key(self) -> Result<Option<Key>, RuntimeError> {
        self?.one_opt_key()
    }

    // --- views ---

    fn views(self) -> Result<Vec<E::ViewType>, RuntimeError> {
        Ok(self?.views())
    }

    fn one_view(self) -> Result<E::ViewType, RuntimeError> {
        self?.one_view()
    }

    fn one_opt_view(self) -> Result<Option<E::ViewType>, RuntimeError> {
        self?.one_opt_view()
    }

    // --- introspection ---

    fn count(self) -> Result<u32, RuntimeError> {
        Ok(self?.count())
    }
}
