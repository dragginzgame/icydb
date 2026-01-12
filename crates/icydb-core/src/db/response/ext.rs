use crate::{db::response::Response, error::InternalError, key::Key, traits::EntityKind};

///
/// ResponseExt
/// Ergonomic helpers for interpreting `Result<Response<E>, InternalError>`.
///
pub trait ResponseExt<E: EntityKind> {
    // --- entities ---

    fn entities(self) -> Result<Vec<E>, InternalError>;
    fn one_entity(self) -> Result<E, InternalError>;
    fn one_opt_entity(self) -> Result<Option<E>, InternalError>;

    // --- primary keys ---

    fn pks(self) -> Result<Vec<E::PrimaryKey>, InternalError>;
    fn one_pk(self) -> Result<E::PrimaryKey, InternalError>;
    fn one_opt_pk(self) -> Result<Option<E::PrimaryKey>, InternalError>;

    // --- keys ---

    fn keys(self) -> Result<Vec<Key>, InternalError>;
    fn one_key(self) -> Result<Key, InternalError>;
    fn one_opt_key(self) -> Result<Option<Key>, InternalError>;

    // --- views ---

    fn views(self) -> Result<Vec<E::ViewType>, InternalError>;
    fn one_view(self) -> Result<E::ViewType, InternalError>;
    fn one_opt_view(self) -> Result<Option<E::ViewType>, InternalError>;

    // --- introspection ---

    fn count(self) -> Result<u32, InternalError>;
}

impl<E: EntityKind> ResponseExt<E> for Result<Response<E>, InternalError> {
    // --- entities ---
    fn entities(self) -> Result<Vec<E>, InternalError> {
        Ok(self?.entities())
    }

    fn one_entity(self) -> Result<E, InternalError> {
        self?.one_entity()
    }

    fn one_opt_entity(self) -> Result<Option<E>, InternalError> {
        self?.one_opt_entity()
    }

    // --- primary keys ---

    fn pks(self) -> Result<Vec<E::PrimaryKey>, InternalError> {
        Ok(self?.pks())
    }

    fn one_pk(self) -> Result<E::PrimaryKey, InternalError> {
        self?.one_pk()
    }

    fn one_opt_pk(self) -> Result<Option<E::PrimaryKey>, InternalError> {
        self?.one_opt_pk()
    }

    // keys

    fn keys(self) -> Result<Vec<Key>, InternalError> {
        Ok(self?.keys())
    }

    fn one_key(self) -> Result<Key, InternalError> {
        self?.one_key()
    }

    fn one_opt_key(self) -> Result<Option<Key>, InternalError> {
        self?.one_opt_key()
    }

    // --- views ---

    fn views(self) -> Result<Vec<E::ViewType>, InternalError> {
        Ok(self?.views())
    }

    fn one_view(self) -> Result<E::ViewType, InternalError> {
        self?.one_view()
    }

    fn one_opt_view(self) -> Result<Option<E::ViewType>, InternalError> {
        self?.one_opt_view()
    }

    // --- introspection ---

    fn count(self) -> Result<u32, InternalError> {
        Ok(self?.count())
    }
}
