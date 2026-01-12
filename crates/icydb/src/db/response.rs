pub use core::db::response::Page;

use crate::Error;
use icydb_core::{self as core, key::Key, traits::EntityKind};

/// Row
pub type Row<E> = (Key, E);

///
/// Response
/// Materialized query result: ordered `(Key, Entity)` pairs.
///

#[derive(Debug)]
pub struct Response<E: EntityKind>(core::db::response::Response<E>);

impl<E: EntityKind> Response<E> {
    /// helper
    pub(crate) const fn from_inner(inner: core::db::response::Response<E>) -> Self {
        Self(inner)
    }

    // ======================================================================
    // Cardinality (introspection only)
    // ======================================================================

    /// Number of rows in the response, truncated to `u32`.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub const fn count(&self) -> u32 {
        self.0.count()
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
        self.0.require_one().map_err(Error::from)
    }

    /// Require at least one row.
    pub fn require_some(&self) -> Result<(), Error> {
        self.0.require_some().map_err(Error::from)
    }

    /// Require exactly `expected` rows.
    pub fn require_len(&self, expected: u32) -> Result<(), Error> {
        self.0.require_len(expected).map_err(Error::from)
    }

    // ======================================================================
    // Row extractors (consume self)
    // ======================================================================

    /// Require exactly one row and return it.
    pub fn one(self) -> Result<Row<E>, Error> {
        self.0.one().map_err(Error::from)
    }

    /// Require at most one row and return it.
    pub fn one_opt(self) -> Result<Option<Row<E>>, Error> {
        self.0.one_opt().map_err(Error::from)
    }

    /// Convert the response into a page of entities with a `has_more` indicator.
    #[must_use]
    pub fn into_page(self, limit: usize) -> Page<E> {
        self.0.into_page(limit)
    }

    // ======================================================================
    // Key extractors
    // ======================================================================

    /// First key in the response, if present.
    #[must_use]
    pub fn key(&self) -> Option<Key> {
        self.0.key()
    }

    /// Collect all keys in order.
    #[must_use]
    pub fn keys(&self) -> Vec<Key> {
        self.0.keys()
    }

    /// Require exactly one row and return its key.
    pub fn one_key(self) -> Result<Key, Error> {
        self.0.one_key().map_err(Error::from)
    }

    /// Require at most one row and return its key.
    pub fn one_opt_key(self) -> Result<Option<Key>, Error> {
        self.0.one_opt_key().map_err(Error::from)
    }

    #[must_use]
    pub fn contains_key(&self, key: &Key) -> bool {
        self.0.contains_key(key)
    }

    // ======================================================================
    // Entity extractors
    // ======================================================================

    /// Consume the response and return the first entity, if any.
    #[must_use]
    pub fn entity(self) -> Option<E> {
        self.0.entity()
    }

    /// Consume the response and collect all entities.
    #[must_use]
    pub fn entities(self) -> Vec<E> {
        self.0.entities()
    }

    /// Require exactly one entity.
    pub fn one_entity(self) -> Result<E, Error> {
        self.0.one_entity().map_err(Error::from)
    }

    /// Require at most one entity.
    pub fn one_opt_entity(self) -> Result<Option<E>, Error> {
        self.0.one_opt_entity().map_err(Error::from)
    }

    // ======================================================================
    // Primary key extractors
    // ======================================================================

    /// First primary key in the response, if present.
    #[must_use]
    pub fn pk(&self) -> Option<E::PrimaryKey> {
        self.0.pk()
    }

    /// Collect all primary keys in order.
    #[must_use]
    pub fn pks(&self) -> Vec<E::PrimaryKey> {
        self.0.pks()
    }

    /// Require exactly one primary key.
    pub fn one_pk(self) -> Result<E::PrimaryKey, Error> {
        self.0.one_pk().map_err(Error::from)
    }

    /// Require at most one primary key.
    pub fn one_opt_pk(self) -> Result<Option<E::PrimaryKey>, Error> {
        self.0.one_opt_pk().map_err(Error::from)
    }

    // ======================================================================
    // View extractors
    // ======================================================================

    /// Convert the first entity to its view type, if present.
    #[must_use]
    pub fn view(self) -> Option<E::ViewType> {
        self.0.view()
    }

    /// Require exactly one view.
    pub fn one_view(self) -> Result<E::ViewType, Error> {
        self.0.one_view().map_err(Error::from)
    }

    /// Require at most one view.
    pub fn one_opt_view(self) -> Result<Option<E::ViewType>, Error> {
        self.0.one_opt_view().map_err(Error::from)
    }

    /// Convert all entities to their view types.
    #[must_use]
    pub fn views(self) -> Vec<E::ViewType> {
        self.0.views()
    }

    // ======================================================================
    // Arbitrary row access (no cardinality guarantees)
    // ======================================================================

    /// Return the first row in the response, if any.
    #[must_use]
    pub fn first(self) -> Option<Row<E>> {
        self.0.first()
    }

    /// Return the first entity in the response, if any.
    #[must_use]
    pub fn first_entity(self) -> Option<E> {
        self.0.first_entity()
    }

    /// Return the first primary key in the response, if any.
    #[must_use]
    pub fn first_pk(self) -> Option<E::PrimaryKey> {
        self.0.first_pk()
    }
}

impl<E: EntityKind> From<core::db::response::Response<E>> for Response<E> {
    fn from(inner: core::db::response::Response<E>) -> Self {
        Self::from_inner(inner)
    }
}

impl<E: EntityKind> IntoIterator for Response<E> {
    type Item = Row<E>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

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
