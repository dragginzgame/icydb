use crate::{
    db::{
        Row,
        query::{
            Query,
            expr::{FilterExpr, SortExpr},
            predicate::Predicate,
        },
        response::Response,
        session::macros::{impl_session_materialization_methods, impl_session_query_shape_methods},
    },
    error::Error,
    traits::{EntityKind, EntityValue, SingletonEntity, View},
    types::Id,
};
use icydb_core as core;

///
/// SessionDeleteQuery
///
/// Session-bound fluent wrapper for delete queries.
///

pub struct SessionDeleteQuery<'a, E: EntityKind> {
    pub(crate) inner: core::db::FluentDeleteQuery<'a, E>,
}

impl<E: EntityKind> SessionDeleteQuery<'_, E> {
    // ------------------------------------------------------------------
    // Intent inspection
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        self.inner.query()
    }

    // ------------------------------------------------------------------
    // Primary-key access (query shaping)
    // ------------------------------------------------------------------

    impl_session_query_shape_methods!();

    // ------------------------------------------------------------------
    // Query refinement
    // ------------------------------------------------------------------

    // ------------------------------------------------------------------
    // Execution primitives
    // ------------------------------------------------------------------
    impl_session_materialization_methods!();
}

impl<E: EntityKind + SingletonEntity> SessionDeleteQuery<'_, E> {
    /// Delete the singleton entity.
    #[must_use]
    pub fn only(mut self) -> Self
    where
        E::Key: Default,
    {
        self.inner = self.inner.only();
        self
    }
}
