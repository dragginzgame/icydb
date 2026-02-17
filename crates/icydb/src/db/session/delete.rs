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
    traits::{CanisterKind, EntityKind, EntityValue, SingletonEntity, View},
    types::Id,
};
use icydb_core as core;

///
/// SessionDeleteQuery
///
/// Session-bound fluent wrapper for delete queries.
///

pub struct SessionDeleteQuery<'a, C: CanisterKind, E: EntityKind<Canister = C>> {
    pub(crate) inner: core::db::SessionDeleteQuery<'a, C, E>,
}

impl<C: CanisterKind, E: EntityKind<Canister = C>> SessionDeleteQuery<'_, C, E> {
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

impl<C: CanisterKind, E: EntityKind<Canister = C> + SingletonEntity> SessionDeleteQuery<'_, C, E> {
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
