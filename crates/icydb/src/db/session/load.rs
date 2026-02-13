use crate::{
    db::{
        Row,
        query::{FilterExpr, Predicate, Query, SortExpr},
        response::{PagedResponse, Response},
        session::macros::{impl_session_materialization_methods, impl_session_query_shape_methods},
    },
    error::Error,
    traits::{CanisterKind, EntityKind, EntityValue, SingletonEntity, View},
    types::Id,
};
use icydb_core as core;
use std::{collections::HashMap, hash::Hash};

///
/// SessionLoadQuery
///
/// Session-bound fluent wrapper for load queries.
///

pub struct SessionLoadQuery<'a, C: CanisterKind, E: EntityKind<Canister = C>> {
    pub(crate) inner: core::db::query::SessionLoadQuery<'a, C, E>,
}

///
/// PagedLoadQuery
///
/// Facade wrapper for cursor-pagination mode.
/// Returns typed view items plus an opaque continuation cursor.
///

pub struct PagedLoadQuery<'a, C: CanisterKind, E: EntityKind<Canister = C>> {
    pub(crate) inner: core::db::query::PagedLoadQuery<'a, C, E>,
}

impl<'a, C: CanisterKind, E: EntityKind<Canister = C>> SessionLoadQuery<'a, C, E> {
    // ------------------------------------------------------------------
    // Intent inspection
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        self.inner.query()
    }

    // ------------------------------------------------------------------
    // Primary-key access (semantic)
    // ------------------------------------------------------------------

    impl_session_query_shape_methods!();

    // ------------------------------------------------------------------
    // Query refinement
    // ------------------------------------------------------------------

    /// Skip a number of rows in the ordered result stream.
    ///
    /// Pagination is only valid with explicit ordering; combine `offset` and/or
    /// `limit` with `order_by(...)` or planning fails.
    #[must_use]
    pub fn offset(mut self, offset: u32) -> Self {
        self.inner = self.inner.offset(offset);
        self
    }

    /// Attach an opaque cursor token for continuation pagination.
    #[must_use]
    pub fn cursor(mut self, token: impl Into<String>) -> Self {
        self.inner = self.inner.cursor(token);
        self
    }

    // ------------------------------------------------------------------
    // Execution primitives
    // ------------------------------------------------------------------
    impl_session_materialization_methods!();

    /// Enter typed cursor-pagination mode for this query.
    ///
    /// Cursor pagination requires explicit ordering and limit, and disallows offset.
    /// Continuation is best-effort and forward-only over live state:
    /// deterministic per request under canonical ordering, with no
    /// snapshot/version pinned across requests.
    pub fn page(self) -> Result<PagedLoadQuery<'a, C, E>, Error> {
        Ok(PagedLoadQuery {
            inner: self.inner.page()?,
        })
    }

    /// Execute as cursor pagination, returning views plus an opaque continuation token.
    pub fn execute_paged(self) -> Result<PagedResponse<E>, Error>
    where
        E: EntityValue,
    {
        self.page()?.execute()
    }

    // ------------------------------------------------------------------
    // Aggregation helpers
    // ------------------------------------------------------------------

    pub fn group_count_by<K>(self, key: impl Fn(&E) -> K) -> Result<HashMap<K, u32>, Error>
    where
        E: EntityValue,
        K: Eq + Hash,
    {
        let entities = self.inner.execute()?.entities();
        let mut counts = HashMap::new();
        for entity in entities {
            *counts.entry(key(&entity)).or_insert(0) += 1;
        }
        Ok(counts)
    }

    // ------------------------------------------------------------------
    // Convenience aliases (semantic sugar)
    // ------------------------------------------------------------------

    pub fn one(&self) -> Result<E, Error>
    where
        E: EntityValue,
    {
        self.entity()
    }

    pub fn one_opt(&self) -> Result<Option<E>, Error>
    where
        E: EntityValue,
    {
        self.try_entity()
    }

    pub fn all(&self) -> Result<Vec<E>, Error>
    where
        E: EntityValue,
    {
        self.entities()
    }
}

impl<C: CanisterKind, E: EntityKind<Canister = C>> PagedLoadQuery<'_, C, E> {
    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        self.inner.query()
    }

    /// Attach an opaque continuation cursor token for the next page.
    #[must_use]
    pub fn cursor(mut self, token: impl Into<String>) -> Self {
        self.inner = self.inner.cursor(token);
        self
    }

    /// Execute in cursor-pagination mode.
    ///
    /// Continuation is best-effort and forward-only over live state:
    /// deterministic per request under canonical ordering, with no
    /// snapshot/version pinned across requests.
    pub fn execute(self) -> Result<PagedResponse<E>, Error>
    where
        E: EntityValue,
    {
        let (items, next_cursor) = self.inner.execute()?;

        Ok(PagedResponse {
            items: items.views(),
            next_cursor: next_cursor.map(|bytes| encode_hex_cursor(&bytes)),
        })
    }
}

fn encode_hex_cursor(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

impl<C: CanisterKind, E: EntityKind<Canister = C> + SingletonEntity> SessionLoadQuery<'_, C, E> {
    /// Load the singleton entity.
    #[must_use]
    pub fn only(mut self) -> Self
    where
        E::Key: Default,
    {
        self.inner = self.inner.only();
        self
    }
}
