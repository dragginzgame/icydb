use crate::{
    Error,
    db::{
        map_response, map_runtime,
        primitives::{FilterDsl, IntoFilterExpr},
        query::{LoadQuery, QueryPlan},
        response::Response,
    },
    traits::{EntityKind, FieldValue},
};
use icydb_core as core;
use std::{collections::HashMap, hash::Hash};

///
/// LoadExecutor
///

pub struct LoadExecutor<E: EntityKind> {
    inner: core::db::executor::LoadExecutor<E>,
}

impl<E: EntityKind> LoadExecutor<E> {
    pub(crate) const fn from_core(inner: core::db::executor::LoadExecutor<E>) -> Self {
        Self { inner }
    }

    /// Execute a query for a single primary key.
    pub fn one(&self, value: impl FieldValue) -> Result<Response<E>, Error> {
        map_response(self.inner.one(value))
    }

    /// Execute a query for the unit primary key.
    pub fn only(&self) -> Result<Response<E>, Error> {
        map_response(self.inner.only())
    }

    /// Execute a query matching multiple primary keys.
    pub fn many<I, V>(&self, values: I) -> Result<Response<E>, Error>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        map_response(self.inner.many(values))
    }

    /// Execute an unfiltered query for all rows.
    pub fn all(&self) -> Result<Response<E>, Error> {
        map_response(self.inner.all())
    }

    /// Execute a query built from a filter.
    pub fn filter<F, I>(&self, f: F) -> Result<Response<E>, Error>
    where
        F: FnOnce(FilterDsl) -> I,
        I: IntoFilterExpr,
    {
        map_response(self.inner.filter(f))
    }

    /// Execute a query and require exactly one row.
    pub fn require_one(&self, query: LoadQuery) -> Result<(), Error> {
        map_runtime(self.inner.require_one(query))
    }

    /// Require exactly one row by primary key.
    pub fn require_one_pk(&self, value: impl FieldValue) -> Result<(), Error> {
        map_runtime(self.inner.require_one_pk(value))
    }

    /// Require exactly one row from a filter.
    pub fn require_one_filter<F, I>(&self, f: F) -> Result<(), Error>
    where
        F: FnOnce(FilterDsl) -> I,
        I: IntoFilterExpr,
    {
        map_runtime(self.inner.require_one_filter(f))
    }

    /// Check whether at least one row matches the query.
    pub fn exists(&self, query: LoadQuery) -> Result<bool, Error> {
        map_runtime(self.inner.exists(query))
    }

    /// Check existence by primary key.
    ///
    /// This performs a direct, non-strict key lookup:
    /// - Missing rows return `false`
    /// - No deserialization is performed
    /// - No scan-based metrics are recorded
    ///
    /// This differs from `exists`, which executes a planned query.
    pub fn exists_one(&self, value: impl FieldValue) -> Result<bool, Error> {
        map_runtime(self.inner.exists_one(value))
    }

    /// Check existence with a filter.
    pub fn exists_filter<F, I>(&self, f: F) -> Result<bool, Error>
    where
        F: FnOnce(FilterDsl) -> I,
        I: IntoFilterExpr,
    {
        map_runtime(self.inner.exists_filter(f))
    }

    /// Check whether the table contains any rows.
    pub fn exists_any(&self) -> Result<bool, Error> {
        map_runtime(self.inner.exists_any())
    }

    /// Require at least one row by primary key.
    pub fn ensure_exists_one(&self, value: impl FieldValue) -> Result<(), Error> {
        map_runtime(self.inner.ensure_exists_one(value))
    }

    /// Require that all provided primary keys exist.
    ///
    /// This is an existence-only guard: missing keys return `NotFound`,
    /// and no deserialization is performed.
    pub fn ensure_exists_all<I, V>(&self, values: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        map_runtime(self.inner.ensure_exists_all(values))
    }

    /// Require at least one row from a filter.
    pub fn ensure_exists_filter<F, I>(&self, f: F) -> Result<(), Error>
    where
        F: FnOnce(FilterDsl) -> I,
        I: IntoFilterExpr,
    {
        map_runtime(self.inner.ensure_exists_filter(f))
    }

    /// Validate and return the query plan without executing.
    pub fn explain(self, query: LoadQuery) -> Result<QueryPlan, Error> {
        map_runtime(self.inner.explain(query))
    }

    /// Execute a full query and return a collection of entities.
    pub fn execute(&self, query: LoadQuery) -> Result<Response<E>, Error> {
        map_response(self.inner.execute(query))
    }

    /// Count rows matching a query.
    pub fn count(&self, query: LoadQuery) -> Result<u32, Error> {
        map_runtime(self.inner.count(query))
    }

    pub fn count_all(&self) -> Result<u32, Error> {
        map_runtime(self.inner.count_all())
    }

    /// Group rows matching a query and count them by a derived key.
    pub fn group_count_by<K, F>(
        &self,
        query: LoadQuery,
        key_fn: F,
    ) -> Result<HashMap<K, u32>, Error>
    where
        K: Eq + Hash,
        F: Fn(&E) -> K,
    {
        let entities = self.execute(query)?.entities();

        let mut counts = HashMap::new();
        for e in entities {
            *counts.entry(key_fn(&e)).or_insert(0) += 1;
        }

        Ok(counts)
    }
}
