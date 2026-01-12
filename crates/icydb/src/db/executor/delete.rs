use crate::{
    Error,
    db::{
        UniqueIndexHandle, map_response, map_runtime,
        primitives::{FilterDsl, IntoFilterExpr},
        query::{DeleteQuery, QueryPlan},
        response::Response,
    },
    traits::{EntityKind, FieldValue, FromKey},
};
use icydb_core::{self as core};

///
/// DeleteExecutor
///

pub struct DeleteExecutor<E: EntityKind> {
    inner: core::db::executor::DeleteExecutor<E>,
}

impl<E: EntityKind> DeleteExecutor<E> {
    pub(crate) const fn from_core(inner: core::db::executor::DeleteExecutor<E>) -> Self {
        Self { inner }
    }

    #[must_use]
    pub const fn debug(self) -> Self {
        Self {
            inner: self.inner.debug(),
        }
    }

    /// Delete a single row by primary key.
    pub fn one(self, pk: impl FieldValue) -> Result<Response<E>, Error> {
        map_response(self.inner.one(pk))
    }

    /// Delete the unit-key row.
    pub fn only(self) -> Result<Response<E>, Error> {
        map_response(self.inner.only())
    }

    /// Delete multiple rows by primary keys.
    pub fn many<I, V>(self, values: I) -> Result<Response<E>, Error>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        map_response(self.inner.many(values))
    }

    /// Delete a single row using a unique index handle.
    pub fn by_unique_index(self, index: UniqueIndexHandle, entity: E) -> Result<Response<E>, Error>
    where
        E::PrimaryKey: FromKey,
    {
        map_response(self.inner.by_unique_index(index.as_core(), entity))
    }

    /// Delete a single row by an arbitrary field value.
    pub fn one_by_field(
        self,
        field: impl AsRef<str>,
        value: impl FieldValue,
    ) -> Result<Response<E>, Error> {
        map_response(self.inner.one_by_field(field, value))
    }

    /// Delete multiple rows by an arbitrary field.
    pub fn many_by_field<I, V>(
        self,
        field: impl AsRef<str>,
        values: I,
    ) -> Result<Response<E>, Error>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        map_response(self.inner.many_by_field(field, values))
    }

    /// Delete all rows.
    pub fn all(self) -> Result<Response<E>, Error> {
        map_response(self.inner.all())
    }

    /// Apply a filter builder and delete matches.
    pub fn filter<F, I>(self, f: F) -> Result<Response<E>, Error>
    where
        F: FnOnce(FilterDsl) -> I,
        I: IntoFilterExpr,
    {
        map_response(self.inner.filter(f))
    }

    pub fn ensure_delete_one(self, pk: impl FieldValue) -> Result<(), Error> {
        map_runtime(self.inner.ensure_delete_one(pk))
    }

    pub fn ensure_delete_any_by_pk<I, V>(self, pks: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        map_runtime(self.inner.ensure_delete_any_by_pk(pks))
    }

    pub fn ensure_delete_any<I, V>(self, values: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        map_runtime(self.inner.ensure_delete_any(values))
    }

    pub fn explain(self, query: DeleteQuery) -> Result<QueryPlan, Error> {
        map_runtime(self.inner.explain(query))
    }

    /// Execute a planner-based delete query.
    pub fn execute(self, query: DeleteQuery) -> Result<Response<E>, Error> {
        map_response(self.inner.execute(query))
    }
}
