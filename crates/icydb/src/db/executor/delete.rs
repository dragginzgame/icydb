use crate::{
    Error,
    db::{
        UniqueIndexHandle, map_response, map_runtime,
        primitives::{FilterDsl, FilterExt, IntoFilterExpr},
        query::{DeleteQuery, QueryPlan},
        response::Response,
    },
    traits::{EntityKind, FieldValue},
};
use icydb_core::{self as core, db::traits::FromKey};

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
        let query = DeleteQuery::new().one_by_field(field, value);
        map_response(self.inner.execute(query))
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
        let query = DeleteQuery::new().many_by_field(field, values);
        map_response(self.inner.execute(query))
    }

    /// Delete all rows.
    pub fn all(self) -> Result<Response<E>, Error> {
        map_response(self.inner.execute(DeleteQuery::new()))
    }

    /// Apply a filter builder and delete matches.
    pub fn filter<F, I>(self, f: F) -> Result<Response<E>, Error>
    where
        F: FnOnce(FilterDsl) -> I,
        I: IntoFilterExpr,
    {
        let query = DeleteQuery::new().filter(f);
        map_response(self.inner.execute(query))
    }

    pub fn explain(self, query: DeleteQuery) -> Result<QueryPlan, Error> {
        map_runtime(self.inner.explain(query))
    }

    /// Execute a planner-based delete query.
    pub fn execute(self, query: DeleteQuery) -> Result<Response<E>, Error> {
        map_response(self.inner.execute(query))
    }
}
