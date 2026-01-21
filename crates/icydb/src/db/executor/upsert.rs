use crate::{
    Error,
    db::{UniqueIndexHandle, map_runtime},
    traits::EntityKind,
};
use icydb_core::{self as core, db::traits::FromKey};

// re-exports
pub use core::db::executor::UpsertResult;

///
/// UpsertExecutor
///

pub struct UpsertExecutor<E: EntityKind> {
    inner: core::db::executor::UpsertExecutor<E>,
}

impl<E: EntityKind> UpsertExecutor<E>
where
    E::PrimaryKey: FromKey,
{
    pub(crate) const fn from_core(inner: core::db::executor::UpsertExecutor<E>) -> Self {
        Self { inner }
    }

    /// Enable debug logging for subsequent upsert operations.
    #[must_use]
    pub const fn debug(self) -> Self {
        Self {
            inner: self.inner.debug(),
        }
    }

    /// Upsert using a unique index specification.
    pub fn by_unique_index(&self, index: UniqueIndexHandle, entity: E) -> Result<E, Error> {
        map_runtime(self.inner.by_unique_index(index.as_core(), entity))
    }

    /// Upsert using a unique index specification with a merge closure.
    pub fn by_unique_index_merge<F>(
        &self,
        index: UniqueIndexHandle,
        entity: E,
        merge: F,
    ) -> Result<E, Error>
    where
        F: FnOnce(E, E) -> E,
    {
        map_runtime(
            self.inner
                .by_unique_index_merge(index.as_core(), entity, merge),
        )
    }

    /// Upsert using a unique index specification with a merge closure, returning an insert/update flag.
    pub fn by_unique_index_merge_result<F>(
        &self,
        index: UniqueIndexHandle,
        entity: E,
        merge: F,
    ) -> Result<UpsertResult<E>, Error>
    where
        F: FnOnce(E, E) -> E,
    {
        map_runtime(
            self.inner
                .by_unique_index_merge_result(index.as_core(), entity, merge),
        )
    }

    /// Upsert using a unique index specification, returning an insert/update flag.
    pub fn by_unique_index_result(
        &self,
        index: UniqueIndexHandle,
        entity: E,
    ) -> Result<UpsertResult<E>, Error> {
        map_runtime(self.inner.by_unique_index_result(index.as_core(), entity))
    }

    /// Upsert using a unique index identified by its field list.
    pub fn by_unique_fields(&self, fields: &[&str], entity: E) -> Result<E, Error> {
        map_runtime(self.inner.by_unique_fields(fields, entity))
    }

    /// Upsert using a unique index identified by its field list with a merge closure.
    pub fn by_unique_fields_merge<F>(
        &self,
        fields: &[&str],
        entity: E,
        merge: F,
    ) -> Result<E, Error>
    where
        F: FnOnce(E, E) -> E,
    {
        map_runtime(self.inner.by_unique_fields_merge(fields, entity, merge))
    }

    /// Upsert using a unique index identified by its field list with a merge closure, returning an insert/update flag.
    pub fn by_unique_fields_merge_result<F>(
        &self,
        fields: &[&str],
        entity: E,
        merge: F,
    ) -> Result<UpsertResult<E>, Error>
    where
        F: FnOnce(E, E) -> E,
    {
        map_runtime(
            self.inner
                .by_unique_fields_merge_result(fields, entity, merge),
        )
    }

    /// Upsert using a unique index identified by its field list, returning an insert/update flag.
    pub fn by_unique_fields_result(
        &self,
        fields: &[&str],
        entity: E,
    ) -> Result<UpsertResult<E>, Error> {
        map_runtime(self.inner.by_unique_fields_result(fields, entity))
    }
}
