use crate::{
    Error,
    db::{UniqueIndexHandle, map_response, query::plan::LogicalPlan, response::Response},
    traits::EntityKind,
};
use icydb_core::{self as core, db::traits::FromKey};

///
/// DeleteExecutor
///

pub(crate) struct DeleteExecutor<E: EntityKind> {
    inner: core::db::executor::DeleteExecutor<E>,
}

impl<E: EntityKind> DeleteExecutor<E> {
    pub(crate) const fn from_core(inner: core::db::executor::DeleteExecutor<E>) -> Self {
        Self { inner }
    }

    #[must_use]
    pub(crate) const fn debug(self) -> Self {
        Self {
            inner: self.inner.debug(),
        }
    }

    /// Delete a single row using a unique index handle.
    pub(crate) fn by_unique_index(
        self,
        index: UniqueIndexHandle,
        entity: E,
    ) -> Result<Response<E>, Error>
    where
        E::PrimaryKey: FromKey,
    {
        map_response(self.inner.by_unique_index(index.as_core(), entity))
    }

    /// Execute a logical plan.
    pub(crate) fn execute(self, plan: LogicalPlan) -> Result<Response<E>, Error> {
        map_response(self.inner.execute(plan))
    }
}
