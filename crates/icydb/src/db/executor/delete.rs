use crate::{
    Error,
    db::{UniqueIndexHandle, map_response, query::v2::plan::LogicalPlan, response::Response},
    traits::EntityKind,
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

    /// Delete a single row using a unique index handle.
    pub fn by_unique_index(self, index: UniqueIndexHandle, entity: E) -> Result<Response<E>, Error>
    where
        E::PrimaryKey: FromKey,
    {
        map_response(self.inner.by_unique_index(index.as_core(), entity))
    }

    /// Execute a v2 logical plan.
    pub fn execute(self, plan: LogicalPlan) -> Result<Response<E>, Error> {
        map_response(self.inner.execute(plan))
    }
}
