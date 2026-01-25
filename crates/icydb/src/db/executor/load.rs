use crate::{
    Error,
    db::{map_response, map_runtime, query::plan::ExecutablePlan, response::Response},
    traits::EntityKind,
};
use icydb_core as core;
use std::{collections::HashMap, hash::Hash};

///
/// LoadExecutor
///

pub(crate) struct LoadExecutor<E: EntityKind> {
    inner: core::db::executor::LoadExecutor<E>,
}

impl<E: EntityKind> LoadExecutor<E> {
    pub(crate) const fn from_core(inner: core::db::executor::LoadExecutor<E>) -> Self {
        Self { inner }
    }

    /// Execute a logical plan.
    pub(crate) fn execute(&self, plan: ExecutablePlan<E>) -> Result<Response<E>, Error> {
        map_response(self.inner.execute(plan))
    }

    /// Execute a plan and require exactly one row.
    pub(crate) fn require_one(&self, plan: ExecutablePlan<E>) -> Result<(), Error> {
        map_runtime(self.inner.require_one(plan))
    }

    /// Count rows matching a plan.
    pub(crate) fn count(&self, plan: ExecutablePlan<E>) -> Result<u32, Error> {
        map_runtime(self.inner.count(plan))
    }

    /// Group rows matching a plan and count them by a derived key.
    pub(crate) fn group_count_by<K, F>(
        &self,
        plan: ExecutablePlan<E>,
        key_fn: F,
    ) -> Result<HashMap<K, u32>, Error>
    where
        K: Eq + Hash,
        F: Fn(&E) -> K,
    {
        map_runtime(self.inner.group_count_by(plan, key_fn))
    }
}
