//! Module: executor::runtime_context::load
//! Responsibility: load-executor construction and recovered-context helper boundaries.
//! Does not own: scalar/grouped execution orchestration or route policy.
//! Boundary: shared executor setup helpers used by load runtime callsites.

use crate::{
    db::{Db, executor::pipeline::contracts::LoadExecutor},
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Construct one load executor bound to a database handle and debug mode.
    #[must_use]
    pub(in crate::db) const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self { db, debug }
    }

    /// Recover one canonical read context for kernel-owned execution setup.
    pub(in crate::db::executor) fn recovered_context(
        &self,
    ) -> Result<crate::db::Context<'_, E>, InternalError> {
        self.db.recovered_context::<E>()
    }
}
