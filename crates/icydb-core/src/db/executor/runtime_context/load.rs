//! Module: executor::runtime_context::load
//! Responsibility: load-executor construction boundaries.
//! Does not own: scalar/grouped execution orchestration or route policy.
//! Boundary: shared executor setup helpers used by load runtime callsites.

use crate::{
    db::{Db, executor::pipeline::contracts::LoadExecutor},
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
}
