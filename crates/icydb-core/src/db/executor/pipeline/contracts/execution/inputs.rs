//! Module: db::executor::pipeline::contracts::inputs
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::contracts::inputs.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        Context,
        executor::{
            AccessStreamBindings, ExecutionPreparation, traversal::row_read_consistency_for_plan,
        },
        predicate::MissingRowPolicy,
        query::plan::AccessPlannedQuery,
    },
    traits::{EntityKind, EntityValue},
};

///
/// ExecutionInputs
///
/// Shared immutable execution inputs for one load execution attempt.
/// Keeps fast-path dispatch signatures compact without changing behavior.
///

pub(in crate::db::executor) struct ExecutionInputs<'a, E: EntityKind + EntityValue> {
    ctx: &'a Context<'a, E>,
    plan: &'a AccessPlannedQuery<E::Key>,
    stream_bindings: AccessStreamBindings<'a>,
    execution_preparation: &'a ExecutionPreparation,
}

impl<'a, E> ExecutionInputs<'a, E>
where
    E: EntityKind + EntityValue,
{
    /// Construct one scalar execution-input projection payload.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        ctx: &'a Context<'a, E>,
        plan: &'a AccessPlannedQuery<E::Key>,
        stream_bindings: AccessStreamBindings<'a>,
        execution_preparation: &'a ExecutionPreparation,
    ) -> Self {
        Self {
            ctx,
            plan,
            stream_bindings,
            execution_preparation,
        }
    }
    /// Borrow recovered execution context for row/index reads.
    pub(in crate::db::executor) const fn ctx(&self) -> &Context<'_, E> {
        self.ctx
    }

    /// Borrow logical access plan payload for this execution attempt.
    pub(in crate::db::executor) const fn plan(&self) -> &AccessPlannedQuery<E::Key> {
        self.plan
    }

    /// Borrow lowered access stream bindings for this execution attempt.
    pub(in crate::db::executor) const fn stream_bindings(&self) -> &AccessStreamBindings<'_> {
        &self.stream_bindings
    }

    /// Borrow precomputed execution-preparation payloads.
    pub(in crate::db::executor) const fn execution_preparation(&self) -> &ExecutionPreparation {
        self.execution_preparation
    }

    /// Return row-read missing-row policy for this execution attempt.
    pub(in crate::db::executor) const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(self.plan)
    }
}
