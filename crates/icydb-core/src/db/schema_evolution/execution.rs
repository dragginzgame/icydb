//! Module: db::schema_evolution::execution
//! Responsibility: guard schema migration execution through the completed registry.
//! Does not own: migration row-op execution or commit durability.
//! Boundary: registry + planner -> migration execution engine.

use crate::{
    db::{
        Db,
        migration::{self, MigrationRunOutcome, MigrationRunState},
        schema_evolution::{MigrationRegistry, SchemaMigrationDescriptor, SchemaMigrationPlanner},
    },
    error::InternalError,
    traits::CanisterKind,
};

///
/// SchemaMigrationExecutionOutcome
///
/// SchemaMigrationExecutionOutcome reports whether schema evolution skipped an
/// already-applied migration or delegated a planned migration to `db::migration`.
/// The lower migration outcome is preserved unchanged when execution occurs.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchemaMigrationExecutionOutcome {
    AlreadyApplied,
    Executed(MigrationRunOutcome),
}

impl SchemaMigrationExecutionOutcome {
    /// Return whether schema evolution skipped execution because the registry
    /// already contains this migration id/version.
    #[must_use]
    pub const fn already_applied(self) -> bool {
        matches!(self, Self::AlreadyApplied)
    }

    /// Return the lower migration-run outcome when execution occurred.
    #[must_use]
    pub const fn migration_outcome(self) -> Option<MigrationRunOutcome> {
        match self {
            Self::AlreadyApplied => None,
            Self::Executed(outcome) => Some(outcome),
        }
    }
}

/// Execute one schema migration descriptor through the derivation layer.
pub(in crate::db) fn execute_schema_migration_descriptor<C: CanisterKind>(
    db: &Db<C>,
    registry: &mut MigrationRegistry,
    planner: &SchemaMigrationPlanner,
    descriptor: &SchemaMigrationDescriptor,
    max_steps: usize,
) -> Result<SchemaMigrationExecutionOutcome, InternalError> {
    // Phase 1: enforce completed-migration idempotency before deriving a row-op
    // plan or touching the lower migration engine.
    if registry.is_applied(descriptor.migration_id(), descriptor.version()) {
        return Ok(SchemaMigrationExecutionOutcome::AlreadyApplied);
    }

    // Phase 2: derive and execute through the existing migration engine. This
    // function intentionally does not duplicate step execution semantics.
    let plan = planner.plan(descriptor)?;
    let outcome = migration::execute_migration_plan(db, &plan, max_steps)?;

    // Phase 3: record completion only after the lower engine reports a completed
    // run. Bounded runs that need resume must remain executable.
    if matches!(outcome.state(), MigrationRunState::Complete) {
        registry.record_applied(descriptor.migration_id(), descriptor.version());
    }

    Ok(SchemaMigrationExecutionOutcome::Executed(outcome))
}
