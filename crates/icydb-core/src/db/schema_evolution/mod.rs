//! Module: db::schema_evolution
//! Responsibility: schema/data evolution descriptors and migration-plan derivation.
//! Does not own: migration execution, commit durability, or storage layout.
//! Boundary: schema/identity/model inputs -> validated migration plans -> migration engine.
//! This module defines schema/data evolution, derives `MigrationPlan` instances
//! that are executed by `db::migration`, and does not execute migrations itself.

mod descriptor;
mod execution;
mod planner;
mod registry;
#[cfg(test)]
mod tests;

pub use descriptor::{
    SchemaDataTransformation, SchemaMigrationDescriptor, SchemaMigrationEntityTarget,
    SchemaMigrationRowOp, SchemaMigrationStepIntent,
};
pub use execution::SchemaMigrationExecutionOutcome;
pub use planner::SchemaMigrationPlanner;
pub use registry::{MigrationRegistry, MigrationRegistryKey};

pub(in crate::db) use execution::execute_schema_migration_descriptor;
