//! Module: icydb_testing_sqlite_reference
//! Responsibility: pinned SQLite reference execution for test-only SQL evidence.
//! Does not own: IcyDB SQL semantics, production execution, or performance verdicts.
//! Boundary: provides one bundled environment, fixture, adapter, and compact profile.

mod adapter;
mod environment;
mod error;
mod mutation;
mod profile;
mod value;

#[cfg(test)]
mod tests;

pub use adapter::{execute_generated_select_case, execute_sqlite_reference_scenario};
pub use environment::{
    SqliteEnvironmentContract, SqliteEnvironmentIdentity, current_sqlite_environment_contract,
    observe_sqlite_environment,
};
pub use error::{SqliteAdapterError, SqliteAdapterErrorKind};
pub use mutation::{MutationSqliteEvidence, execute_generated_mutation_sequence};
pub use profile::{
    SQLITE_REFERENCE_FIXTURE_ROWS, SqliteReferenceFamily, SqliteReferenceFixtureRow,
    SqliteReferencePredicateFamily, SqliteReferenceScenario, SqliteReferenceWindow,
    required_sqlite_reference_scenarios,
};
pub use value::{
    SqliteReferenceColumnKind, SqliteReferenceResult, SqliteReferenceRowOrder, SqliteReferenceValue,
};
