//! Module: session
//! Responsibility: user-facing query/write execution facade over db executors.
//! Does not own: planning semantics, cursor validation rules, or storage mutation protocol.
//! Boundary: converts fluent/query intent calls into executor operations and response DTOs.

mod accepted_schema;
mod bounded_cache;
mod catalog;
mod integrity;
mod mutation_job;
mod query;
mod read_set;
mod request;
mod response;
mod resumable_job;
#[cfg(feature = "sql")]
mod sql;
mod write;

#[cfg(all(test, feature = "sql"))]
mod tests;

use crate::{
    db::{Db, StoreRegistry},
    traits::CanisterKind,
};
use std::thread::LocalKey;

pub(in crate::db) use accepted_schema::AcceptedSchemaCatalogContext;
#[doc(hidden)]
pub use query::{
    MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES, MAX_TYPED_EXACT_KEY_BATCH_ITEMS,
    MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES, MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES,
};
pub use request::RequestExecutionRoot;
pub(in crate::db) use request::RequestExecutionScope;
pub(in crate::db) use response::finalize_structural_grouped_projection_result;
pub(in crate::db) use response::grouped_cursor_from_bytes;
#[cfg(feature = "sql")]
pub use sql::{
    SqlConstraintValidationPage, SqlConstraintValidationRevisionStatus,
    SqlConstraintValidationState, SqlDdlExecutionStatus, SqlDdlMutationKind,
    SqlDdlPreparationReport, SqlIntegrityError, SqlStatementDispatch, SqlStatementResult,
    SqlStatementShellSurface, SqlStatementSurface, sql_statement_dispatch,
    sql_statement_entity_name, sql_statement_shell_surface, sql_statement_surface,
};
#[cfg(feature = "sql")]
pub(in crate::db::session) use write::{
    AcceptedStructuralMutation, AcceptedStructuralMutationTarget,
    structural_data_key_from_runtime_values,
};

///
/// DbSession
///
/// Session-scoped database handle with debug policy and execution routing.
///

pub struct DbSession<C: CanisterKind> {
    db: Db<C>,
    debug: bool,
}

impl<C: CanisterKind> DbSession<C> {
    /// Construct one session facade over a sealed runtime store registry.
    #[must_use]
    pub fn new(
        store: &'static LocalKey<StoreRegistry>,
        request_root: &RequestExecutionRoot,
    ) -> Self {
        Self {
            db: Db::new(store, request_root.scope()),
            debug: false,
        }
    }

    /// Drive one bounded startup page while retaining its persisted failure owner.
    pub(in crate::db) fn drive_startup_recovery_page_with_failure_authority(
        &self,
    ) -> Result<bool, crate::db::commit::StartupRecoveryFailure> {
        self.db.drive_startup_recovery_page_with_failure_authority()
    }

    /// Construct a session from the active synchronous request scope.
    ///
    /// Generated zero-argument `db!()` wiring uses this entry. `None` means
    /// that the caller did not establish a request execution boundary.
    #[doc(hidden)]
    #[must_use]
    pub fn __new_from_current_request(store: &'static LocalKey<StoreRegistry>) -> Option<Self> {
        request::current_request_scope().map(|scope| Self {
            db: Db::new(store, scope),
            debug: false,
        })
    }

    /// Enable debug execution behavior where supported by executors.
    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.debug = true;
        self
    }
}
