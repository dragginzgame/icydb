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
pub(in crate::db) use bounded_cache::CacheEntryWeight;
#[cfg(feature = "sql")]
pub(in crate::db) use query::QueryPlanCacheReuse;
#[doc(hidden)]
pub use query::{
    MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES, MAX_TYPED_EXACT_KEY_BATCH_ITEMS,
    MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES, MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES,
};
pub(in crate::db) use request::RequestExecutionScope;
pub use request::{RequestBudgetSnapshot, RequestExecutionRoot};
pub(in crate::db) use response::finalize_structural_grouped_projection_result;
pub(in crate::db) use response::grouped_cursor_from_bytes;
#[cfg(feature = "sql")]
pub use sql::{
    SqlConstraintValidationPage, SqlConstraintValidationRevisionStatus,
    SqlConstraintValidationState, SqlDdlExecutionStatus, SqlDdlMutationKind,
    SqlDdlPreparationReport, SqlIntegrityError, SqlStatementDispatch, SqlStatementResult,
    SqlStatementShellSurface, SqlStatementSurface, sql_statement_dispatch,
    sql_statement_shell_surface, sql_statement_surface,
};
#[cfg(feature = "sql")]
pub(in crate::db::session) use write::{
    AcceptedStructuralMutation, AcceptedStructuralMutationTarget,
};

///
/// DbSession
///
/// Session-scoped database handle with execution routing.
///

pub struct DbSession<C: CanisterKind> {
    db: Db<C>,
    #[cfg(feature = "sql")]
    sql_returning_response_len: fn(crate::db::RowProjectionOutput) -> candid::Result<usize>,
}

impl<C: CanisterKind> DbSession<C> {
    /// Select the outward Candid projection envelope for precommit SQL bounds.
    ///
    /// The encoder must preserve `RowProjectionOutput`'s value encoding inside
    /// a fixed envelope independent of row contents. The facade installs its
    /// actual public response encoder; raw core sessions encode the projection.
    /// This is session-local framing, not schema authority or mutation policy.
    #[cfg(feature = "sql")]
    #[doc(hidden)]
    #[must_use]
    pub const fn __with_sql_returning_response_len(
        mut self,
        encode: fn(crate::db::RowProjectionOutput) -> candid::Result<usize>,
    ) -> Self {
        self.sql_returning_response_len = encode;
        self
    }

    /// Snapshot the aggregate request budget shared by this session's root.
    ///
    /// Reads the retained owner even outside its active call tree; observing
    /// capacity neither charges work nor reserves the next execution.
    #[must_use]
    pub fn request_budget(&self) -> RequestBudgetSnapshot {
        self.db.request_scope.request_budget()
    }

    /// Construct one session facade over a sealed runtime store registry.
    #[must_use]
    pub fn new(
        store: &'static LocalKey<StoreRegistry>,
        request_root: &RequestExecutionRoot,
    ) -> Self {
        Self {
            db: Db::new(store, request_root.scope()),
            #[cfg(feature = "sql")]
            sql_returning_response_len: sql::encoded_returning_response_len,
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
            #[cfg(feature = "sql")]
            sql_returning_response_len: sql::encoded_returning_response_len,
        })
    }
}
