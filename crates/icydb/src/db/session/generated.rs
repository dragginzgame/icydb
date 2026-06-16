//! Module: db::session::generated
//!
//! Responsibility: public session and fluent query facade.
//! Does not own: core execution, storage engines, or planner semantics.
//! Boundary: wraps core sessions with stable generated-code and application APIs.

use crate::{
    db::{DbSession, StorageReport},
    error::Error,
    traits::CanisterKind,
};

/// Execute one generated storage snapshot request through the hidden facade.
///
/// This helper keeps the generated metrics endpoint on the default snapshot
/// path so canister exports do not retain alias-remapping diagnostics helpers
/// they never use.
pub fn execute_generated_storage_report<C: CanisterKind>(
    session: &DbSession<C>,
) -> Result<StorageReport, Error> {
    Ok(session.inner.storage_report_default()?)
}
