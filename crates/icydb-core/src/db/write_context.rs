//! Module: db::write_context
//! Responsibility: canonical mutation mode and durable operation-time context.
//! Does not own: application normalization, validation, or accepted write policy.
//! Boundary: frontend mutation intent -> accepted after-image resolution.

use crate::types::Timestamp;

///
/// MutationMode
///
/// Exact row-existence contract selected for one database mutation.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationMode {
    /// Insert only when the target row is absent.
    Insert,

    /// Replace either an absent or existing target row.
    Replace,

    /// Update only when the target row exists.
    Update,
}

impl MutationMode {
    /// Project the canonical mutation mode into the bounded diagnostic taxonomy.
    #[must_use]
    pub(crate) const fn diagnostic_operation(
        self,
    ) -> icydb_diagnostic_code::DiagnosticMutationOperation {
        match self {
            Self::Insert => icydb_diagnostic_code::DiagnosticMutationOperation::Insert,
            Self::Replace => icydb_diagnostic_code::DiagnosticMutationOperation::Replace,
            Self::Update => icydb_diagnostic_code::DiagnosticMutationOperation::Update,
        }
    }
}

///
/// AcceptedWriteContext
///
/// Database-owned operation facts frozen before accepted after-image
/// resolution. Application normalizers and validators never receive this
/// context.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedWriteContext {
    operation_timestamp: Timestamp,
}

impl AcceptedWriteContext {
    /// Build one accepted write context from frontend-frozen operation facts.
    #[must_use]
    pub(in crate::db) const fn new(operation_timestamp: Timestamp) -> Self {
        Self {
            operation_timestamp,
        }
    }

    /// Return the durable timestamp shared by the logical operation.
    #[must_use]
    pub(in crate::db) const fn operation_timestamp(self) -> Timestamp {
        self.operation_timestamp
    }
}
