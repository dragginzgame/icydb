//! Public, read-only diagnostics for query planning and execution.
//!
//! Diagnostics contract:
//! - `ExplainPlan` is deterministic for equivalent queries and plans.
//! - `PlanFingerprint` is stable within a major version (inputs are normalized).
//! - Execution trace events are best-effort diagnostics and may evolve.
//! - Diagnostics never execute queries unless explicitly requested.
//! - Diagnostics are observational only; they are not correctness proofs.

pub use crate::db::executor::trace::{QueryTraceEvent, TraceAccess, TraceExecutorKind, TracePhase};
use crate::db::query::plan::{ExplainPlan, PlanFingerprint};

///
/// QueryDiagnostics
///
/// Read-only planning diagnostics derived from a `Query`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryDiagnostics {
    pub explain: ExplainPlan,
    pub fingerprint: PlanFingerprint,
}

impl From<ExplainPlan> for QueryDiagnostics {
    fn from(explain: ExplainPlan) -> Self {
        let fingerprint = explain.fingerprint();
        Self {
            explain,
            fingerprint,
        }
    }
}

///
/// QueryExecutionDiagnostics
///
/// Read-only execution diagnostics emitted for a single query execution.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryExecutionDiagnostics {
    pub fingerprint: PlanFingerprint,
    pub events: Vec<QueryTraceEvent>,
}

/// Public alias for trace access kinds in query diagnostics.
pub type QueryTraceAccess = TraceAccess;

/// Public alias for trace executor kinds in query diagnostics.
pub type QueryTraceExecutorKind = TraceExecutorKind;

/// Public alias for trace phase kinds in query diagnostics.
pub type QueryTracePhase = TracePhase;
