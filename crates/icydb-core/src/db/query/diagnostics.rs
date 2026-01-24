//! Public, read-only diagnostics for query planning and execution.
//!
//! Diagnostics contract:
//! - `ExplainPlan` is deterministic for equivalent queries and plans.
//! - `PlanFingerprint` is stable within a major version (inputs are normalized).
//! - Execution trace events are best-effort diagnostics and may evolve.
//! - Diagnostics never execute queries unless explicitly requested.
//! - Diagnostics are observational only; they are not correctness proofs.

use crate::db::query::{
    builder::QueryExplain,
    plan::{AccessPath, ExplainPlan, PlanFingerprint},
};

///
/// QueryDiagnostics
///
/// Read-only planning diagnostics derived from a `QuerySpec`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryDiagnostics {
    pub explain: ExplainPlan,
    pub fingerprint: PlanFingerprint,
}

impl From<QueryExplain> for QueryDiagnostics {
    fn from(explain: QueryExplain) -> Self {
        Self {
            explain: explain.explain,
            fingerprint: explain.fingerprint,
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

pub use crate::db::executor::trace::{QueryTraceEvent, TraceAccess, TraceExecutorKind};

/// Public alias for trace access kinds in query diagnostics.
pub type QueryTraceAccess = TraceAccess;

/// Public alias for trace executor kinds in query diagnostics.
pub type QueryTraceExecutorKind = TraceExecutorKind;

pub(crate) fn trace_access_from_path(path: &AccessPath) -> TraceAccess {
    match path {
        AccessPath::ByKey(_) => TraceAccess::ByKey,
        AccessPath::ByKeys(keys) => TraceAccess::ByKeys {
            count: u32::try_from(keys.len()).unwrap_or(u32::MAX),
        },
        AccessPath::KeyRange { .. } => TraceAccess::KeyRange,
        AccessPath::IndexPrefix { index, values } => TraceAccess::IndexPrefix {
            name: index.name,
            prefix_len: u32::try_from(values.len()).unwrap_or(u32::MAX),
        },
        AccessPath::FullScan => TraceAccess::FullScan,
    }
}

#[must_use]
pub const fn start_event(
    fingerprint: PlanFingerprint,
    access: TraceAccess,
    executor: TraceExecutorKind,
) -> QueryTraceEvent {
    QueryTraceEvent::Start {
        fingerprint,
        executor,
        access: Some(access),
    }
}

#[must_use]
pub const fn finish_event(
    fingerprint: PlanFingerprint,
    access: TraceAccess,
    executor: TraceExecutorKind,
    rows: u64,
) -> QueryTraceEvent {
    QueryTraceEvent::Finish {
        fingerprint,
        executor,
        access: Some(access),
        rows,
    }
}
