use icydb_core as core;

///
/// Re-exports
/// Query planning types are exposed for diagnostics and intent composition.
///
pub use core::db::query::{
    DeleteSpec, LoadSpec, Query, QueryMode, ReadConsistency, builder,
    builder::*,
    diagnostics::{
        QueryDiagnostics, QueryExecutionDiagnostics, QueryTraceAccess, QueryTraceEvent,
        QueryTraceExecutorKind, QueryTracePhase,
    },
    predicate,
};

pub mod plan {
    pub use icydb_core::db::query::plan::{
        ExplainAccessPath, ExplainDeleteLimit, ExplainOrder, ExplainOrderBy, ExplainPagination,
        ExplainPlan, ExplainPredicate, ExplainProjection, OrderDirection, PlanError,
        PlanFingerprint,
    };
}
