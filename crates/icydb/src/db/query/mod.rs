pub mod expr;

use icydb_core as core;

pub use expr::{FilterExpr, OrderDirection, SortExpr};

///
/// Re-exports
/// Query planning types are exposed for diagnostics and intent composition.
///
pub use core::db::query::{
    DeleteSpec, LoadSpec, Query, QueryMode, ReadConsistency, SortLowerError, builder,
    builder::*,
    diagnostics::{
        QueryDiagnostics, QueryExecutionDiagnostics, QueryTraceAccess, QueryTraceEvent,
        QueryTraceExecutorKind, QueryTracePhase,
    },
    predicate,
    predicate::Predicate,
};

pub mod plan {
    pub use icydb_core::db::query::plan::{
        ExplainAccessPath, ExplainDeleteLimit, ExplainOrder, ExplainOrderBy, ExplainPagination,
        ExplainPlan, ExplainPredicate, OrderDirection, PlanError, PlanFingerprint,
    };
}
