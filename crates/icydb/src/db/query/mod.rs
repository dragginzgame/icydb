pub mod expr;

use icydb_core as core;

pub use expr::{FilterExpr, OrderDirection, SortExpr};

/// Stable query facade surface.
pub use core::db::query::{
    Query, ReadConsistency, builder, builder::*, predicate, predicate::Predicate,
};

/// Planner and intent internals retained for compatibility.
#[doc(hidden)]
pub use core::db::query::{DeleteSpec, LoadSpec, QueryMode, SortLowerError};

/// Diagnostics internals retained for compatibility.
#[doc(hidden)]
pub use core::db::query::diagnostics::{
    QueryDiagnostics, QueryExecutionDiagnostics, QueryTraceAccess, QueryTraceEvent,
    QueryTraceExecutorKind, QueryTracePhase,
};

/// Planner explain/fingerprint internals retained for compatibility.
#[doc(hidden)]
pub mod plan {
    pub use icydb_core::db::query::plan::{
        ExplainAccessPath, ExplainDeleteLimit, ExplainOrder, ExplainOrderBy, ExplainPagination,
        ExplainPlan, ExplainPredicate, OrderDirection, PlanError, PlanFingerprint,
    };
}
