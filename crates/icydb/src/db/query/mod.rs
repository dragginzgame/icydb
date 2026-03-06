pub mod expr;

pub use expr::{FilterExpr, OrderDirection, SortExpr};
pub use icydb_core::db::{
    AggregateExpr, FieldRef, MissingRowPolicy, Predicate, Query, QueryTracePlan,
    TraceExecutionStrategy, count, count_by, exists, first, last, max, max_by, min, min_by, sum,
};

/// Field-reference helpers exposed by the facade query API.
pub mod builder {
    pub use icydb_core::db::{
        AggregateExpr, FieldRef, count, count_by, exists, first, last, max, max_by, min, min_by,
        sum,
    };
}

/// Predicate type exposed at the facade query boundary.
pub mod predicate {
    pub use icydb_core::db::Predicate;
}
