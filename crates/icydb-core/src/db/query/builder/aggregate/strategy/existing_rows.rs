use crate::db::{
    executor::ScalarTerminalBoundaryRequest,
    query::{builder::aggregate::AggregateExplain, plan::AggregateKind},
};

#[cfg(test)]
use crate::db::query::builder::aggregate::{AggregateExpr, count, exists};

///
/// CountRowsTerminal
///
/// Concrete fluent `count()` terminal descriptor.
/// The descriptor is zero-sized because count has exactly one executor request
/// and one output shape once the fluent method has selected it.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct CountRowsTerminal;

impl CountRowsTerminal {
    /// Prepare one fluent `count(*)` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new() -> Self {
        Self
    }

    /// Build the explain-visible aggregate expression projected by this descriptor.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::query) const fn aggregate() -> AggregateExpr {
        count()
    }

    /// Move the executor request out of this descriptor.
    #[must_use]
    pub(in crate::db) const fn into_executor_request(self) -> ScalarTerminalBoundaryRequest {
        let _ = self;

        ScalarTerminalBoundaryRequest::Count
    }
}

impl AggregateExplain for CountRowsTerminal {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        Some(AggregateKind::Count)
    }
}

///
/// ExistsRowsTerminal
///
/// Concrete fluent `exists()` terminal descriptor.
/// The descriptor is zero-sized because existence checks always map to one
/// executor request and one boolean output.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExistsRowsTerminal;

impl ExistsRowsTerminal {
    /// Prepare one fluent `exists()` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new() -> Self {
        Self
    }

    /// Build the explain-visible aggregate expression projected by this descriptor.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::query) const fn aggregate() -> AggregateExpr {
        exists()
    }

    /// Move the executor request out of this descriptor.
    #[must_use]
    pub(in crate::db) const fn into_executor_request(self) -> ScalarTerminalBoundaryRequest {
        let _ = self;

        ScalarTerminalBoundaryRequest::Exists
    }
}

impl AggregateExplain for ExistsRowsTerminal {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        Some(AggregateKind::Exists)
    }
}
