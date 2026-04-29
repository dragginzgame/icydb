use crate::db::{
    executor::ScalarTerminalBoundaryRequest,
    query::{builder::aggregate::AggregateExplain, plan::AggregateKind},
};

#[cfg(test)]
use crate::db::query::builder::aggregate::{AggregateExpr, count, exists};

///
/// ExistingRowsRequest
///
/// Stable existing-rows terminal executor request projection derived once at
/// the fluent aggregate entrypoint boundary.
/// This keeps count/exists request choice aligned with the aggregate expression
/// used for explain projection.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExistingRowsRequest {
    CountRows,
    ExistsRows,
}

impl ExistingRowsRequest {
    // Return the aggregate kind represented by this existing-rows request.
    #[expect(
        clippy::unnecessary_wraps,
        reason = "all request enums expose the same optional aggregate-kind shape"
    )]
    const fn aggregate_kind(self) -> Option<AggregateKind> {
        match self {
            Self::CountRows => Some(AggregateKind::Count),
            Self::ExistsRows => Some(AggregateKind::Exists),
        }
    }

    // Return the scalar executor request represented by this existing-rows
    // request.
    const fn boundary_request(self) -> ScalarTerminalBoundaryRequest {
        match self {
            Self::CountRows => ScalarTerminalBoundaryRequest::Count,
            Self::ExistsRows => ScalarTerminalBoundaryRequest::Exists,
        }
    }
}

///
/// ExistingRowsTerminalStrategy
///
/// ExistingRowsTerminalStrategy is the single fluent existing-rows behavior
/// source.
/// It resolves terminal request shape once and projects explain aggregate
/// metadata from that same strategy state on demand.
/// This keeps `count()` and `exists()` off the mixed id/extrema scalar
/// strategy without carrying owned explain-only aggregate expressions through
/// execution.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExistingRowsTerminalStrategy {
    request: ExistingRowsRequest,
}

impl ExistingRowsTerminalStrategy {
    /// Prepare one fluent `count(*)` terminal strategy.
    #[must_use]
    pub(crate) const fn count_rows() -> Self {
        Self {
            request: ExistingRowsRequest::CountRows,
        }
    }

    /// Prepare one fluent `exists()` terminal strategy.
    #[must_use]
    pub(crate) const fn exists_rows() -> Self {
        Self {
            request: ExistingRowsRequest::ExistsRows,
        }
    }

    /// Build the explain-visible aggregate expression projected by this strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn aggregate(&self) -> AggregateExpr {
        match self.request.aggregate_kind() {
            Some(AggregateKind::Count) => count(),
            Some(AggregateKind::Exists) => exists(),
            Some(
                AggregateKind::Sum
                | AggregateKind::Avg
                | AggregateKind::Min
                | AggregateKind::Max
                | AggregateKind::First
                | AggregateKind::Last,
            )
            | None => unreachable!(),
        }
    }

    /// Borrow the request projected by this existing-rows strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn request(&self) -> &ExistingRowsRequest {
        &self.request
    }

    /// Move the executor request and output shape out of this strategy.
    ///
    /// Fluent strategies own executor request construction so the
    /// session adapter does not duplicate strategy-to-boundary mapping.
    #[must_use]
    pub(in crate::db) const fn into_executor_request(
        self,
    ) -> (ScalarTerminalBoundaryRequest, ExistingRowsRequest) {
        (self.request.boundary_request(), self.request)
    }
}

impl AggregateExplain for ExistingRowsTerminalStrategy {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        self.request.aggregate_kind()
    }
}
