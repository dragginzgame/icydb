use crate::db::{
    executor::ScalarNumericFieldBoundaryRequest,
    query::{
        builder::aggregate::AggregateExplain,
        plan::{AggregateKind, FieldSlot},
    },
};

#[cfg(test)]
use crate::db::query::builder::aggregate::{AggregateExpr, avg, sum};

///
/// NumericFieldRequest
///
/// Stable numeric-field executor request projection derived once at the
/// fluent aggregate entrypoint boundary.
/// This keeps numeric boundary selection aligned with the same strategy
/// metadata that request and explain projections share.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NumericFieldRequest {
    Sum,
    SumDistinct,
    Avg,
    AvgDistinct,
}

impl NumericFieldRequest {
    // Return the aggregate kind represented by this numeric-field request.
    #[expect(
        clippy::unnecessary_wraps,
        reason = "all request enums expose the same optional aggregate-kind shape"
    )]
    const fn aggregate_kind(self) -> Option<AggregateKind> {
        match self {
            Self::Sum | Self::SumDistinct => Some(AggregateKind::Sum),
            Self::Avg | Self::AvgDistinct => Some(AggregateKind::Avg),
        }
    }

    // Return whether this numeric-field request carries DISTINCT.
    const fn is_distinct(self) -> bool {
        matches!(self, Self::SumDistinct | Self::AvgDistinct)
    }

    // Return the scalar numeric executor request represented by this
    // numeric-field request.
    fn boundary_request(self) -> ScalarNumericFieldBoundaryRequest {
        match (self.aggregate_kind(), self.is_distinct()) {
            (Some(AggregateKind::Sum), false) => ScalarNumericFieldBoundaryRequest::Sum,
            (Some(AggregateKind::Sum), true) => ScalarNumericFieldBoundaryRequest::SumDistinct,
            (Some(AggregateKind::Avg), false) => ScalarNumericFieldBoundaryRequest::Avg,
            (Some(AggregateKind::Avg), true) => ScalarNumericFieldBoundaryRequest::AvgDistinct,
            (
                Some(
                    AggregateKind::Count
                    | AggregateKind::Exists
                    | AggregateKind::Min
                    | AggregateKind::Max
                    | AggregateKind::First
                    | AggregateKind::Last,
                )
                | None,
                _,
            ) => unreachable!("numeric field requests only project SUM/AVG"),
        }
    }
}

///
/// NumericFieldStrategy
///
/// NumericFieldStrategy is the single fluent numeric-field behavior source.
/// It resolves target-slot ownership and boundary request once so `SUM/AVG`
/// callers do not rebuild those decisions through parallel branch trees.
/// Explain-visible aggregate shape is projected on demand from that strategy
/// state instead of being carried as owned execution metadata.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NumericFieldStrategy {
    target_field: FieldSlot,
    request: NumericFieldRequest,
}

impl NumericFieldStrategy {
    /// Prepare one fluent `sum(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn sum_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            request: NumericFieldRequest::Sum,
        }
    }

    /// Prepare one fluent `sum(distinct field)` terminal strategy.
    #[must_use]
    pub(crate) const fn sum_distinct_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            request: NumericFieldRequest::SumDistinct,
        }
    }

    /// Prepare one fluent `avg(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn avg_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            request: NumericFieldRequest::Avg,
        }
    }

    /// Prepare one fluent `avg(distinct field)` terminal strategy.
    #[must_use]
    pub(crate) const fn avg_distinct_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            request: NumericFieldRequest::AvgDistinct,
        }
    }

    /// Build the explain-visible aggregate expression projected by this strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn aggregate(&self) -> AggregateExpr {
        let field = self.target_field.field();
        let aggregate = match self.request.aggregate_kind() {
            Some(AggregateKind::Sum) => sum(field),
            Some(AggregateKind::Avg) => avg(field),
            Some(
                AggregateKind::Count
                | AggregateKind::Exists
                | AggregateKind::Min
                | AggregateKind::Max
                | AggregateKind::First
                | AggregateKind::Last,
            )
            | None => {
                unreachable!("numeric field strategy only projects SUM/AVG aggregate expressions")
            }
        };

        if self.request.is_distinct() {
            aggregate.distinct()
        } else {
            aggregate
        }
    }

    /// Return the aggregate kind projected by this numeric-field strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn aggregate_kind(&self) -> AggregateKind {
        match self.request.aggregate_kind() {
            Some(kind) => kind,
            None => unreachable!(),
        }
    }

    /// Borrow the projected field label for this numeric-field strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn projected_field(&self) -> &str {
        self.target_field.field()
    }

    /// Borrow the resolved planner target slot owned by this numeric-field
    /// strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn target_field(&self) -> &FieldSlot {
        &self.target_field
    }

    /// Return the request projected by this numeric-field strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn request(&self) -> NumericFieldRequest {
        self.request
    }

    /// Move the executor numeric-field request out of this strategy.
    #[must_use]
    pub(in crate::db) fn into_executor_request(
        self,
    ) -> (FieldSlot, ScalarNumericFieldBoundaryRequest) {
        (self.target_field, self.request.boundary_request())
    }
}

impl AggregateExplain for NumericFieldStrategy {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        self.request.aggregate_kind()
    }

    fn explain_projected_field(&self) -> Option<&str> {
        Some(self.target_field.field())
    }
}
