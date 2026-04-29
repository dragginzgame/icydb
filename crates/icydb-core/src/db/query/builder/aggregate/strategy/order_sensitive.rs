use crate::db::{
    executor::ScalarTerminalBoundaryRequest,
    query::{
        builder::aggregate::AggregateExplain,
        plan::{AggregateKind, FieldSlot},
    },
};

#[cfg(test)]
use crate::db::query::builder::aggregate::AggregateExpr;

///
/// OrderRequest
///
/// Stable order-sensitive executor request projection derived once at the
/// fluent aggregate entrypoint boundary.
/// This keeps response-order and field-order terminal request shape aligned
/// with the strategy state that fluent execution consumes and explain
/// projects on demand.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OrderRequest {
    ResponseOrder { kind: AggregateKind },
    NthBySlot { target_field: FieldSlot, nth: usize },
    MedianBySlot { target_field: FieldSlot },
    MinMaxBySlot { target_field: FieldSlot },
}

impl OrderRequest {
    // Return the aggregate kind represented by this order-sensitive request,
    // when the request maps onto one explain-visible aggregate terminal.
    const fn aggregate_kind(&self) -> Option<AggregateKind> {
        match self {
            Self::ResponseOrder { kind } => Some(*kind),
            Self::NthBySlot { .. } | Self::MedianBySlot { .. } | Self::MinMaxBySlot { .. } => None,
        }
    }
}

///
/// OrderSensitiveTerminalStrategy
///
/// OrderSensitiveTerminalStrategy is the single fluent order-sensitive
/// behavior source.
/// It resolves EXPLAIN-visible aggregate shape where applicable and the
/// terminal request once so `first/last/nth_by/median_by/min_max_by`
/// do not rebuild those decisions through parallel branch trees.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrderSensitiveTerminalStrategy {
    request: OrderRequest,
}

impl OrderSensitiveTerminalStrategy {
    /// Prepare one fluent `first()` terminal strategy.
    #[must_use]
    pub(crate) const fn first() -> Self {
        Self {
            request: OrderRequest::ResponseOrder {
                kind: AggregateKind::First,
            },
        }
    }

    /// Prepare one fluent `last()` terminal strategy.
    #[must_use]
    pub(crate) const fn last() -> Self {
        Self {
            request: OrderRequest::ResponseOrder {
                kind: AggregateKind::Last,
            },
        }
    }

    /// Prepare one fluent `nth_by(field, nth)` terminal strategy.
    #[must_use]
    pub(crate) const fn nth_by_slot(target_field: FieldSlot, nth: usize) -> Self {
        Self {
            request: OrderRequest::NthBySlot { target_field, nth },
        }
    }

    /// Prepare one fluent `median_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn median_by_slot(target_field: FieldSlot) -> Self {
        Self {
            request: OrderRequest::MedianBySlot { target_field },
        }
    }

    /// Prepare one fluent `min_max_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn min_max_by_slot(target_field: FieldSlot) -> Self {
        Self {
            request: OrderRequest::MinMaxBySlot { target_field },
        }
    }

    /// Build the explain-visible aggregate expression projected by this
    /// order-sensitive strategy when one exists.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn explain_aggregate(&self) -> Option<AggregateExpr> {
        self.request
            .aggregate_kind()
            .map(AggregateExpr::terminal_for_kind)
    }

    /// Borrow the request projected by this order-sensitive strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn request(&self) -> &OrderRequest {
        &self.request
    }

    /// Move the executor scalar terminal request and output family out of this strategy.
    #[must_use]
    pub(in crate::db) fn into_executor_request(self) -> (ScalarTerminalBoundaryRequest, bool) {
        match self.request {
            OrderRequest::ResponseOrder { kind } => {
                (ScalarTerminalBoundaryRequest::IdTerminal { kind }, false)
            }
            OrderRequest::NthBySlot { target_field, nth } => (
                ScalarTerminalBoundaryRequest::NthBySlot { target_field, nth },
                false,
            ),
            OrderRequest::MedianBySlot { target_field } => (
                ScalarTerminalBoundaryRequest::MedianBySlot { target_field },
                false,
            ),
            OrderRequest::MinMaxBySlot { target_field } => (
                ScalarTerminalBoundaryRequest::MinMaxBySlot { target_field },
                true,
            ),
        }
    }
}

impl AggregateExplain for OrderSensitiveTerminalStrategy {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        self.request.aggregate_kind()
    }
}
