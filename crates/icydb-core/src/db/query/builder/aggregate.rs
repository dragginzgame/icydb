//! Module: query::builder::aggregate
//! Responsibility: composable grouped/global aggregate expression builders.
//! Does not own: aggregate validation policy or executor fold semantics.
//! Boundary: fluent aggregate intent construction lowered into grouped specs.

use crate::db::query::plan::{AggregateKind, FieldSlot};

///
/// AggregateExpr
///
/// Composable aggregate expression used by query/fluent aggregate entrypoints.
/// This builder only carries declarative shape (`kind`, `target_field`,
/// `distinct`) and does not perform semantic validation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateExpr {
    kind: AggregateKind,
    target_field: Option<String>,
    distinct: bool,
}

impl AggregateExpr {
    /// Construct one aggregate expression from explicit shape components.
    const fn new(kind: AggregateKind, target_field: Option<String>) -> Self {
        Self {
            kind,
            target_field,
            distinct: false,
        }
    }

    /// Enable DISTINCT modifier for this aggregate expression.
    #[must_use]
    pub const fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Borrow aggregate kind.
    #[must_use]
    pub(crate) const fn kind(&self) -> AggregateKind {
        self.kind
    }

    /// Borrow optional target field.
    #[must_use]
    pub(crate) fn target_field(&self) -> Option<&str> {
        self.target_field.as_deref()
    }

    /// Return true when DISTINCT is enabled.
    #[must_use]
    pub(crate) const fn is_distinct(&self) -> bool {
        self.distinct
    }

    /// Build one aggregate expression directly from planner semantic parts.
    pub(in crate::db::query) const fn from_semantic_parts(
        kind: AggregateKind,
        target_field: Option<String>,
        distinct: bool,
    ) -> Self {
        Self {
            kind,
            target_field,
            distinct,
        }
    }

    /// Build one non-field-target terminal aggregate expression from one kind.
    #[must_use]
    pub(in crate::db) fn terminal_for_kind(kind: AggregateKind) -> Self {
        match kind {
            AggregateKind::Count => count(),
            AggregateKind::Exists => exists(),
            AggregateKind::Min => min(),
            AggregateKind::Max => max(),
            AggregateKind::First => first(),
            AggregateKind::Last => last(),
            AggregateKind::Sum | AggregateKind::Avg => unreachable!(
                "AggregateExpr::terminal_for_kind does not support SUM/AVG field-target kinds"
            ),
        }
    }

    /// Build one field-target extrema aggregate expression from one kind.
    #[must_use]
    pub(in crate::db) fn field_target_extrema_for_kind(
        kind: AggregateKind,
        field: impl AsRef<str>,
    ) -> Self {
        match kind {
            AggregateKind::Min => min_by(field),
            AggregateKind::Max => max_by(field),
            _ => unreachable!("AggregateExpr::field_target_extrema_for_kind requires MIN/MAX kind"),
        }
    }
}

/// PreparedFluentScalarTerminalRuntimeRequest
///
/// Stable fluent scalar terminal runtime request projection derived once at the
/// fluent aggregate entrypoint boundary.
/// This keeps execution-side request choice aligned with the aggregate
/// expression used for explain/descriptor projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PreparedFluentScalarTerminalRuntimeRequest {
    CountRows,
    ExistsRows,
    IdTerminal {
        kind: AggregateKind,
    },
    IdBySlot {
        kind: AggregateKind,
        target_field: FieldSlot,
    },
}

///
/// PreparedFluentScalarTerminalStrategy
///
/// PreparedFluentScalarTerminalStrategy is the single fluent scalar terminal
/// behavior source for the first non-SQL `0.71` slice.
/// It resolves aggregate expression and runtime terminal request once so
/// fluent execution and fluent EXPLAIN do not rebuild those decisions through
/// parallel branch trees.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedFluentScalarTerminalStrategy {
    aggregate: AggregateExpr,
    runtime_request: PreparedFluentScalarTerminalRuntimeRequest,
}

impl PreparedFluentScalarTerminalStrategy {
    /// Prepare one fluent `count(*)` terminal strategy.
    #[must_use]
    pub(crate) const fn count_rows() -> Self {
        Self {
            aggregate: count(),
            runtime_request: PreparedFluentScalarTerminalRuntimeRequest::CountRows,
        }
    }

    /// Prepare one fluent `exists()` terminal strategy.
    #[must_use]
    pub(crate) const fn exists_rows() -> Self {
        Self {
            aggregate: exists(),
            runtime_request: PreparedFluentScalarTerminalRuntimeRequest::ExistsRows,
        }
    }

    /// Prepare one fluent id-returning scalar terminal without a field target.
    #[must_use]
    pub(crate) fn id_terminal(kind: AggregateKind) -> Self {
        Self {
            aggregate: AggregateExpr::terminal_for_kind(kind),
            runtime_request: PreparedFluentScalarTerminalRuntimeRequest::IdTerminal { kind },
        }
    }

    /// Prepare one fluent field-targeted extrema terminal with a resolved
    /// planner slot.
    #[must_use]
    pub(crate) fn id_by_slot(kind: AggregateKind, target_field: FieldSlot) -> Self {
        Self {
            aggregate: AggregateExpr::field_target_extrema_for_kind(kind, target_field.field()),
            runtime_request: PreparedFluentScalarTerminalRuntimeRequest::IdBySlot {
                kind,
                target_field,
            },
        }
    }

    /// Borrow the aggregate expression projected by this prepared fluent
    /// scalar terminal strategy.
    #[must_use]
    pub(crate) const fn aggregate(&self) -> &AggregateExpr {
        &self.aggregate
    }

    /// Borrow the prepared runtime request projected by this fluent scalar
    /// terminal strategy.
    #[must_use]
    pub(crate) const fn runtime_request(&self) -> &PreparedFluentScalarTerminalRuntimeRequest {
        &self.runtime_request
    }
}

///
/// PreparedFluentNumericFieldRuntimeRequest
///
/// Stable fluent numeric-field runtime request projection derived once at the
/// fluent aggregate entrypoint boundary.
/// This keeps numeric boundary selection aligned with the aggregate expression
/// used by runtime and explain projections.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedFluentNumericFieldRuntimeRequest {
    Sum,
    SumDistinct,
    Avg,
    AvgDistinct,
}

///
/// PreparedFluentNumericFieldStrategy
///
/// PreparedFluentNumericFieldStrategy is the single fluent numeric-field
/// behavior source for the next `0.71` slice.
/// It resolves aggregate expression, target-slot ownership, and runtime
/// boundary request once so `SUM/AVG` callers do not rebuild those decisions
/// through parallel branch trees.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedFluentNumericFieldStrategy {
    aggregate: AggregateExpr,
    target_field: FieldSlot,
    runtime_request: PreparedFluentNumericFieldRuntimeRequest,
}

impl PreparedFluentNumericFieldStrategy {
    /// Prepare one fluent `sum(field)` terminal strategy.
    #[must_use]
    pub(crate) fn sum_by_slot(target_field: FieldSlot) -> Self {
        Self {
            aggregate: sum(target_field.field()),
            target_field,
            runtime_request: PreparedFluentNumericFieldRuntimeRequest::Sum,
        }
    }

    /// Prepare one fluent `sum(distinct field)` terminal strategy.
    #[must_use]
    pub(crate) fn sum_distinct_by_slot(target_field: FieldSlot) -> Self {
        Self {
            aggregate: sum(target_field.field()).distinct(),
            target_field,
            runtime_request: PreparedFluentNumericFieldRuntimeRequest::SumDistinct,
        }
    }

    /// Prepare one fluent `avg(field)` terminal strategy.
    #[must_use]
    pub(crate) fn avg_by_slot(target_field: FieldSlot) -> Self {
        Self {
            aggregate: avg(target_field.field()),
            target_field,
            runtime_request: PreparedFluentNumericFieldRuntimeRequest::Avg,
        }
    }

    /// Prepare one fluent `avg(distinct field)` terminal strategy.
    #[must_use]
    pub(crate) fn avg_distinct_by_slot(target_field: FieldSlot) -> Self {
        Self {
            aggregate: avg(target_field.field()).distinct(),
            target_field,
            runtime_request: PreparedFluentNumericFieldRuntimeRequest::AvgDistinct,
        }
    }

    /// Borrow the aggregate expression projected by this prepared fluent
    /// numeric strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn aggregate(&self) -> &AggregateExpr {
        &self.aggregate
    }

    /// Return the aggregate kind projected by this prepared fluent numeric
    /// strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn aggregate_kind(&self) -> AggregateKind {
        self.aggregate.kind()
    }

    /// Borrow the projected field label for this prepared fluent numeric
    /// strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn projected_field(&self) -> Option<&str> {
        self.aggregate.target_field()
    }

    /// Borrow the resolved planner target slot owned by this prepared fluent
    /// numeric strategy.
    #[must_use]
    pub(crate) const fn target_field(&self) -> &FieldSlot {
        &self.target_field
    }

    /// Return the prepared runtime request projected by this fluent numeric
    /// strategy.
    #[must_use]
    pub(crate) const fn runtime_request(&self) -> PreparedFluentNumericFieldRuntimeRequest {
        self.runtime_request
    }
}

///
/// PreparedFluentOrderSensitiveTerminalRuntimeRequest
///
/// Stable fluent order-sensitive runtime request projection derived once at
/// the fluent aggregate entrypoint boundary.
/// This keeps response-order and field-order terminal request shape aligned
/// with the prepared strategy that fluent execution consumes.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PreparedFluentOrderSensitiveTerminalRuntimeRequest {
    ResponseOrder { kind: AggregateKind },
    NthBySlot { target_field: FieldSlot, nth: usize },
    MedianBySlot { target_field: FieldSlot },
    MinMaxBySlot { target_field: FieldSlot },
}

///
/// PreparedFluentOrderSensitiveTerminalStrategy
///
/// PreparedFluentOrderSensitiveTerminalStrategy is the single fluent
/// order-sensitive behavior source for the next `0.71` slice.
/// It resolves EXPLAIN-visible aggregate shape where applicable and the
/// runtime terminal request once so `first/last/nth_by/median_by/min_max_by`
/// do not rebuild those decisions through parallel branch trees.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedFluentOrderSensitiveTerminalStrategy {
    explain_aggregate: Option<AggregateExpr>,
    runtime_request: PreparedFluentOrderSensitiveTerminalRuntimeRequest,
}

impl PreparedFluentOrderSensitiveTerminalStrategy {
    /// Prepare one fluent `first()` terminal strategy.
    #[must_use]
    pub(crate) const fn first() -> Self {
        Self {
            explain_aggregate: Some(first()),
            runtime_request: PreparedFluentOrderSensitiveTerminalRuntimeRequest::ResponseOrder {
                kind: AggregateKind::First,
            },
        }
    }

    /// Prepare one fluent `last()` terminal strategy.
    #[must_use]
    pub(crate) const fn last() -> Self {
        Self {
            explain_aggregate: Some(last()),
            runtime_request: PreparedFluentOrderSensitiveTerminalRuntimeRequest::ResponseOrder {
                kind: AggregateKind::Last,
            },
        }
    }

    /// Prepare one fluent `nth_by(field, nth)` terminal strategy.
    #[must_use]
    pub(crate) const fn nth_by_slot(target_field: FieldSlot, nth: usize) -> Self {
        Self {
            explain_aggregate: None,
            runtime_request: PreparedFluentOrderSensitiveTerminalRuntimeRequest::NthBySlot {
                target_field,
                nth,
            },
        }
    }

    /// Prepare one fluent `median_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn median_by_slot(target_field: FieldSlot) -> Self {
        Self {
            explain_aggregate: None,
            runtime_request: PreparedFluentOrderSensitiveTerminalRuntimeRequest::MedianBySlot {
                target_field,
            },
        }
    }

    /// Prepare one fluent `min_max_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn min_max_by_slot(target_field: FieldSlot) -> Self {
        Self {
            explain_aggregate: None,
            runtime_request: PreparedFluentOrderSensitiveTerminalRuntimeRequest::MinMaxBySlot {
                target_field,
            },
        }
    }

    /// Borrow the aggregate expression projected by this prepared
    /// order-sensitive strategy when an EXPLAIN-visible aggregate kind exists.
    #[must_use]
    pub(crate) const fn explain_aggregate(&self) -> Option<&AggregateExpr> {
        self.explain_aggregate.as_ref()
    }

    /// Borrow the prepared runtime request projected by this fluent
    /// order-sensitive strategy.
    #[must_use]
    pub(crate) const fn runtime_request(
        &self,
    ) -> &PreparedFluentOrderSensitiveTerminalRuntimeRequest {
        &self.runtime_request
    }
}

///
/// PreparedFluentProjectionRuntimeRequest
///
/// Stable fluent projection/distinct runtime request projection derived once
/// at the fluent aggregate entrypoint boundary.
/// This keeps field-target projection terminal request shape aligned with the
/// prepared strategy that fluent execution consumes.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedFluentProjectionRuntimeRequest {
    Values,
    DistinctValues,
    CountDistinct,
    ValuesWithIds,
    TerminalValue { terminal_kind: AggregateKind },
}

///
/// PreparedFluentProjectionStrategy
///
/// PreparedFluentProjectionStrategy is the single fluent projection/distinct
/// behavior source for the next `0.71` slice.
/// It resolves target-slot ownership plus runtime request shape once so
/// `values_by`/`distinct_values_by`/`count_distinct_by`/`values_by_with_ids`/
/// `first_value_by`/`last_value_by` do not rebuild those decisions inline.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedFluentProjectionStrategy {
    target_field: FieldSlot,
    runtime_request: PreparedFluentProjectionRuntimeRequest,
}

impl PreparedFluentProjectionStrategy {
    /// Prepare one fluent `values_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn values_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            runtime_request: PreparedFluentProjectionRuntimeRequest::Values,
        }
    }

    /// Prepare one fluent `distinct_values_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn distinct_values_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            runtime_request: PreparedFluentProjectionRuntimeRequest::DistinctValues,
        }
    }

    /// Prepare one fluent `count_distinct_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn count_distinct_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            runtime_request: PreparedFluentProjectionRuntimeRequest::CountDistinct,
        }
    }

    /// Prepare one fluent `values_by_with_ids(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn values_by_with_ids_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            runtime_request: PreparedFluentProjectionRuntimeRequest::ValuesWithIds,
        }
    }

    /// Prepare one fluent `first_value_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn first_value_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            runtime_request: PreparedFluentProjectionRuntimeRequest::TerminalValue {
                terminal_kind: AggregateKind::First,
            },
        }
    }

    /// Prepare one fluent `last_value_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn last_value_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            runtime_request: PreparedFluentProjectionRuntimeRequest::TerminalValue {
                terminal_kind: AggregateKind::Last,
            },
        }
    }

    /// Borrow the resolved planner target slot owned by this prepared fluent
    /// projection strategy.
    #[must_use]
    pub(crate) const fn target_field(&self) -> &FieldSlot {
        &self.target_field
    }

    /// Return the prepared runtime request projected by this fluent
    /// projection strategy.
    #[must_use]
    pub(crate) const fn runtime_request(&self) -> PreparedFluentProjectionRuntimeRequest {
        self.runtime_request
    }
}

/// Build `count(*)`.
#[must_use]
pub const fn count() -> AggregateExpr {
    AggregateExpr::new(AggregateKind::Count, None)
}

/// Build `count(field)`.
#[must_use]
pub fn count_by(field: impl AsRef<str>) -> AggregateExpr {
    AggregateExpr::new(AggregateKind::Count, Some(field.as_ref().to_string()))
}

/// Build `sum(field)`.
#[must_use]
pub fn sum(field: impl AsRef<str>) -> AggregateExpr {
    AggregateExpr::new(AggregateKind::Sum, Some(field.as_ref().to_string()))
}

/// Build `avg(field)`.
#[must_use]
pub fn avg(field: impl AsRef<str>) -> AggregateExpr {
    AggregateExpr::new(AggregateKind::Avg, Some(field.as_ref().to_string()))
}

/// Build `exists`.
#[must_use]
pub const fn exists() -> AggregateExpr {
    AggregateExpr::new(AggregateKind::Exists, None)
}

/// Build `first`.
#[must_use]
pub const fn first() -> AggregateExpr {
    AggregateExpr::new(AggregateKind::First, None)
}

/// Build `last`.
#[must_use]
pub const fn last() -> AggregateExpr {
    AggregateExpr::new(AggregateKind::Last, None)
}

/// Build `min`.
#[must_use]
pub const fn min() -> AggregateExpr {
    AggregateExpr::new(AggregateKind::Min, None)
}

/// Build `min(field)`.
#[must_use]
pub fn min_by(field: impl AsRef<str>) -> AggregateExpr {
    AggregateExpr::new(AggregateKind::Min, Some(field.as_ref().to_string()))
}

/// Build `max`.
#[must_use]
pub const fn max() -> AggregateExpr {
    AggregateExpr::new(AggregateKind::Max, None)
}

/// Build `max(field)`.
#[must_use]
pub fn max_by(field: impl AsRef<str>) -> AggregateExpr {
    AggregateExpr::new(AggregateKind::Max, Some(field.as_ref().to_string()))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::query::{
        builder::{
            PreparedFluentNumericFieldRuntimeRequest, PreparedFluentNumericFieldStrategy,
            PreparedFluentOrderSensitiveTerminalRuntimeRequest,
            PreparedFluentOrderSensitiveTerminalStrategy, PreparedFluentProjectionRuntimeRequest,
            PreparedFluentProjectionStrategy,
        },
        plan::{AggregateKind, FieldSlot},
    };

    #[test]
    fn prepared_fluent_numeric_field_strategy_sum_distinct_preserves_runtime_shape() {
        let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
        let strategy = PreparedFluentNumericFieldStrategy::sum_distinct_by_slot(rank_slot.clone());

        assert_eq!(
            strategy.aggregate_kind(),
            AggregateKind::Sum,
            "sum(distinct field) should preserve SUM aggregate kind",
        );
        assert_eq!(
            strategy.projected_field(),
            Some("rank"),
            "sum(distinct field) should preserve projected field labels",
        );
        assert!(
            strategy.aggregate().is_distinct(),
            "sum(distinct field) should preserve DISTINCT aggregate shape",
        );
        assert_eq!(
            strategy.target_field(),
            &rank_slot,
            "sum(distinct field) should preserve the resolved planner field slot",
        );
        assert_eq!(
            strategy.runtime_request(),
            PreparedFluentNumericFieldRuntimeRequest::SumDistinct,
            "sum(distinct field) should project the numeric DISTINCT runtime request",
        );
    }

    #[test]
    fn prepared_fluent_numeric_field_strategy_avg_preserves_runtime_shape() {
        let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
        let strategy = PreparedFluentNumericFieldStrategy::avg_by_slot(rank_slot.clone());

        assert_eq!(
            strategy.aggregate_kind(),
            AggregateKind::Avg,
            "avg(field) should preserve AVG aggregate kind",
        );
        assert_eq!(
            strategy.projected_field(),
            Some("rank"),
            "avg(field) should preserve projected field labels",
        );
        assert!(
            !strategy.aggregate().is_distinct(),
            "avg(field) should stay non-distinct unless requested explicitly",
        );
        assert_eq!(
            strategy.target_field(),
            &rank_slot,
            "avg(field) should preserve the resolved planner field slot",
        );
        assert_eq!(
            strategy.runtime_request(),
            PreparedFluentNumericFieldRuntimeRequest::Avg,
            "avg(field) should project the numeric AVG runtime request",
        );
    }

    #[test]
    fn prepared_fluent_order_sensitive_strategy_first_preserves_explain_and_runtime_shape() {
        let strategy = PreparedFluentOrderSensitiveTerminalStrategy::first();

        assert_eq!(
            strategy.explain_aggregate().map(super::AggregateExpr::kind),
            Some(AggregateKind::First),
            "first() should preserve the explain-visible aggregate kind",
        );
        assert_eq!(
            strategy.runtime_request(),
            &PreparedFluentOrderSensitiveTerminalRuntimeRequest::ResponseOrder {
                kind: AggregateKind::First,
            },
            "first() should project the response-order runtime request",
        );
    }

    #[test]
    fn prepared_fluent_order_sensitive_strategy_nth_preserves_field_order_runtime_shape() {
        let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
        let strategy =
            PreparedFluentOrderSensitiveTerminalStrategy::nth_by_slot(rank_slot.clone(), 2);

        assert_eq!(
            strategy.explain_aggregate(),
            None,
            "nth_by(field, nth) should stay off the current explain aggregate surface",
        );
        assert_eq!(
            strategy.runtime_request(),
            &PreparedFluentOrderSensitiveTerminalRuntimeRequest::NthBySlot {
                target_field: rank_slot,
                nth: 2,
            },
            "nth_by(field, nth) should preserve the resolved field-order runtime request",
        );
    }

    #[test]
    fn prepared_fluent_projection_strategy_count_distinct_preserves_runtime_shape() {
        let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
        let strategy = PreparedFluentProjectionStrategy::count_distinct_by_slot(rank_slot.clone());

        assert_eq!(
            strategy.target_field(),
            &rank_slot,
            "count_distinct_by(field) should preserve the resolved planner field slot",
        );
        assert_eq!(
            strategy.runtime_request(),
            PreparedFluentProjectionRuntimeRequest::CountDistinct,
            "count_distinct_by(field) should project the distinct-count runtime request",
        );
    }

    #[test]
    fn prepared_fluent_projection_strategy_terminal_value_preserves_runtime_shape() {
        let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
        let strategy = PreparedFluentProjectionStrategy::last_value_by_slot(rank_slot.clone());

        assert_eq!(
            strategy.target_field(),
            &rank_slot,
            "last_value_by(field) should preserve the resolved planner field slot",
        );
        assert_eq!(
            strategy.runtime_request(),
            PreparedFluentProjectionRuntimeRequest::TerminalValue {
                terminal_kind: AggregateKind::Last,
            },
            "last_value_by(field) should project the terminal-value runtime request",
        );
    }
}
