//! Module: query::plan::semantics::identity
//! Responsibility: aggregate identity rules shared by global and grouped paths.
//! Does not own: aggregate execution, grouping keys, or runtime reducer state.
//! Boundary: normalizes aggregate function, input, and observable DISTINCT meaning.

use crate::db::query::{
    builder::AggregateExpr,
    plan::{AggregateKind, expr::Expr},
};

///
/// AggregateIdentity
///
/// AggregateIdentity is the canonical identity of one aggregate terminal.
/// It intentionally excludes grouping keys, runtime state, and null-handling
/// rules so planner, SQL lowering, hashing, and grouped projection dedup share
/// one meaning-level authority without importing executor policy.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AggregateIdentity {
    Count {
        input_expr: Option<Expr>,
        distinct: bool,
    },
    Sum {
        input_expr: Option<Expr>,
        distinct: bool,
    },
    Avg {
        input_expr: Option<Expr>,
        distinct: bool,
    },
    Min {
        input_expr: Option<Expr>,
    },
    Max {
        input_expr: Option<Expr>,
    },
    Exists {
        input_expr: Option<Expr>,
        distinct: bool,
    },
    First {
        input_expr: Option<Expr>,
        distinct: bool,
    },
    Last {
        input_expr: Option<Expr>,
        distinct: bool,
    },
}

impl AggregateIdentity {
    /// Build aggregate identity from raw planner aggregate parts.
    #[must_use]
    pub(crate) const fn from_parts(
        kind: AggregateKind,
        input_expr: Option<Expr>,
        distinct: bool,
    ) -> Self {
        match kind {
            AggregateKind::Count => Self::Count {
                input_expr,
                distinct,
            },
            AggregateKind::Sum => Self::Sum {
                input_expr,
                distinct,
            },
            AggregateKind::Avg => Self::Avg {
                input_expr,
                distinct,
            },
            AggregateKind::Min => Self::Min { input_expr },
            AggregateKind::Max => Self::Max { input_expr },
            AggregateKind::Exists => Self::Exists {
                input_expr,
                distinct,
            },
            AggregateKind::First => Self::First {
                input_expr,
                distinct,
            },
            AggregateKind::Last => Self::Last {
                input_expr,
                distinct,
            },
        }
    }

    /// Build aggregate identity from one raw aggregate expression.
    #[must_use]
    pub(crate) fn from_aggregate_expr(aggregate: &AggregateExpr) -> Self {
        Self::from_parts(
            aggregate.kind(),
            aggregate.input_expr().cloned(),
            aggregate.is_distinct(),
        )
    }

    /// Return whether raw DISTINCT is observable in aggregate identity.
    #[must_use]
    pub(crate) const fn normalize_distinct_for_kind(kind: AggregateKind, distinct: bool) -> bool {
        match kind {
            AggregateKind::Min | AggregateKind::Max => false,
            AggregateKind::Count
            | AggregateKind::Sum
            | AggregateKind::Avg
            | AggregateKind::Exists
            | AggregateKind::First
            | AggregateKind::Last => distinct,
        }
    }

    /// Return the aggregate kind represented by this identity.
    #[must_use]
    pub(crate) const fn kind(&self) -> AggregateKind {
        match self {
            Self::Count { .. } => AggregateKind::Count,
            Self::Sum { .. } => AggregateKind::Sum,
            Self::Avg { .. } => AggregateKind::Avg,
            Self::Min { .. } => AggregateKind::Min,
            Self::Max { .. } => AggregateKind::Max,
            Self::Exists { .. } => AggregateKind::Exists,
            Self::First { .. } => AggregateKind::First,
            Self::Last { .. } => AggregateKind::Last,
        }
    }

    /// Borrow the identity aggregate input expression, if any.
    #[must_use]
    pub(crate) const fn input_expr(&self) -> Option<&Expr> {
        match self {
            Self::Count { input_expr, .. }
            | Self::Sum { input_expr, .. }
            | Self::Avg { input_expr, .. }
            | Self::Min { input_expr }
            | Self::Max { input_expr }
            | Self::Exists { input_expr, .. }
            | Self::First { input_expr, .. }
            | Self::Last { input_expr, .. } => input_expr.as_ref(),
        }
    }

    /// Move the identity aggregate input expression out of this identity.
    #[must_use]
    pub(crate) fn into_input_expr(self) -> Option<Expr> {
        match self {
            Self::Count { input_expr, .. }
            | Self::Sum { input_expr, .. }
            | Self::Avg { input_expr, .. }
            | Self::Min { input_expr }
            | Self::Max { input_expr }
            | Self::Exists { input_expr, .. }
            | Self::First { input_expr, .. }
            | Self::Last { input_expr, .. } => input_expr,
        }
    }

    /// Return whether DISTINCT changes observable aggregate behavior.
    #[must_use]
    pub(crate) const fn distinct(&self) -> bool {
        match self {
            Self::Count { distinct, .. }
            | Self::Sum { distinct, .. }
            | Self::Avg { distinct, .. }
            | Self::Exists { distinct, .. }
            | Self::First { distinct, .. }
            | Self::Last { distinct, .. } => *distinct,
            Self::Min { .. } | Self::Max { .. } => false,
        }
    }

    /// Borrow the direct field input label when this aggregate is field-backed.
    #[must_use]
    pub(crate) const fn target_field(&self) -> Option<&str> {
        let Some(Expr::Field(field)) = self.input_expr() else {
            return None;
        };

        Some(field.as_str())
    }

    /// Return whether this aggregate is the optimized `COUNT(*)` identity shape.
    #[must_use]
    pub(crate) const fn is_count_rows_only(&self) -> bool {
        matches!(
            self,
            Self::Count {
                input_expr: None,
                distinct: false
            }
        )
    }

    /// Return whether grouped DISTINCT needs per-value deduplication.
    #[must_use]
    pub(crate) const fn uses_grouped_distinct_value_dedup(&self) -> bool {
        matches!(
            self,
            Self::Count { distinct: true, .. }
                | Self::Sum { distinct: true, .. }
                | Self::Avg { distinct: true, .. }
        )
    }
}
