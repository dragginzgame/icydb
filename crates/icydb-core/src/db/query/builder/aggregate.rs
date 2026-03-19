//! Module: query::builder::aggregate
//! Responsibility: composable grouped/global aggregate expression builders.
//! Does not own: aggregate validation policy or executor fold semantics.
//! Boundary: fluent aggregate intent construction lowered into grouped specs.

use crate::db::{direction::Direction, query::plan::AggregateKind};

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

    /// Return whether this expression kind is `COUNT`.
    #[must_use]
    pub(crate) const fn is_count_kind(kind: AggregateKind) -> bool {
        matches!(kind, AggregateKind::Count)
    }

    /// Return whether this expression kind is `SUM`.
    #[must_use]
    pub(crate) const fn is_sum_kind(kind: AggregateKind) -> bool {
        matches!(kind, AggregateKind::Sum | AggregateKind::Avg)
    }

    /// Return whether this expression kind supports explicit field targets.
    #[must_use]
    pub(crate) const fn supports_field_targets_kind(kind: AggregateKind) -> bool {
        matches!(
            kind,
            AggregateKind::Min | AggregateKind::Max | AggregateKind::Sum | AggregateKind::Avg
        )
    }

    /// Return whether this expression kind belongs to the extrema family.
    #[must_use]
    pub(crate) const fn is_extrema_kind(kind: AggregateKind) -> bool {
        Self::supports_field_targets_kind(kind)
    }

    /// Return whether reducer updates for this kind require a decoded id payload.
    #[must_use]
    pub(crate) const fn requires_decoded_id_kind(kind: AggregateKind) -> bool {
        !matches!(
            kind,
            AggregateKind::Count | AggregateKind::Sum | AggregateKind::Avg | AggregateKind::Exists
        )
    }

    /// Return whether grouped aggregate DISTINCT is supported for this kind.
    #[must_use]
    pub(crate) const fn supports_grouped_distinct_kind_v1(kind: AggregateKind) -> bool {
        matches!(
            kind,
            AggregateKind::Count
                | AggregateKind::Min
                | AggregateKind::Max
                | AggregateKind::Sum
                | AggregateKind::Avg
        )
    }

    /// Return whether global DISTINCT without GROUP BY keys is supported for this kind.
    #[must_use]
    pub(crate) const fn supports_global_distinct_without_group_keys_kind(
        kind: AggregateKind,
    ) -> bool {
        matches!(
            kind,
            AggregateKind::Count | AggregateKind::Sum | AggregateKind::Avg
        )
    }

    /// Return the canonical extrema traversal direction for this kind.
    #[must_use]
    pub(crate) const fn extrema_direction_for_kind(kind: AggregateKind) -> Option<Direction> {
        match kind {
            AggregateKind::Min => Some(Direction::Asc),
            AggregateKind::Max => Some(Direction::Desc),
            AggregateKind::Count
            | AggregateKind::Sum
            | AggregateKind::Avg
            | AggregateKind::Exists
            | AggregateKind::First
            | AggregateKind::Last => None,
        }
    }

    /// Return the canonical materialized fold direction for this kind.
    #[must_use]
    pub(crate) const fn materialized_fold_direction_for_kind(kind: AggregateKind) -> Direction {
        match kind {
            AggregateKind::Min => Direction::Desc,
            AggregateKind::Count
            | AggregateKind::Sum
            | AggregateKind::Avg
            | AggregateKind::Exists
            | AggregateKind::Max
            | AggregateKind::First
            | AggregateKind::Last => Direction::Asc,
        }
    }

    /// Return true when this kind can use bounded aggregate probe hints.
    #[must_use]
    pub(crate) const fn supports_bounded_probe_hint_for_kind(kind: AggregateKind) -> bool {
        !Self::is_count_kind(kind) && !Self::is_sum_kind(kind)
    }

    /// Derive a bounded aggregate probe fetch hint for this kind.
    #[must_use]
    pub(crate) fn bounded_probe_fetch_hint_for_kind(
        kind: AggregateKind,
        direction: Direction,
        offset: usize,
        page_limit: Option<usize>,
    ) -> Option<usize> {
        match kind {
            AggregateKind::Exists | AggregateKind::First => Some(offset.saturating_add(1)),
            AggregateKind::Min if direction == Direction::Asc => Some(offset.saturating_add(1)),
            AggregateKind::Max if direction == Direction::Desc => Some(offset.saturating_add(1)),
            AggregateKind::Last => page_limit.map(|limit| offset.saturating_add(limit)),
            AggregateKind::Count
            | AggregateKind::Sum
            | AggregateKind::Avg
            | AggregateKind::Min
            | AggregateKind::Max => None,
        }
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

/// Build one non-field-target terminal aggregate expression from one kind.
#[must_use]
pub(crate) fn terminal_expr_for_kind(kind: AggregateKind) -> AggregateExpr {
    match kind {
        AggregateKind::Count => count(),
        AggregateKind::Exists => exists(),
        AggregateKind::Min => min(),
        AggregateKind::Max => max(),
        AggregateKind::First => first(),
        AggregateKind::Last => last(),
        AggregateKind::Sum | AggregateKind::Avg => {
            unreachable!("terminal_expr_for_kind does not support SUM/AVG field-target kinds")
        }
    }
}

/// Build one field-target extrema aggregate expression from one kind.
#[must_use]
pub(crate) fn field_target_extrema_expr_for_kind(
    kind: AggregateKind,
    field: impl AsRef<str>,
) -> AggregateExpr {
    match kind {
        AggregateKind::Min => min_by(field),
        AggregateKind::Max => max_by(field),
        _ => {
            unreachable!(
                "field_target_extrema_expr_for_kind requires MIN/MAX kind for field-target extrema"
            )
        }
    }
}
