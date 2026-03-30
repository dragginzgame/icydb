//! Module: query::builder::aggregate
//! Responsibility: composable grouped/global aggregate expression builders.
//! Does not own: aggregate validation policy or executor fold semantics.
//! Boundary: fluent aggregate intent construction lowered into grouped specs.

use crate::db::query::plan::AggregateKind;

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
