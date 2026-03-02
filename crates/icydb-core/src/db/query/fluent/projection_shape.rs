//! Module: query::fluent::projection_shape
//! Responsibility: canonical semantic shape builders for fluent projection terminals.
//! Does not own: executor projection materialization behavior.
//! Boundary: one semantic projection-construction spine for fluent load terminals.

use crate::db::query::builder::{
    AggregateExpr,
    aggregate::{count_by, sum},
};

/// Projection output shape for ranked `top_k` / `bottom_k` terminals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::fluent) enum RankedProjectionMode {
    Rows,
    Values,
    ValuesWithIds,
}

/// Ranking direction for `top_k` / `bottom_k` projection terminals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::fluent) enum RankedProjectionDirection {
    Top,
    Bottom,
}

/// Canonical fluent projection shape shared by projection helper terminals.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::query::fluent) enum ProjectionFieldShape {
    Field {
        target_field: String,
    },
    DistinctAggregate {
        aggregate: AggregateExpr,
    },
    Ranked {
        target_field: String,
        direction: RankedProjectionDirection,
        mode: RankedProjectionMode,
    },
}

impl ProjectionFieldShape {
    // Build one plain field projection shape.
    #[must_use]
    pub(in crate::db::query::fluent) fn field(field: impl AsRef<str>) -> Self {
        Self::Field {
            target_field: field.as_ref().to_string(),
        }
    }

    // Build one COUNT(DISTINCT field) projection shape through AggregateExpr.
    #[must_use]
    pub(in crate::db::query::fluent) fn count_distinct(field: impl AsRef<str>) -> Self {
        Self::DistinctAggregate {
            aggregate: count_by(field).distinct(),
        }
    }

    // Build one SUM(DISTINCT field) projection shape through AggregateExpr.
    #[must_use]
    pub(in crate::db::query::fluent) fn sum_distinct(field: impl AsRef<str>) -> Self {
        Self::DistinctAggregate {
            aggregate: sum(field).distinct(),
        }
    }

    // Build one ranked projection shape (`top_k` / `bottom_k`) for one target field.
    #[must_use]
    pub(in crate::db::query::fluent) fn ranked(
        field: impl AsRef<str>,
        direction: RankedProjectionDirection,
        mode: RankedProjectionMode,
    ) -> Self {
        Self::Ranked {
            target_field: field.as_ref().to_string(),
            direction,
            mode,
        }
    }

    // Borrow target-field identity shared by all fluent projection variants.
    #[must_use]
    pub(in crate::db::query::fluent) fn target_field(&self) -> &str {
        match self {
            Self::Field { target_field } | Self::Ranked { target_field, .. } => target_field,
            Self::DistinctAggregate { aggregate } => aggregate.target_field().expect(
                "distinct aggregate projection shape requires explicit target field in fluent spine",
            ),
        }
    }

    // Borrow ranked projection direction when this is one ranked terminal shape.
    #[must_use]
    pub(in crate::db::query::fluent) const fn ranked_direction(
        &self,
    ) -> Option<RankedProjectionDirection> {
        match self {
            Self::Ranked { direction, .. } => Some(*direction),
            Self::Field { .. } | Self::DistinctAggregate { .. } => None,
        }
    }

    // Borrow ranked projection output mode when this is one ranked terminal shape.
    #[must_use]
    pub(in crate::db::query::fluent) const fn ranked_mode(&self) -> Option<RankedProjectionMode> {
        match self {
            Self::Ranked { mode, .. } => Some(*mode),
            Self::Field { .. } | Self::DistinctAggregate { .. } => None,
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::query::{
        builder::{count_by, sum},
        fluent::projection_shape::{
            ProjectionFieldShape, RankedProjectionDirection, RankedProjectionMode,
        },
    };

    #[test]
    fn count_distinct_shape_is_backed_by_aggregate_expr() {
        let shape = ProjectionFieldShape::count_distinct("rank");
        let ProjectionFieldShape::DistinctAggregate { aggregate } = shape else {
            panic!("count distinct projection shape should be aggregate-backed");
        };

        assert_eq!(aggregate, count_by("rank").distinct());
        assert_eq!(
            aggregate.target_field(),
            Some("rank"),
            "count distinct aggregate shape must preserve target field",
        );
    }

    #[test]
    fn sum_distinct_shape_is_backed_by_aggregate_expr() {
        let shape = ProjectionFieldShape::sum_distinct("rank");
        let ProjectionFieldShape::DistinctAggregate { aggregate } = shape else {
            panic!("sum distinct projection shape should be aggregate-backed");
        };

        assert_eq!(aggregate, sum("rank").distinct());
        assert_eq!(
            aggregate.target_field(),
            Some("rank"),
            "sum distinct aggregate shape must preserve target field",
        );
    }

    #[test]
    fn ranked_shape_preserves_direction_and_projection_mode() {
        let shape = ProjectionFieldShape::ranked(
            "rank",
            RankedProjectionDirection::Top,
            RankedProjectionMode::ValuesWithIds,
        );

        assert_eq!(shape.target_field(), "rank");
        assert_eq!(
            shape.ranked_direction(),
            Some(RankedProjectionDirection::Top),
        );
        assert_eq!(
            shape.ranked_mode(),
            Some(RankedProjectionMode::ValuesWithIds)
        );
    }
}
