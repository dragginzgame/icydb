use crate::db::query::plan::{
    AggregateKind,
    expr::{Expr, FieldId, canonicalize_aggregate_input_expr},
};

///
/// AggregateExpr
///
/// Composable aggregate expression used by query/fluent aggregate entrypoints.
/// This builder only carries declarative shape (`kind`, aggregate input
/// expression, optional filter expression, `distinct`) and does not perform
/// semantic validation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateExpr {
    kind: AggregateKind,
    input_expr: Option<Box<Expr>>,
    filter_expr: Option<Box<Expr>>,
    distinct: bool,
}

impl AggregateExpr {
    /// Construct one terminal aggregate expression with no input expression.
    const fn terminal(kind: AggregateKind) -> Self {
        Self {
            kind,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }
    }

    /// Construct one aggregate expression over one canonical field leaf.
    fn field_target(kind: AggregateKind, field: impl Into<String>) -> Self {
        Self {
            kind,
            input_expr: Some(Box::new(Expr::Field(FieldId::new(field.into())))),
            filter_expr: None,
            distinct: false,
        }
    }

    /// Construct one aggregate expression from one planner-owned input expression.
    pub(in crate::db) fn from_expression_input(kind: AggregateKind, input_expr: Expr) -> Self {
        Self {
            kind,
            input_expr: Some(Box::new(canonicalize_aggregate_input_expr(
                kind, input_expr,
            ))),
            filter_expr: None,
            distinct: false,
        }
    }

    /// Attach one planner-owned pre-aggregate filter expression to this aggregate.
    #[must_use]
    pub(in crate::db) fn with_filter_expr(mut self, filter_expr: Expr) -> Self {
        self.filter_expr = Some(Box::new(filter_expr));
        self
    }

    /// Enable DISTINCT modifier for this aggregate expression.
    #[must_use]
    pub const fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Borrow aggregate kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> AggregateKind {
        self.kind
    }

    /// Borrow the aggregate input expression, if any.
    #[must_use]
    pub(in crate::db) fn input_expr(&self) -> Option<&Expr> {
        self.input_expr.as_deref()
    }

    /// Borrow the aggregate filter expression, if any.
    #[must_use]
    pub(in crate::db) fn filter_expr(&self) -> Option<&Expr> {
        self.filter_expr.as_deref()
    }

    /// Borrow the optional target field when this aggregate input stays a plain field leaf.
    #[must_use]
    pub(in crate::db) fn target_field(&self) -> Option<&str> {
        match self.input_expr() {
            Some(Expr::Field(field)) => Some(field.as_str()),
            _ => None,
        }
    }

    /// Return true when DISTINCT is enabled.
    #[must_use]
    pub(in crate::db) const fn is_distinct(&self) -> bool {
        self.distinct
    }

    /// Build one aggregate expression directly from planner semantic parts.
    pub(in crate::db::query) fn from_semantic_parts(
        kind: AggregateKind,
        target_field: Option<String>,
        distinct: bool,
    ) -> Self {
        Self {
            kind,
            input_expr: target_field.map(|field| Box::new(Expr::Field(FieldId::new(field)))),
            filter_expr: None,
            distinct,
        }
    }

    /// Build one non-field-target terminal aggregate expression from one kind.
    #[cfg(test)]
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
}

/// Build `count(*)`.
#[must_use]
pub const fn count() -> AggregateExpr {
    AggregateExpr::terminal(AggregateKind::Count)
}

/// Build `count(field)`.
#[must_use]
pub fn count_by(field: impl AsRef<str>) -> AggregateExpr {
    AggregateExpr::field_target(AggregateKind::Count, field.as_ref().to_string())
}

/// Build `sum(field)`.
#[must_use]
pub fn sum(field: impl AsRef<str>) -> AggregateExpr {
    AggregateExpr::field_target(AggregateKind::Sum, field.as_ref().to_string())
}

/// Build `avg(field)`.
#[must_use]
pub fn avg(field: impl AsRef<str>) -> AggregateExpr {
    AggregateExpr::field_target(AggregateKind::Avg, field.as_ref().to_string())
}

/// Build `exists`.
#[must_use]
pub const fn exists() -> AggregateExpr {
    AggregateExpr::terminal(AggregateKind::Exists)
}

/// Build `first`.
#[must_use]
pub const fn first() -> AggregateExpr {
    AggregateExpr::terminal(AggregateKind::First)
}

/// Build `last`.
#[must_use]
pub const fn last() -> AggregateExpr {
    AggregateExpr::terminal(AggregateKind::Last)
}

/// Build `min`.
#[must_use]
pub const fn min() -> AggregateExpr {
    AggregateExpr::terminal(AggregateKind::Min)
}

/// Build `min(field)`.
#[must_use]
pub fn min_by(field: impl AsRef<str>) -> AggregateExpr {
    AggregateExpr::field_target(AggregateKind::Min, field.as_ref().to_string())
}

/// Build `max`.
#[must_use]
pub const fn max() -> AggregateExpr {
    AggregateExpr::terminal(AggregateKind::Max)
}

/// Build `max(field)`.
#[must_use]
pub fn max_by(field: impl AsRef<str>) -> AggregateExpr {
    AggregateExpr::field_target(AggregateKind::Max, field.as_ref().to_string())
}
