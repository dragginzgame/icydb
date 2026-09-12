//! Module: query::plan::semantics::identity
//! Responsibility: aggregate identity rules shared by global and grouped paths.
//! Does not own: aggregate execution, grouping keys, or runtime reducer state.
//! Boundary: normalizes aggregate function, input, and observable DISTINCT meaning.

use crate::db::query::{
    builder::AggregateExpr,
    plan::{
        AggregateKind,
        expr::{Expr, aggregate_count_input_expr_is_non_null_literal},
    },
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
pub(in crate::db) enum AggregateIdentity {
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
    /// Inspect row-count identity without constructing or copying its input.
    /// Owned identity normalization uses this same rule.
    #[must_use]
    pub(in crate::db) fn is_count_rows_input(
        kind: AggregateKind,
        input_expr: Option<&Expr>,
        distinct: bool,
    ) -> bool {
        kind == AggregateKind::Count
            && !distinct
            && input_expr.is_none_or(aggregate_count_input_expr_is_non_null_literal)
    }

    /// Build aggregate identity from its kind, input, and DISTINCT bit.
    ///
    /// Non-distinct `COUNT` over a non-null literal normalizes to row count
    /// identity so SQL, grouped, and fluent aggregate callers share the same
    /// meaning-level key.
    #[must_use]
    pub(in crate::db) fn from_kind_input_and_distinct(
        kind: AggregateKind,
        input_expr: Option<Expr>,
        distinct: bool,
    ) -> Self {
        let input_expr = normalize_aggregate_identity_input(kind, input_expr, distinct);

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
    pub(in crate::db) fn from_aggregate_expr(aggregate: &AggregateExpr) -> Self {
        Self::from_kind_input_and_distinct(
            aggregate.kind(),
            aggregate.input_expr().cloned(),
            aggregate.is_distinct(),
        )
    }

    /// Return whether raw DISTINCT is observable in aggregate identity.
    #[must_use]
    pub(in crate::db) const fn normalize_distinct_for_kind(
        kind: AggregateKind,
        distinct: bool,
    ) -> bool {
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
    pub(in crate::db) const fn kind(&self) -> AggregateKind {
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
    pub(in crate::db) const fn input_expr(&self) -> Option<&Expr> {
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

    /// Return whether DISTINCT changes observable aggregate behavior.
    #[must_use]
    pub(in crate::db) const fn distinct(&self) -> bool {
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
    pub(in crate::db) const fn target_field(&self) -> Option<&str> {
        let Some(Expr::Field(field)) = self.input_expr() else {
            return None;
        };

        Some(field.as_str())
    }

    /// Return whether grouped DISTINCT needs per-value deduplication.
    #[must_use]
    pub(in crate::db) const fn uses_grouped_distinct_value_dedup(&self) -> bool {
        matches!(
            self,
            Self::Count { distinct: true, .. }
                | Self::Sum { distinct: true, .. }
                | Self::Avg { distinct: true, .. }
        )
    }
}

fn normalize_aggregate_identity_input(
    kind: AggregateKind,
    input_expr: Option<Expr>,
    distinct: bool,
) -> Option<Expr> {
    if AggregateIdentity::is_count_rows_input(kind, input_expr.as_ref(), distinct) {
        return None;
    }

    input_expr
}

///
/// AggregateSemanticKey
///
/// AggregateSemanticKey is the filter-aware aggregate equivalence key shared
/// by grouped planning and SQL aggregate lowering. `AggregateIdentity` owns the
/// observable aggregate function/input/DISTINCT meaning, while this wrapper
/// keeps aggregate-local filters as a separate semantic dimension.
///
#[cfg(any(feature = "sql", test))]
#[derive(Clone, Debug, Eq)]
pub(in crate::db) struct AggregateSemanticKey {
    identity: AggregateIdentity,
    filter_expr: Option<Expr>,
}

#[cfg(any(feature = "sql", test))]
impl AggregateSemanticKey {
    /// Borrow the retained key through the same comparison form as raw inputs.
    #[must_use]
    pub(in crate::db) fn as_ref(&self) -> AggregateSemanticKeyRef<'_> {
        AggregateSemanticKeyRef::new(
            self.identity.kind(),
            self.identity.input_expr(),
            self.filter_expr.as_ref(),
            self.identity.distinct(),
        )
    }

    /// Build one semantic key from one raw aggregate expression.
    #[must_use]
    pub(in crate::db) fn from_aggregate_expr(aggregate: &AggregateExpr) -> Self {
        Self {
            identity: AggregateIdentity::from_aggregate_expr(aggregate),
            filter_expr: aggregate.filter_expr().cloned(),
        }
    }

    /// Move this key into its identity and filter components.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) fn into_identity_and_filter(self) -> (AggregateIdentity, Option<Expr>) {
        (self.identity, self.filter_expr)
    }
}

#[cfg(any(feature = "sql", test))]
impl PartialEq for AggregateSemanticKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

/// Borrowed semantic comparison; owning a key is necessary only for retention.
/// COUNT input and DISTINCT normalization remain owned by AggregateIdentity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AggregateSemanticKeyRef<'a> {
    kind: AggregateKind,
    input_expr: Option<&'a Expr>,
    filter_expr: Option<&'a Expr>,
    distinct: bool,
}

impl<'a> AggregateSemanticKeyRef<'a> {
    /// Return the canonical aggregate function.
    #[must_use]
    pub(in crate::db) const fn kind(self) -> AggregateKind {
        self.kind
    }

    /// Borrow the canonical input, including COUNT row-count normalization.
    #[must_use]
    pub(in crate::db) const fn input_expr(self) -> Option<&'a Expr> {
        self.input_expr
    }

    /// Borrow the aggregate-local filter without formatting or copying it.
    #[must_use]
    pub(in crate::db) const fn filter_expr(self) -> Option<&'a Expr> {
        self.filter_expr
    }

    /// Return whether DISTINCT is observable for this function.
    #[must_use]
    pub(in crate::db) const fn distinct(self) -> bool {
        self.distinct
    }

    /// Borrow canonical identity components without copying input/filter trees.
    #[must_use]
    pub(in crate::db) fn new(
        kind: AggregateKind,
        input_expr: Option<&'a Expr>,
        filter_expr: Option<&'a Expr>,
        distinct: bool,
    ) -> Self {
        Self {
            kind,
            input_expr: if AggregateIdentity::is_count_rows_input(kind, input_expr, distinct) {
                None
            } else {
                input_expr
            },
            filter_expr,
            distinct: AggregateIdentity::normalize_distinct_for_kind(kind, distinct),
        }
    }

    /// Borrow one authored aggregate's semantic comparison key.
    #[must_use]
    pub(in crate::db) fn from_aggregate_expr(aggregate: &'a AggregateExpr) -> Self {
        Self::new(
            aggregate.kind(),
            aggregate.input_expr(),
            aggregate.filter_expr(),
            aggregate.is_distinct(),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::value::Value;

    use super::*;

    #[test]
    fn borrowed_semantic_keys_preserve_owned_identity_and_filter_equivalence() {
        use crate::db::query::plan::AggregateShape;

        let mut expressions = Vec::new();
        for kind in [
            AggregateKind::Count,
            AggregateKind::Sum,
            AggregateKind::Avg,
            AggregateKind::Min,
            AggregateKind::Max,
            AggregateKind::Exists,
            AggregateKind::First,
            AggregateKind::Last,
        ] {
            for distinct in [false, true] {
                for input in [
                    None,
                    Some(Expr::Literal(Value::Null)),
                    Some(Expr::Literal(Value::Nat64(1))),
                    Some(Expr::Literal(Value::Nat64(2))),
                    Some(Expr::Field("amount".into())),
                    Some(Expr::Field("rank".into())),
                ] {
                    for filter in [
                        None,
                        Some(Expr::Literal(Value::Bool(true))),
                        Some(Expr::Literal(Value::Bool(false))),
                    ] {
                        let mut shape = input
                            .clone()
                            .map_or_else(
                                || AggregateShape::terminal(kind),
                                |input| AggregateShape::from_expression_input(kind, input),
                            )
                            .with_raw_distinct(distinct);
                        if let Some(filter) = filter {
                            shape = shape.with_filter_expr(filter);
                        }
                        expressions.push(AggregateExpr::from_shape(shape));
                    }
                }
            }
        }
        let owned: Vec<_> = expressions
            .iter()
            .map(AggregateSemanticKey::from_aggregate_expr)
            .collect();
        for (left_expr, left) in expressions.iter().zip(&owned) {
            let borrowed = AggregateSemanticKeyRef::from_aggregate_expr(left_expr);
            assert_eq!(borrowed, left.as_ref());
            // Canonical views share retained expression nodes; normalization
            // may omit COUNT's input, but never copies or rewrites the source.
            if let Some(input) = borrowed.input_expr {
                assert!(std::ptr::eq(input, left_expr.input_expr().unwrap()));
            }
            if let Some(filter) = borrowed.filter_expr {
                assert!(std::ptr::eq(filter, left_expr.filter_expr().unwrap()));
            }
            for (right_expr, right) in expressions.iter().zip(&owned) {
                let expected =
                    left.identity == right.identity && left.filter_expr == right.filter_expr;
                assert_eq!(
                    borrowed == AggregateSemanticKeyRef::from_aggregate_expr(right_expr),
                    expected
                );
                assert_eq!(left == right, expected);
            }
        }
    }

    #[test]
    fn aggregate_identity_normalizes_only_non_distinct_count_non_null_literals() {
        let literal_count = AggregateIdentity::from_kind_input_and_distinct(
            AggregateKind::Count,
            Some(Expr::Literal(Value::Nat64(1))),
            false,
        );
        let null_count = AggregateIdentity::from_kind_input_and_distinct(
            AggregateKind::Count,
            Some(Expr::Literal(Value::Null)),
            false,
        );
        let distinct_literal_count = AggregateIdentity::from_kind_input_and_distinct(
            AggregateKind::Count,
            Some(Expr::Literal(Value::Nat64(1))),
            true,
        );

        assert!(matches!(
            literal_count,
            AggregateIdentity::Count {
                input_expr: None,
                distinct: false,
            }
        ));
        assert!(matches!(
            null_count.input_expr(),
            Some(Expr::Literal(Value::Null))
        ));
        assert!(matches!(
            distinct_literal_count,
            AggregateIdentity::Count {
                input_expr: Some(Expr::Literal(Value::Nat64(1))),
                distinct: true
            }
        ));
    }
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(AggregateIdentity {
Self::Count{input_expr,distinct} => [input_expr,distinct],
Self::Sum{input_expr,distinct} => [input_expr,distinct],
Self::Avg{input_expr,distinct} => [input_expr,distinct],
Self::Min{input_expr} => [input_expr],
Self::Max{input_expr} => [input_expr],
Self::Exists{input_expr,distinct} => [input_expr,distinct],
Self::First{input_expr,distinct} => [input_expr,distinct],
Self::Last{input_expr,distinct} => [input_expr,distinct],
});
