//! Module: query::plan::aggregate_shape
//! Responsibility: raw field-bearing aggregate declaration shape shared by query wrappers.
//! Does not own: aggregate semantic equality, validation, or executor state.
//! Boundary: builder and logical-plan wrappers choose their own equality over this raw shape.

use crate::db::query::plan::{
    expr::{Expr, FieldId, canonicalize_aggregate_input_expr},
    model::AggregateKind,
};

/// Raw aggregate declaration fields shared by builder and logical-plan wrappers.
///
/// Equality on this type is deliberately structural. Semantic aggregate
/// equality remains owned by `AggregateIdentity` and `AggregateSemanticKey`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AggregateShape {
    kind: AggregateKind,
    input_expr: Option<Box<Expr>>,
    filter_expr: Option<Box<Expr>>,
    distinct: bool,
}

impl AggregateShape {
    /// Construct one terminal aggregate declaration with no input expression.
    #[must_use]
    pub(in crate::db) const fn terminal(kind: AggregateKind) -> Self {
        Self {
            kind,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }
    }

    /// Construct one aggregate declaration over a canonical field leaf.
    #[must_use]
    pub(in crate::db) fn field_target(kind: AggregateKind, field: String) -> Self {
        Self {
            kind,
            input_expr: Some(Box::new(Expr::Field(FieldId::new(field)))),
            filter_expr: None,
            distinct: false,
        }
    }

    /// Construct one aggregate declaration over a canonicalized input expression.
    #[must_use]
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

    /// Construct one aggregate declaration from an optional field input.
    #[must_use]
    pub(in crate::db) fn from_optional_field_input(
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

    /// Attach one pre-aggregate filter expression.
    #[must_use]
    pub(in crate::db) fn with_filter_expr(mut self, filter_expr: Expr) -> Self {
        self.filter_expr = Some(Box::new(filter_expr));
        self
    }

    /// Replace the raw authored DISTINCT bit without applying semantic normalization.
    #[must_use]
    pub(in crate::db) const fn with_raw_distinct(mut self, distinct: bool) -> Self {
        self.distinct = distinct;
        self
    }

    /// Replace the raw authored DISTINCT bit in place.
    pub(in crate::db) const fn set_raw_distinct(&mut self, distinct: bool) {
        self.distinct = distinct;
    }

    /// Return the aggregate kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> AggregateKind {
        self.kind
    }

    /// Borrow the aggregate input expression, if present.
    #[must_use]
    pub(in crate::db) fn input_expr(&self) -> Option<&Expr> {
        self.input_expr.as_deref()
    }

    /// Borrow the aggregate-local filter expression, if present.
    #[must_use]
    pub(in crate::db) fn filter_expr(&self) -> Option<&Expr> {
        self.filter_expr.as_deref()
    }

    /// Return the raw authored DISTINCT bit.
    #[must_use]
    pub(in crate::db) const fn raw_distinct(&self) -> bool {
        self.distinct
    }
}
