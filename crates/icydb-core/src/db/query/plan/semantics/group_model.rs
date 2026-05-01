//! Module: query::plan::semantics::group_model
//! Responsibility: grouped semantic model helpers for aggregates, symbols, and group fields.
//! Does not own: grouped runtime fold execution or cursor token handling.
//! Boundary: derives planner-owned grouped semantic projections from query/model inputs.

use std::borrow::Cow;

use crate::{
    db::query::{
        builder::AggregateExpr,
        plan::{
            AggregateIdentity, AggregateKind, AggregateSemanticKey, FieldSlot, GroupAggregateSpec,
            GroupPlan, GroupSpec, GroupedExecutionConfig, expr::Expr,
        },
    },
    model::{entity::EntityModel, field::FieldKind},
};

impl GroupAggregateSpec {
    /// Build one grouped aggregate spec from one aggregate expression.
    #[must_use]
    pub(in crate::db) fn from_aggregate_expr(aggregate: &AggregateExpr) -> Self {
        Self {
            kind: aggregate.kind(),
            input_expr: aggregate.input_expr().cloned().map(Box::new),
            filter_expr: aggregate.filter_expr().cloned().map(Box::new),
            distinct: aggregate.is_distinct(),
        }
    }

    /// Return the canonical grouped aggregate terminal kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> AggregateKind {
        self.kind
    }

    /// Build the canonical aggregate identity for this grouped terminal.
    #[must_use]
    pub(in crate::db) fn identity(&self) -> AggregateIdentity {
        AggregateIdentity::from_parts(self.kind(), self.identity_input_expr_owned(), self.distinct)
    }

    /// Build the filter-aware semantic key for this grouped aggregate.
    #[must_use]
    pub(in crate::db) fn semantic_key(&self) -> AggregateSemanticKey {
        AggregateSemanticKey::from_identity(self.identity(), self.filter_expr().cloned())
    }

    /// Return the optional grouped aggregate target field.
    #[must_use]
    pub(in crate::db) fn target_field(&self) -> Option<&str> {
        match self.input_expr() {
            Some(Expr::Field(field_id)) => Some(field_id.as_str()),
            _ => None,
        }
    }

    /// Borrow the canonical grouped aggregate input expression, if any.
    #[must_use]
    pub(in crate::db) fn input_expr(&self) -> Option<&Expr> {
        self.input_expr.as_deref()
    }

    /// Borrow the canonical grouped aggregate filter expression, if any.
    #[must_use]
    pub(in crate::db) fn filter_expr(&self) -> Option<&Expr> {
        self.filter_expr.as_deref()
    }

    /// Build the canonical grouped aggregate input expression for identity-only
    /// comparisons.
    #[must_use]
    pub(in crate::db) fn identity_input_expr_owned(&self) -> Option<Expr> {
        if let Some(expr) = self.input_expr() {
            return Some(expr.clone());
        }

        None
    }

    /// Return whether this grouped aggregate terminal uses DISTINCT in identity.
    #[must_use]
    pub(in crate::db) fn distinct(&self) -> bool {
        self.identity().distinct()
    }

    /// Return true when this aggregate is eligible for grouped ordered streaming.
    #[must_use]
    pub(in crate::db) fn streaming_compatible_v1(&self) -> bool {
        self.kind
            .supports_grouped_streaming_v1(self.target_field().is_some(), self.distinct())
    }
}

impl GroupSpec {
    /// Build one global DISTINCT grouped shape from one aggregate expression.
    #[must_use]
    pub(in crate::db) fn global_distinct_shape_from_aggregate_expr(
        aggregate: &AggregateExpr,
        execution: GroupedExecutionConfig,
    ) -> Self {
        Self {
            group_fields: Vec::new(),
            aggregates: vec![GroupAggregateSpec::from_aggregate_expr(aggregate)],
            execution,
        }
    }
}

impl GroupPlan {
    /// Borrow the effective grouped HAVING expression for this grouped plan.
    #[must_use]
    pub(in crate::db) fn effective_having_expr(&self) -> Option<Cow<'_, Expr>> {
        self.having_expr.as_ref().map(Cow::Borrowed)
    }
}

/// Convert one grouped aggregate declaration back into the shared planner
/// aggregate expression used by grouped `HAVING`, explain, and tests.
#[must_use]
pub(in crate::db) fn group_aggregate_spec_expr(aggregate: &GroupAggregateSpec) -> AggregateExpr {
    let expr = match aggregate.identity_input_expr_owned() {
        Some(input_expr) => AggregateExpr::from_expression_input(aggregate.kind(), input_expr),
        None => AggregateExpr::from_semantic_parts(aggregate.kind(), None, false),
    };
    let expr = match aggregate.filter_expr() {
        Some(filter_expr) => expr.with_filter_expr(filter_expr.clone()),
        None => expr,
    };

    if aggregate.identity().distinct() {
        expr.distinct()
    } else {
        expr
    }
}

impl FieldSlot {
    /// Resolve one field name into its canonical model slot.
    #[must_use]
    pub(in crate::db) fn resolve(model: &EntityModel, field: &str) -> Option<Self> {
        let index = model.resolve_field_slot(field)?;
        let canonical = model
            .fields
            .get(index)
            .map_or(field, |model_field| model_field.name);

        Some(Self {
            index,
            field: canonical.to_string(),
            kind: model.fields.get(index).map(|field| field.kind),
        })
    }

    /// Return the stable slot index in `EntityModel::fields`.
    #[must_use]
    pub(in crate::db) const fn index(&self) -> usize {
        self.index
    }

    /// Return the diagnostic field label associated with this slot.
    #[must_use]
    pub(in crate::db) fn field(&self) -> &str {
        &self.field
    }

    /// Return the planner-frozen field kind when the slot has been validated.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> Option<FieldKind> {
        self.kind
    }
}
