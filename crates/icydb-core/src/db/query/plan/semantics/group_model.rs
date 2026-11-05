//! Module: query::plan::semantics::group_model
//! Responsibility: grouped semantic model helpers for aggregates, symbols, and group fields.
//! Does not own: grouped runtime fold execution or cursor token handling.
//! Boundary: derives planner-owned grouped semantic projections from query/model inputs.

#[cfg(test)]
mod aggregate_facts_tests;
#[cfg(test)]
mod having_tests;

use crate::{
    db::{
        QueryError,
        query::{
            builder::AggregateExpr,
            plan::{
                AggregateIdentity, AggregateKind, AggregateSemanticKeyRef, FieldSlot,
                FieldSlotAuthority, GroupAggregateSpec, GroupPlan, expr::Expr,
            },
            preparation::PreparationWork,
        },
        schema::{AcceptedFieldKind, SchemaInfo, canonicalize_filter_literal_for_persisted_kind},
    },
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::{borrow::Cow, sync::Arc};

/// Normalize an owned lowering intermediate in place. Unlike ordinary filter
/// conversion, grouped lists retain children that do not convert. On exhaustion
/// the caller must discard the intermediate, never publish a partial result.
fn canonicalize_grouped_having_numeric_literal_for_accepted_kind(
    field_kind: &AcceptedFieldKind,
    value: &mut Value,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    work.charge(Resource::NestedValueSteps, 1)?;
    match field_kind {
        AcceptedFieldKind::Relation { key_kind, .. } => {
            canonicalize_grouped_having_numeric_literal_for_accepted_kind(key_kind, value, work)?;
        }
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => {
            if let Value::List(values) = value {
                for item in values {
                    canonicalize_grouped_having_numeric_literal_for_accepted_kind(
                        inner, item, work,
                    )?;
                }
            }
        }
        AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::Map { .. }
        | AcceptedFieldKind::Composite { .. }
        | AcceptedFieldKind::Ulid => {}
        _ => {
            if let Some(Cow::Owned(canonical)) =
                canonicalize_filter_literal_for_persisted_kind(field_kind, value, work)?
            {
                *value = canonical;
            }
        }
    }
    Ok(())
}

/// Normalize one owned grouped `HAVING` intermediate through a direct/path expression
/// owner. The caller discards the intermediate if preparation fails.
pub(in crate::db) fn canonicalize_grouped_having_numeric_literal_for_expr(
    schema: &SchemaInfo,
    expr: &Expr,
    value: &mut Value,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    let Some(kind) = crate::db::query::plan::GroupField::accepted_kind_for_expr(schema, expr)
    else {
        return Ok(());
    };
    canonicalize_grouped_having_numeric_literal_for_accepted_kind(kind, value, work)
}

impl GroupAggregateSpec {
    /// Move one authored aggregate into grouped intent without normalization.
    #[must_use]
    pub(in crate::db) fn from_aggregate_expr(aggregate: AggregateExpr) -> Self {
        Self::from_shape(aggregate.into_shape())
    }

    /// Return the canonical grouped aggregate terminal kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> AggregateKind {
        self.shape().kind()
    }

    /// Build the canonical aggregate identity for this grouped terminal.
    #[must_use]
    pub(in crate::db) fn identity(&self) -> AggregateIdentity {
        AggregateIdentity::from_kind_input_and_distinct(
            self.kind(),
            self.identity_input_expr_owned(),
            self.raw_distinct(),
        )
    }

    /// Borrow the filter-aware semantic key for this grouped aggregate.
    #[must_use]
    pub(in crate::db) fn semantic_key(&self) -> AggregateSemanticKeyRef<'_> {
        AggregateSemanticKeyRef::new(
            self.kind(),
            self.input_expr(),
            self.filter_expr(),
            self.raw_distinct(),
        )
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
        self.shape().input_expr()
    }

    /// Borrow the canonical grouped aggregate filter expression, if any.
    #[must_use]
    pub(in crate::db) fn filter_expr(&self) -> Option<&Expr> {
        self.shape().filter_expr()
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
    pub(in crate::db) const fn semantic_distinct(&self) -> bool {
        AggregateIdentity::normalize_distinct_for_kind(self.kind(), self.raw_distinct())
    }

    /// Inspect row-count identity without owning or traversing aggregate input.
    #[must_use]
    pub(in crate::db) fn is_count_rows_only(&self) -> bool {
        AggregateIdentity::is_count_rows_input(self.kind(), self.input_expr(), self.raw_distinct())
    }

    /// Return the raw authored DISTINCT bit before semantic normalization.
    #[must_use]
    pub(in crate::db) const fn raw_distinct(&self) -> bool {
        self.shape().raw_distinct()
    }

    /// Return true when this aggregate is eligible for grouped ordered streaming.
    #[must_use]
    pub(in crate::db) fn streaming_compatible(&self) -> bool {
        self.kind()
            .supports_grouped_streaming(self.target_field().is_some(), self.semantic_distinct())
    }
}

impl GroupPlan {
    /// Borrow the canonical grouped HAVING expression.
    #[must_use]
    pub(in crate::db) const fn having_expr(&self) -> Option<&Expr> {
        self.having_expr.as_ref()
    }
}

/// Convert one grouped aggregate declaration back into the shared planner
/// aggregate expression used by grouped `HAVING`, explain, and tests.
#[must_use]
pub(in crate::db) fn group_aggregate_spec_expr(aggregate: &GroupAggregateSpec) -> AggregateExpr {
    AggregateExpr::from_shape(
        aggregate
            .shape()
            .clone()
            .with_raw_distinct(aggregate.semantic_distinct()),
    )
}

impl FieldSlot {
    /// Build one unresolved field slot used only where no field contract exists.
    #[must_use]
    pub(in crate::db) fn unresolved(index: usize, field: impl Into<String>) -> Self {
        Self {
            index,
            field: field.into().into(),
            authority: FieldSlotAuthority::Unresolved,
        }
    }

    const fn from_accepted_kind(
        index: usize,
        field: Arc<str>,
        kind: Arc<AcceptedFieldKind>,
    ) -> Self {
        Self {
            index,
            field,
            authority: FieldSlotAuthority::Accepted(kind),
        }
    }

    /// Resolve one field through exactly one schema authority lane.
    #[must_use]
    pub(in crate::db) fn resolve_with_schema(schema: &SchemaInfo, field: &str) -> Option<Self> {
        let (index, label, kind) = schema.retained_query_field_authority(field)?;
        Some(Self::from_accepted_kind(index, label, kind))
    }

    /// Return the stable accepted field slot.
    #[must_use]
    pub(in crate::db) const fn index(&self) -> usize {
        self.index
    }

    /// Return the diagnostic field label associated with this slot.
    #[must_use]
    pub(in crate::db) fn field(&self) -> &str {
        &self.field
    }

    /// Borrow the accepted field kind frozen by schema-backed planning.
    #[must_use]
    pub(in crate::db) fn accepted_kind(&self) -> Option<&AcceptedFieldKind> {
        match &self.authority {
            FieldSlotAuthority::Accepted(kind) => Some(kind.as_ref()),
            FieldSlotAuthority::Unresolved => None,
        }
    }

    /// Return whether this slot has no resolved field contract.
    #[must_use]
    pub(in crate::db) const fn is_unresolved(&self) -> bool {
        matches!(&self.authority, FieldSlotAuthority::Unresolved)
    }

    /// Build one accepted slot directly for focused boundary tests.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn from_test_accepted_kind(
        index: usize,
        field: impl Into<String>,
        kind: AcceptedFieldKind,
    ) -> Self {
        Self::from_accepted_kind(index, field.into().into(), Arc::new(kind))
    }
}

#[cfg(test)]
mod tests;
