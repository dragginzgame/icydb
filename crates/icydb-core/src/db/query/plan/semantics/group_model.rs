//! Module: query::plan::semantics::group_model
//! Responsibility: grouped semantic model helpers for aggregates, symbols, and group fields.
//! Does not own: grouped runtime fold execution or cursor token handling.
//! Boundary: derives planner-owned grouped semantic projections from query/model inputs.

#[cfg(test)]
mod having_tests;

use crate::{
    db::{
        QueryError,
        query::{
            builder::AggregateExpr,
            plan::{
                AggregateIdentity, AggregateKind, AggregateSemanticKey, AggregateShape, FieldSlot,
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

    /// Build one grouped aggregate spec from an optional field input.
    #[must_use]
    pub(in crate::db) fn from_optional_field_input(
        kind: AggregateKind,
        target_field: Option<String>,
        distinct: bool,
    ) -> Self {
        Self::from_shape(AggregateShape::from_optional_field_input(
            kind,
            target_field,
            distinct,
        ))
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
    pub(in crate::db) fn semantic_distinct(&self) -> bool {
        self.identity().distinct()
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
            field: field.into(),
            authority: FieldSlotAuthority::Unresolved,
        }
    }

    fn from_accepted_kind(
        index: usize,
        field: impl Into<String>,
        kind: Arc<AcceptedFieldKind>,
    ) -> Self {
        Self {
            index,
            field: field.into(),
            authority: FieldSlotAuthority::Accepted(kind),
        }
    }

    /// Resolve one field through exactly one schema authority lane.
    #[must_use]
    pub(in crate::db) fn resolve_with_schema(schema: &SchemaInfo, field: &str) -> Option<Self> {
        let (index, kind) = schema.retained_query_field_authority(field)?;
        Some(Self::from_accepted_kind(index, field, kind))
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
        Self::from_accepted_kind(index, field, Arc::new(kind))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            query::{
                builder::{AggregateExpr, count, min_by, sum},
                plan::{AggregateKind, GroupAggregateSpec, expr::Expr},
            },
            schema::AcceptedFieldKind,
        },
        types::EntityTag,
        value::Value,
    };

    use super::{
        canonicalize_grouped_having_numeric_literal_for_accepted_kind, group_aggregate_spec_expr,
    };

    #[test]
    fn owned_group_aggregate_preserves_operands_and_raw_shape() {
        let aggregate = min_by("rank")
            .with_filter_expr(Expr::Literal(Value::Text("x".repeat(4096))))
            .distinct();
        let expected = aggregate.clone().into_shape();
        let input_address = std::ptr::from_ref(aggregate.input_expr().expect("input"));
        let filter_address = std::ptr::from_ref(aggregate.filter_expr().expect("filter"));

        let grouped = GroupAggregateSpec::from_aggregate_expr(aggregate);
        assert_eq!(grouped.shape(), &expected);
        assert!(grouped.raw_distinct());
        assert!(!grouped.semantic_distinct());
        assert_eq!(
            std::ptr::from_ref(grouped.input_expr().expect("input")),
            input_address
        );
        assert_eq!(
            std::ptr::from_ref(grouped.filter_expr().expect("filter")),
            filter_address
        );
    }

    #[test]
    fn aggregate_wrappers_preserve_raw_and_semantic_equality_domains() {
        let raw_min = min_by("rank");
        let raw_distinct_min = min_by("rank").distinct();
        assert_ne!(raw_min, raw_distinct_min);

        let grouped_min = GroupAggregateSpec::from_aggregate_expr(raw_min);
        let grouped_distinct_min = GroupAggregateSpec::from_aggregate_expr(raw_distinct_min);
        assert_eq!(grouped_min, grouped_distinct_min);
        assert!(grouped_distinct_min.raw_distinct());
        assert!(!grouped_distinct_min.semantic_distinct());

        let raw_count_rows = count();
        let raw_count_literal = AggregateExpr::from_expression_input(
            AggregateKind::Count,
            Expr::Literal(Value::Nat64(1)),
        );
        assert_ne!(raw_count_rows, raw_count_literal);
        assert_eq!(
            GroupAggregateSpec::from_aggregate_expr(raw_count_rows),
            GroupAggregateSpec::from_aggregate_expr(raw_count_literal),
        );

        assert_ne!(
            GroupAggregateSpec::from_aggregate_expr(sum("rank")),
            GroupAggregateSpec::from_aggregate_expr(sum("rank").distinct()),
        );
        assert_ne!(
            GroupAggregateSpec::from_aggregate_expr(
                sum("rank").with_filter_expr(Expr::Literal(Value::Bool(true))),
            ),
            GroupAggregateSpec::from_aggregate_expr(
                sum("rank").with_filter_expr(Expr::Literal(Value::Bool(false))),
            ),
        );
    }

    #[test]
    fn grouped_projection_round_trip_normalizes_only_semantic_distinct() {
        let grouped = GroupAggregateSpec::from_aggregate_expr(min_by("rank").distinct());
        let projected = group_aggregate_spec_expr(&grouped);

        assert_eq!(projected, min_by("rank"));
        assert!(!projected.is_distinct());
        assert!(grouped.raw_distinct());
    }

    #[test]
    fn accepted_grouped_having_literal_canonicalization_recurses_through_relations() {
        let relation = AcceptedFieldKind::Relation {
            target_path: "demo::Target".to_string(),
            target_entity_name: "Target".to_string(),
            target_entity_tag: EntityTag::new(1),
            target_store_path: "demo::store::TargetStore".to_string(),
            key_kind: Box::new(AcceptedFieldKind::Nat64),
        };

        let mut value = Value::Int64(7);
        crate::db::query::preparation::with_preparation_work(|work| {
            canonicalize_grouped_having_numeric_literal_for_accepted_kind(
                &relation, &mut value, work,
            )
        })
        .expect("literal preparation");
        assert_eq!(value, Value::Nat64(7));
    }

    #[test]
    fn accepted_grouped_having_literal_canonicalization_recurses_through_lists() {
        let list = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Int64));

        let mut value = Value::List(vec![Value::Nat64(3), Value::Int64(5)]);
        crate::db::query::preparation::with_preparation_work(|work| {
            canonicalize_grouped_having_numeric_literal_for_accepted_kind(&list, &mut value, work)
        })
        .expect("literal preparation");
        assert_eq!(value, Value::List(vec![Value::Int64(3), Value::Int64(5)]));
    }

    #[test]
    fn accepted_grouped_having_literal_canonicalization_does_not_widen_ulid_text() {
        let mut value = Value::Text("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string());
        crate::db::query::preparation::with_preparation_work(|work| {
            canonicalize_grouped_having_numeric_literal_for_accepted_kind(
                &AcceptedFieldKind::Ulid,
                &mut value,
                work,
            )
        })
        .expect("literal preparation");
        assert_eq!(value, Value::Text("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string()));
    }
}
