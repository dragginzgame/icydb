//! Module: query::plan::semantics::group_model
//! Responsibility: grouped semantic model helpers for aggregates, symbols, and group fields.
//! Does not own: grouped runtime fold execution or cursor token handling.
//! Boundary: derives planner-owned grouped semantic projections from query/model inputs.

use std::borrow::Cow;

use crate::{
    db::{
        query::{
            builder::AggregateExpr,
            plan::{
                AggregateIdentity, AggregateKind, AggregateSemanticKey, FieldSlot,
                FieldSlotAuthority, GroupAggregateSpec, GroupPlan, GroupSpec, expr::Expr,
            },
        },
        schema::{AcceptedFieldKind, SchemaInfo, canonicalize_filter_literal_for_persisted_kind},
    },
    value::Value,
};

/// Canonicalize one grouped `HAVING` literal through accepted schema authority.
#[must_use]
fn canonicalize_grouped_having_numeric_literal_for_accepted_kind(
    field_kind: &AcceptedFieldKind,
    value: &Value,
) -> Option<Value> {
    match field_kind {
        AcceptedFieldKind::Relation { key_kind, .. } => {
            canonicalize_grouped_having_numeric_literal_for_accepted_kind(key_kind, value)
        }
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => match value {
            Value::List(values) => Some(Value::List(
                values
                    .iter()
                    .map(|item| {
                        canonicalize_grouped_having_numeric_literal_for_accepted_kind(inner, item)
                            .unwrap_or_else(|| item.clone())
                    })
                    .collect(),
            )),
            _ => None,
        },
        AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::Map { .. }
        | AcceptedFieldKind::Composite { .. }
        | AcceptedFieldKind::Ulid => None,
        _ => canonicalize_filter_literal_for_persisted_kind(field_kind, value),
    }
}

/// Canonicalize one grouped `HAVING` literal through the strongest authority
/// carried by its planner slot.
#[must_use]
pub(in crate::db) fn canonicalize_grouped_having_numeric_literal_for_slot(
    field_slot: &FieldSlot,
    value: &Value,
) -> Option<Value> {
    canonicalize_grouped_having_numeric_literal_for_accepted_kind(
        field_slot.accepted_kind()?,
        value,
    )
}

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
        AggregateIdentity::from_kind_input_and_distinct(
            self.kind(),
            self.identity_input_expr_owned(),
            self.distinct,
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
    pub(in crate::db) fn streaming_compatible(&self) -> bool {
        self.kind
            .supports_grouped_streaming(self.target_field().is_some(), self.distinct())
    }
}

impl GroupSpec {}

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
        None => AggregateExpr::from_optional_field_input(aggregate.kind(), None, false),
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
    /// Build one unresolved field slot used only where no field contract exists.
    #[must_use]
    pub(in crate::db) fn unresolved(index: usize, field: impl Into<String>) -> Self {
        Self {
            index,
            field: field.into(),
            authority: FieldSlotAuthority::Unresolved,
        }
    }

    fn from_accepted_kind(index: usize, field: impl Into<String>, kind: AcceptedFieldKind) -> Self {
        Self {
            index,
            field: field.into(),
            authority: FieldSlotAuthority::Accepted(kind),
        }
    }

    /// Resolve one field through exactly one schema authority lane.
    #[must_use]
    pub(in crate::db) fn resolve_with_schema(schema: &SchemaInfo, field: &str) -> Option<Self> {
        let index = schema.field_slot_index(field)?;
        let kind = schema.accepted_field_contract(field)?.kind().clone();
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
    pub(in crate::db) const fn accepted_kind(&self) -> Option<&AcceptedFieldKind> {
        match &self.authority {
            FieldSlotAuthority::Accepted(kind) => Some(kind),
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
        Self::from_accepted_kind(index, field, kind)
    }
}

#[cfg(test)]
mod tests {
    use super::canonicalize_grouped_having_numeric_literal_for_accepted_kind;
    use crate::{db::schema::AcceptedFieldKind, types::EntityTag, value::Value};

    #[test]
    fn accepted_grouped_having_literal_canonicalization_recurses_through_relations() {
        let relation = AcceptedFieldKind::Relation {
            target_path: "demo::Target".to_string(),
            target_entity_name: "Target".to_string(),
            target_entity_tag: EntityTag::new(1),
            target_store_path: "demo::store::TargetStore".to_string(),
            key_kind: Box::new(AcceptedFieldKind::Nat64),
        };

        assert_eq!(
            canonicalize_grouped_having_numeric_literal_for_accepted_kind(
                &relation,
                &Value::Int64(7),
            ),
            Some(Value::Nat64(7)),
        );
    }

    #[test]
    fn accepted_grouped_having_literal_canonicalization_recurses_through_lists() {
        let list = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Int64));

        assert_eq!(
            canonicalize_grouped_having_numeric_literal_for_accepted_kind(
                &list,
                &Value::List(vec![Value::Nat64(3), Value::Int64(5)]),
            ),
            Some(Value::List(vec![Value::Int64(3), Value::Int64(5)])),
        );
    }

    #[test]
    fn accepted_grouped_having_literal_canonicalization_does_not_widen_ulid_text() {
        assert_eq!(
            canonicalize_grouped_having_numeric_literal_for_accepted_kind(
                &AcceptedFieldKind::Ulid,
                &Value::Text("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string()),
            ),
            None,
        );
    }
}
