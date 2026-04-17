//! Module: query::plan::semantics::group_model
//! Responsibility: grouped semantic model helpers for aggregates, symbols, and group fields.
//! Does not own: grouped runtime fold execution or cursor token handling.
//! Boundary: derives planner-owned grouped semantic projections from query/model inputs.

use std::borrow::Cow;

use crate::{
    db::query::{
        builder::AggregateExpr,
        plan::{
            AggregateKind, FieldSlot, GroupAggregateSpec, GroupHavingClause, GroupHavingExpr,
            GroupHavingSymbol, GroupHavingValueExpr, GroupPlan, GroupSpec, GroupedExecutionConfig,
        },
    },
    model::{
        entity::{EntityModel, resolve_field_slot},
        field::FieldKind,
    },
    value::Value,
};

impl GroupAggregateSpec {
    /// Build one grouped aggregate spec from one aggregate expression.
    #[must_use]
    pub(in crate::db) fn from_aggregate_expr(aggregate: &AggregateExpr) -> Self {
        Self {
            kind: aggregate.kind(),
            target_field: aggregate.target_field().map(str::to_string),
            distinct: aggregate.is_distinct(),
        }
    }

    /// Return the canonical grouped aggregate terminal kind.
    #[must_use]
    pub(crate) const fn kind(&self) -> AggregateKind {
        self.kind
    }

    /// Return the optional grouped aggregate target field.
    #[must_use]
    pub(crate) fn target_field(&self) -> Option<&str> {
        self.target_field.as_deref()
    }

    /// Return whether this grouped aggregate terminal uses DISTINCT semantics.
    #[must_use]
    pub(crate) const fn distinct(&self) -> bool {
        self.distinct
    }

    /// Return true when this aggregate is eligible for grouped ordered streaming.
    #[must_use]
    pub(in crate::db) const fn streaming_compatible_v1(&self) -> bool {
        match self.kind {
            AggregateKind::Count => !self.distinct,
            AggregateKind::Sum | AggregateKind::Avg | AggregateKind::Min | AggregateKind::Max => {
                !self.distinct && self.target_field.is_some()
            }
            AggregateKind::Exists | AggregateKind::First | AggregateKind::Last => {
                self.target_field.is_none()
                    && (!self.distinct || self.kind.supports_grouped_distinct_v1())
            }
        }
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
    pub(in crate::db) fn effective_having_expr(&self) -> Option<Cow<'_, GroupHavingExpr>> {
        self.having_expr.as_ref().map(Cow::Borrowed)
    }
}

impl GroupHavingExpr {
    /// Lower one legacy compare clause into the `0.86` grouped HAVING expression model.
    #[must_use]
    pub(in crate::db) fn from_clause(clause: &GroupHavingClause) -> Self {
        Self::Compare {
            left: GroupHavingValueExpr::from_legacy_symbol(clause.symbol()),
            op: clause.op(),
            right: GroupHavingValueExpr::Literal(clause.value().clone()),
        }
    }

    /// Append one additional grouped HAVING expression onto this tree.
    #[must_use]
    pub(in crate::db) fn and(self, expr: Self) -> Self {
        match self {
            Self::And(mut children) => {
                children.push(expr);
                Self::And(children)
            }
            existing @ Self::Compare { .. } => Self::And(vec![existing, expr]),
        }
    }

    /// Construct one grouped HAVING compare expression from one grouped symbol.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn compare_symbol(
        symbol: GroupHavingSymbol,
        op: crate::db::predicate::CompareOp,
        value: Value,
    ) -> Self {
        Self::Compare {
            left: GroupHavingValueExpr::from_legacy_symbol(&symbol),
            op,
            right: GroupHavingValueExpr::Literal(value),
        }
    }
}

impl GroupHavingValueExpr {
    /// Lower one legacy grouped HAVING symbol into the slot-resolved value-expression model.
    #[must_use]
    pub(in crate::db) fn from_legacy_symbol(symbol: &GroupHavingSymbol) -> Self {
        match symbol {
            GroupHavingSymbol::GroupField(field_slot) => Self::GroupField(field_slot.clone()),
            GroupHavingSymbol::AggregateIndex(index) => Self::AggregateIndex(*index),
        }
    }
}

// Canonicalize one grouped-key compare literal against one grouped field kind
// when the Int<->Uint conversion is lossless and unambiguous. Both fluent
// grouped HAVING and SQL grouped HAVING bind through this helper so those two
// surfaces cannot drift on numeric grouped-key literal normalization again.
pub(in crate::db) fn canonicalize_grouped_having_numeric_literal_for_field_kind(
    field_kind: Option<FieldKind>,
    value: &Value,
) -> Option<Value> {
    match field_kind? {
        FieldKind::Relation { key_kind, .. } => {
            canonicalize_grouped_having_numeric_literal_for_field_kind(Some(*key_kind), value)
        }
        FieldKind::Int => match value {
            Value::Int(inner) => Some(Value::Int(*inner)),
            Value::Uint(inner) => i64::try_from(*inner).ok().map(Value::Int),
            _ => None,
        },
        FieldKind::Uint => match value {
            Value::Int(inner) => u64::try_from(*inner).ok().map(Value::Uint),
            Value::Uint(inner) => Some(Value::Uint(*inner)),
            _ => None,
        },
        _ => None,
    }
}

impl GroupHavingClause {
    /// Borrow grouped HAVING symbol reference.
    #[must_use]
    pub(crate) const fn symbol(&self) -> &GroupHavingSymbol {
        &self.symbol
    }

    /// Borrow grouped HAVING compare operator.
    #[must_use]
    pub(crate) const fn op(&self) -> crate::db::predicate::CompareOp {
        self.op
    }

    /// Borrow grouped HAVING comparison value.
    #[must_use]
    pub(crate) const fn value(&self) -> &Value {
        &self.value
    }
}

impl FieldSlot {
    /// Resolve one field name into its canonical model slot.
    #[must_use]
    pub(crate) fn resolve(model: &EntityModel, field: &str) -> Option<Self> {
        let index = resolve_field_slot(model, field)?;
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
    pub(crate) const fn index(&self) -> usize {
        self.index
    }

    /// Return the diagnostic field label associated with this slot.
    #[must_use]
    pub(crate) fn field(&self) -> &str {
        &self.field
    }

    /// Return the planner-frozen field kind when the slot has been validated.
    #[must_use]
    pub(crate) const fn kind(&self) -> Option<FieldKind> {
        self.kind
    }
}
