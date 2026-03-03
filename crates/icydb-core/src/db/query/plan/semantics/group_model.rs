use crate::{
    db::query::{
        builder::AggregateExpr,
        plan::{
            AggregateKind, FieldSlot, GroupAggregateSpec, GroupHavingClause, GroupHavingSpec,
            GroupHavingSymbol, GroupSpec, GroupedExecutionConfig,
        },
    },
    model::entity::{EntityModel, resolve_field_slot},
    value::Value,
};

impl AggregateKind {
    /// Return whether this terminal kind is `COUNT`.
    #[must_use]
    pub(in crate::db) const fn is_count(self) -> bool {
        AggregateExpr::is_count_kind(self)
    }

    /// Return whether this terminal kind is `SUM`.
    #[must_use]
    pub(in crate::db) const fn is_sum(self) -> bool {
        AggregateExpr::is_sum_kind(self)
    }

    /// Return whether this terminal kind supports explicit field targets.
    #[must_use]
    pub(in crate::db) const fn supports_field_targets(self) -> bool {
        AggregateExpr::supports_field_targets_kind(self)
    }

    /// Return whether this terminal kind belongs to the extrema family.
    #[must_use]
    pub(in crate::db) const fn is_extrema(self) -> bool {
        AggregateExpr::is_extrema_kind(self)
    }

    /// Return whether this terminal kind supports first/last value projection.
    #[must_use]
    pub(in crate::db) const fn supports_terminal_value_projection(self) -> bool {
        AggregateExpr::supports_terminal_value_projection_kind(self)
    }

    /// Return whether reducer updates for this kind require a decoded id payload.
    #[must_use]
    pub(in crate::db) const fn requires_decoded_id(self) -> bool {
        AggregateExpr::requires_decoded_id_kind(self)
    }

    /// Return whether grouped aggregate DISTINCT is supported for this kind.
    #[must_use]
    pub(in crate::db) const fn supports_grouped_distinct_v1(self) -> bool {
        AggregateExpr::supports_grouped_distinct_kind_v1(self)
    }

    /// Return whether global DISTINCT aggregate shape is supported without GROUP BY keys.
    #[must_use]
    pub(in crate::db) const fn supports_global_distinct_without_group_keys(self) -> bool {
        AggregateExpr::supports_global_distinct_without_group_keys_kind(self)
    }
}

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
        self.target_field.is_none()
            && (!self.distinct || AggregateExpr::supports_grouped_distinct_kind_v1(self.kind))
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

impl GroupHavingSpec {
    /// Borrow grouped HAVING clauses in declaration order.
    #[must_use]
    pub(crate) const fn clauses(&self) -> &[GroupHavingClause] {
        self.clauses.as_slice()
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
}
