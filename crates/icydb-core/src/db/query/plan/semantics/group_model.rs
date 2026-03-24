//! Module: query::plan::semantics::group_model
//! Responsibility: grouped semantic model helpers for aggregates, symbols, and group fields.
//! Does not own: grouped runtime fold execution or cursor token handling.
//! Boundary: derives planner-owned grouped semantic projections from query/model inputs.

use crate::{
    db::query::{
        builder::AggregateExpr,
        plan::{
            AggregateKind, FieldSlot, GroupAggregateSpec, GroupHavingClause, GroupHavingSpec,
            GroupHavingSymbol, GroupSpec, GroupedExecutionConfig,
        },
    },
    error::InternalError,
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

    /// Return whether this terminal kind belongs to the extrema family.
    #[must_use]
    pub(in crate::db) const fn is_extrema(self) -> bool {
        AggregateExpr::is_extrema_kind(self)
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

    /// Construct one grouped HAVING executor invariant for unsupported compare
    /// operators that should already have been rejected by planner policy.
    pub(in crate::db) fn unsupported_operator(
        op: crate::db::predicate::CompareOp,
    ) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "unsupported grouped HAVING operator reached executor: {op:?}",
        ))
    }
}

impl GroupHavingSymbol {
    /// Construct one grouped HAVING executor invariant for non-list grouped keys.
    pub(in crate::db) fn grouped_key_must_be_list(value: &Value) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped HAVING requires list-shaped grouped keys, found {value:?}",
        ))
    }

    /// Construct one grouped HAVING executor invariant for symbols that are
    /// absent from the grouped key projection.
    pub(in crate::db) fn field_not_in_group_key_projection(field: &str) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped HAVING field is not in grouped key projection: field='{field}'",
        ))
    }

    /// Construct one grouped HAVING executor invariant for grouped-key offsets
    /// that exceed the materialized grouped key width.
    pub(in crate::db) fn group_key_offset_out_of_bounds(
        clause_index: usize,
        offset: usize,
        key_len: usize,
    ) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped HAVING group key offset out of bounds: clause_index={clause_index}, offset={offset}, key_len={key_len}",
        ))
    }

    /// Construct one grouped HAVING executor invariant for aggregate indexes
    /// that exceed the finalized grouped aggregate output width.
    pub(in crate::db) fn aggregate_index_out_of_bounds(
        clause_index: usize,
        aggregate_index: usize,
        aggregate_count: usize,
    ) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped HAVING aggregate index out of bounds: clause_index={clause_index}, aggregate_index={aggregate_index}, aggregate_count={aggregate_count}",
        ))
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
