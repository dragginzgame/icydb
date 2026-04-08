//! Module: query::intent::model
//! Responsibility: query-intent model normalization and planner handoff construction.
//! Does not own: executor runtime behavior or post-plan execution routing.
//! Boundary: turns fluent/query intent state into validated logical/planned contracts.

#[cfg(feature = "sql")]
use crate::db::query::plan::expr::{FieldId, ProjectionSelection};
use crate::{
    db::{
        access::{AccessPlan, canonical::canonicalize_value_set},
        predicate::{CompareOp, MissingRowPolicy, Predicate},
        query::{
            builder::aggregate::AggregateExpr,
            expr::{FilterExpr, SortExpr},
            intent::{IntentError, QueryError, QueryIntent},
            plan::{
                AccessPlannedQuery, GroupAggregateSpec, GroupHavingClause, GroupHavingSymbol,
                LogicalPlan, OrderSpec, QueryMode, build_logical_plan, fold_constant_predicate,
                is_limit_zero_load_window, logical_query_from_logical_inputs,
                normalize_query_predicate, plan_query_access, predicate_is_constant_false,
                resolve_group_field_slot, validate_group_query_semantics, validate_order_shape,
                validate_query_semantics,
            },
        },
        schema::SchemaInfo,
    },
    model::{entity::EntityModel, field::FieldKind, index::IndexModel},
    traits::FieldValue,
    value::Value,
};

///
/// QueryModel
///
/// Model-level query intent and planning context.
/// Consumes an `EntityModel` derived from typed entity definitions.
///

#[derive(Debug)]
pub(crate) struct QueryModel<'m, K> {
    model: &'m EntityModel,
    intent: QueryIntent<K>,
    consistency: MissingRowPolicy,
}

impl<'m, K: FieldValue> QueryModel<'m, K> {
    #[must_use]
    pub(crate) const fn new(model: &'m EntityModel, consistency: MissingRowPolicy) -> Self {
        Self {
            model,
            intent: QueryIntent::new(),
            consistency,
        }
    }

    /// Return the intent mode (load vs delete).
    #[must_use]
    pub(crate) const fn mode(&self) -> QueryMode {
        self.intent.mode()
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn model(&self) -> &'m EntityModel {
        self.model
    }

    #[must_use]
    pub(in crate::db::query::intent) fn has_explicit_order(&self) -> bool {
        self.intent.has_explicit_order()
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn has_grouping(&self) -> bool {
        self.intent.is_grouped()
    }

    #[must_use]
    pub(crate) fn filter(mut self, predicate: Predicate) -> Self {
        self.intent.append_predicate(predicate);
        self
    }

    /// Apply a dynamic filter expression using the model schema.
    pub(crate) fn filter_expr(self, expr: FilterExpr) -> Result<Self, QueryError> {
        let schema = SchemaInfo::from_entity_model(self.model)?;
        let predicate = expr.lower_with(&schema).map_err(QueryError::validate)?;

        Ok(self.filter(predicate))
    }

    /// Apply a dynamic sort expression using the model schema.
    pub(crate) fn sort_expr(self, expr: SortExpr) -> Result<Self, QueryError> {
        let schema = SchemaInfo::from_entity_model(self.model)?;
        let order = expr.lower_with(&schema).map_err(QueryError::from)?;

        validate_order_shape(Some(&order))
            .map_err(IntentError::from)
            .map_err(QueryError::from)?;

        Ok(self.order_spec(order))
    }

    /// Append an ascending sort key.
    #[must_use]
    pub(crate) fn order_by(mut self, field: impl AsRef<str>) -> Self {
        self.intent.push_order_ascending(field.as_ref());
        self
    }

    /// Append a descending sort key.
    #[must_use]
    pub(crate) fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
        self.intent.push_order_descending(field.as_ref());
        self
    }

    /// Set a fully-specified order spec (validated before reaching this boundary).
    pub(crate) fn order_spec(mut self, order: OrderSpec) -> Self {
        self.intent.set_order_spec(order);
        self
    }

    /// Enable DISTINCT semantics for this query intent.
    #[must_use]
    pub(crate) const fn distinct(mut self) -> Self {
        self.intent.set_distinct();
        self
    }

    /// Select one explicit scalar field projection list.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(crate) fn select_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let fields = fields
            .into_iter()
            .map(|field| FieldId::new(field.into()))
            .collect::<Vec<_>>();
        self.intent
            .set_projection_selection(ProjectionSelection::Fields(fields));

        self
    }

    // Resolve one grouped field into one stable field slot and append it to the
    // grouped spec in declaration order.
    pub(in crate::db::query::intent) fn push_group_field(
        mut self,
        field: &str,
    ) -> Result<Self, QueryError> {
        let field_slot = resolve_group_field_slot(self.model, field).map_err(QueryError::from)?;
        self.intent.push_group_field_slot(field_slot);

        Ok(self)
    }

    // Append one grouped aggregate terminal to the grouped declarative spec.
    pub(in crate::db::query::intent) fn push_group_aggregate(
        mut self,
        aggregate: AggregateExpr,
    ) -> Self {
        self.intent
            .push_group_aggregate(GroupAggregateSpec::from_aggregate_expr(&aggregate));

        self
    }

    // Override grouped hard limits for this grouped query.
    pub(in crate::db::query::intent) fn grouped_limits(
        mut self,
        max_groups: u64,
        max_group_bytes: u64,
    ) -> Self {
        self.intent.set_grouped_limits(max_groups, max_group_bytes);

        self
    }

    // Append one grouped HAVING compare clause after GROUP BY terminal declaration.
    fn push_having_clause(mut self, clause: GroupHavingClause) -> Result<Self, QueryError> {
        self.intent
            .push_having_clause(clause)
            .map_err(QueryError::intent)?;

        Ok(self)
    }

    // Append one grouped HAVING clause that references one grouped key field.
    pub(in crate::db::query::intent) fn push_having_group_clause(
        self,
        field: &str,
        op: CompareOp,
        value: Value,
    ) -> Result<Self, QueryError> {
        let field_slot = resolve_group_field_slot(self.model, field).map_err(QueryError::from)?;
        let value = canonicalize_group_field_numeric_value_for_kind(self.model, field, &value)
            .unwrap_or(value);

        self.push_having_clause(GroupHavingClause {
            symbol: GroupHavingSymbol::GroupField(field_slot),
            op,
            value,
        })
    }

    // Append one grouped HAVING clause that references one grouped aggregate output.
    pub(in crate::db::query::intent) fn push_having_aggregate_clause(
        self,
        aggregate_index: usize,
        op: CompareOp,
        value: Value,
    ) -> Result<Self, QueryError> {
        self.push_having_clause(GroupHavingClause {
            symbol: GroupHavingSymbol::AggregateIndex(aggregate_index),
            op,
            value,
        })
    }

    /// Set the access path to a single primary key lookup.
    pub(crate) fn by_id(mut self, id: K) -> Self {
        self.intent.set_by_id(id);
        self
    }

    /// Set the access path to a primary key batch lookup.
    pub(crate) fn by_ids<I>(mut self, ids: I) -> Self
    where
        I: IntoIterator<Item = K>,
    {
        self.intent.set_by_ids(ids);
        self
    }

    /// Set the access path to the singleton primary key.
    pub(crate) fn only(mut self, id: K) -> Self {
        self.intent.set_only(id);
        self
    }

    /// Mark this intent as a delete query.
    #[must_use]
    pub(crate) fn delete(mut self) -> Self {
        self.intent = self.intent.set_delete_mode();
        self
    }

    /// Apply a limit to the current mode.
    ///
    /// Load limits bound result size; delete limits bound mutation size.
    #[must_use]
    pub(crate) fn limit(mut self, limit: u32) -> Self {
        self.intent = self.intent.apply_limit(limit);
        self
    }

    /// Apply an offset to a load intent.
    ///
    /// When the intent is already in delete mode, this is recorded so
    /// intent validation can reject the invalid modifier combination.
    #[must_use]
    pub(crate) fn offset(mut self, offset: u32) -> Self {
        self.intent = self.intent.apply_offset(offset);
        self
    }

    /// Build a model-level logical plan using Value-based access keys.
    #[inline(never)]
    pub(in crate::db::query::intent) fn build_plan_model(
        &self,
    ) -> Result<AccessPlannedQuery, QueryError> {
        self.build_plan_model_with_indexes(self.model.indexes())
    }

    /// Build a model-level logical plan using one explicit planner-visible
    /// secondary-index set.
    #[inline(never)]
    pub(in crate::db::query::intent) fn build_plan_model_with_indexes(
        &self,
        visible_indexes: &[&'static IndexModel],
    ) -> Result<AccessPlannedQuery, QueryError> {
        // Phase 1: schema surface and intent validation.
        let schema_info = SchemaInfo::from_entity_model(self.model)?;
        self.intent.validate_policy_shape()?;

        // Phase 2: normalize scalar predicate and fold constant predicates
        // before access planning.
        let access_inputs = self.intent.planning_access_inputs();
        let normalized_predicate = fold_constant_predicate(normalize_query_predicate(
            &schema_info,
            access_inputs.predicate(),
        )?);
        let plan_mode = self.intent.mode();
        let limit_zero_window = is_limit_zero_load_window(plan_mode);
        let constant_false_predicate = predicate_is_constant_false(normalized_predicate.as_ref());
        let access_plan_value = if limit_zero_window || constant_false_predicate {
            AccessPlan::by_keys(Vec::new())
        } else {
            plan_query_access(
                self.model,
                visible_indexes,
                &schema_info,
                normalized_predicate.as_ref(),
                access_inputs.order(),
                access_inputs.into_key_access_override(),
            )?
        };
        let normalized_predicate = strip_redundant_primary_key_predicate_for_exact_access(
            self.model,
            &access_plan_value,
            normalized_predicate,
        );

        // Phase 3: assemble logical plan from normalized scalar/grouped intent.
        let logical_inputs = self.intent.planning_logical_inputs();
        let logical_query = logical_query_from_logical_inputs(
            logical_inputs,
            normalized_predicate,
            self.consistency,
        );
        let logical = build_logical_plan(self.model, logical_query);
        let mut plan = AccessPlannedQuery::from_parts_with_projection(
            logical,
            access_plan_value,
            self.intent.scalar().projection_selection.clone(),
        );
        simplify_limit_one_page_for_by_key_access(&mut plan);

        if plan.grouped_plan().is_some() {
            validate_group_query_semantics(&schema_info, self.model, &plan)?;
        } else {
            validate_query_semantics(&schema_info, self.model, &plan)?;
        }

        Ok(plan)
    }
}

// Keep grouped fluent HAVING literals aligned with the model-owned field kind
// when the conversion is lossless and unambiguous. This preserves plan parity
// with the SQL lowering boundary for grouped key clauses without widening the
// rule to unrelated aggregate symbols or non-numeric values.
fn canonicalize_group_field_numeric_value_for_kind(
    model: &EntityModel,
    field: &str,
    value: &Value,
) -> Option<Value> {
    let field_kind = model
        .fields()
        .iter()
        .find(|candidate| candidate.name() == field)
        .map(crate::model::field::FieldModel::kind)?;

    canonicalize_group_field_numeric_value(field_kind, value)
}

// Only the narrow Int<->Uint lossless cases are canonicalized here. Grouped
// fluent clauses already carry runtime `Value`s, so this helper only removes
// the remaining representation drift for numeric key fields that SQL literals
// already normalize before grouped HAVING binding.
fn canonicalize_group_field_numeric_value(field_kind: FieldKind, value: &Value) -> Option<Value> {
    match field_kind {
        FieldKind::Relation { key_kind, .. } => {
            canonicalize_group_field_numeric_value(*key_kind, value)
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
        FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Enum { .. }
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::List(_)
        | FieldKind::Map { .. }
        | FieldKind::Principal
        | FieldKind::Set(_)
        | FieldKind::Structured { .. }
        | FieldKind::Subaccount
        | FieldKind::Text
        | FieldKind::Timestamp
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Ulid
        | FieldKind::Unit => None,
    }
}

// Drop one normalized primary-key predicate when access planning already
// resolved the exact same authoritative PK access path. This prevents duplicate
// predicate evaluation and unlocks downstream PK fast paths.
fn strip_redundant_primary_key_predicate_for_exact_access(
    model: &EntityModel,
    access: &AccessPlan<Value>,
    normalized_predicate: Option<Predicate>,
) -> Option<Predicate> {
    let predicate = normalized_predicate?;

    if let Some(access_keys) = access.as_path().and_then(|path| path.as_by_keys())
        && !access_keys.is_empty()
        && predicate_matches_primary_key_in_set(&predicate, model.primary_key.name, access_keys)
    {
        return None;
    }

    if let Some(access_key) = access.as_path().and_then(|path| path.as_by_key()) {
        let Predicate::Compare(cmp) = &predicate else {
            return Some(predicate);
        };
        if cmp.field != model.primary_key.name || cmp.op != CompareOp::Eq {
            return Some(predicate);
        }
        if cmp.value != *access_key {
            return Some(predicate);
        }

        return None;
    }

    if let Some((start, end)) = access.as_primary_key_range_path()
        && predicate_matches_primary_key_half_open_range(
            &predicate,
            model.primary_key.name,
            start,
            end,
        )
    {
        return None;
    }

    Some(predicate)
}

// Return whether one normalized predicate is exactly the same primary-key IN
// set already guaranteed by one canonical `ByKeys` access path.
fn predicate_matches_primary_key_in_set(
    predicate: &Predicate,
    primary_key_name: &str,
    access_keys: &[Value],
) -> bool {
    let Predicate::Compare(cmp) = predicate else {
        return false;
    };
    if cmp.field != primary_key_name || cmp.op != CompareOp::In {
        return false;
    }

    let Value::List(predicate_keys) = &cmp.value else {
        return false;
    };

    let mut canonical_predicate_keys = predicate_keys.clone();
    canonicalize_value_set(&mut canonical_predicate_keys);

    canonical_predicate_keys == access_keys
}

// Return whether one normalized predicate is exactly the same half-open
// primary-key range already guaranteed by one `KeyRange` access path.
fn predicate_matches_primary_key_half_open_range(
    predicate: &Predicate,
    primary_key_name: &str,
    start: &Value,
    end: &Value,
) -> bool {
    let Predicate::And(children) = predicate else {
        return false;
    };
    if children.len() != 2 {
        return false;
    }

    let mut lower_matches = false;
    let mut upper_matches = false;
    for child in children {
        let Predicate::Compare(cmp) = child else {
            return false;
        };
        if cmp.field != primary_key_name {
            return false;
        }

        match cmp.op {
            CompareOp::Gte if cmp.value == *start => lower_matches = true,
            CompareOp::Lt if cmp.value == *end => upper_matches = true,
            _ => return false,
        }
    }

    lower_matches && upper_matches
}

// Collapse `LIMIT 1` pagination overhead when access is already one exact
// primary-key lookup and no offset is requested.
fn simplify_limit_one_page_for_by_key_access(plan: &mut AccessPlannedQuery) {
    if !plan
        .access
        .as_path()
        .is_some_and(|path: &crate::db::access::AccessPath<Value>| path.is_by_key())
    {
        return;
    }

    let scalar = match &mut plan.logical {
        LogicalPlan::Scalar(scalar) => scalar,
        LogicalPlan::Grouped(grouped) => &mut grouped.scalar,
    };
    let Some(page) = scalar.page.as_ref() else {
        return;
    };
    if page.offset != 0 || page.limit != Some(1) {
        return;
    }

    scalar.page = None;
}
