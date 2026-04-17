//! Module: query::intent::model
//! Responsibility: query-intent model normalization and planner handoff construction.
//! Does not own: executor runtime behavior or post-plan execution routing.
//! Boundary: turns fluent/query intent state into validated logical/planned contracts.

use crate::db::query::intent::{
    StructuralQueryCacheKey,
    state::{GroupedIntent, ScalarIntent},
};
#[cfg(feature = "sql")]
use crate::db::query::plan::expr::FieldId;
use crate::{
    db::{
        access::{AccessPlan, canonical::canonicalize_value_set},
        predicate::{CompareOp, MissingRowPolicy, Predicate},
        query::{
            builder::aggregate::AggregateExpr,
            expr::{FilterExpr, SortExpr},
            intent::{IntentError, QueryError, QueryIntent},
            plan::{
                AccessPlannedQuery, GroupAggregateSpec, GroupHavingClause, GroupHavingExpr,
                GroupHavingSymbol, LogicalPlan, OrderSpec, QueryMode, VisibleIndexes,
                build_logical_plan, canonicalize_grouped_having_numeric_literal_for_field_kind,
                expr::ProjectionSelection, fold_constant_predicate, is_limit_zero_load_window,
                logical_query_from_logical_inputs, normalize_query_predicate, plan_query_access,
                predicate_is_constant_false, resolve_group_field_slot,
                validate_group_query_semantics, validate_order_shape, validate_query_semantics,
            },
        },
        schema::SchemaInfo,
    },
    model::entity::EntityModel,
    traits::FieldValue,
    value::Value,
};

///
/// QueryModel
///
/// Model-level query intent and planning context.
/// Consumes an `EntityModel` derived from typed entity definitions.
///

#[derive(Clone, Debug)]
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

    pub(in crate::db) fn structural_cache_key_with_normalized_predicate(
        &self,
        predicate: Option<&Predicate>,
    ) -> StructuralQueryCacheKey {
        StructuralQueryCacheKey::from_query_model_with_normalized_predicate(self, predicate)
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
    pub(in crate::db::query::intent) const fn scalar_intent_for_cache_key(
        &self,
    ) -> &ScalarIntent<K> {
        self.intent.scalar()
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn grouped_intent_for_cache_key(
        &self,
    ) -> Option<&GroupedIntent<K>> {
        self.intent.grouped()
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn consistency_for_cache_key(&self) -> MissingRowPolicy {
        self.consistency
    }

    #[must_use]
    pub(crate) fn filter(mut self, predicate: Predicate) -> Self {
        self.intent.append_predicate(predicate);
        self
    }

    /// Apply a dynamic filter expression using the model schema.
    pub(crate) fn filter_expr(self, expr: FilterExpr) -> Result<Self, QueryError> {
        let schema = SchemaInfo::cached_for_entity_model(self.model);
        let predicate = expr.lower_with(schema).map_err(QueryError::validate)?;

        Ok(self.filter(predicate))
    }

    /// Apply a dynamic sort expression using the model schema.
    pub(crate) fn sort_expr(self, expr: SortExpr) -> Result<Self, QueryError> {
        let schema = SchemaInfo::cached_for_entity_model(self.model);
        let order = expr.lower_with(schema).map_err(QueryError::from)?;

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

    /// Override scalar projection selection with one already-lowered planner contract.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db::query::intent) fn projection_selection(
        mut self,
        selection: ProjectionSelection,
    ) -> Self {
        self.intent.set_projection_selection(selection);
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
        let value =
            canonicalize_grouped_having_numeric_literal_for_field_kind(field_slot.kind(), &value)
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

    // Append one widened grouped HAVING expression after GROUP BY terminal declaration.
    pub(in crate::db::query::intent) fn push_having_expr(
        mut self,
        expr: GroupHavingExpr,
    ) -> Result<Self, QueryError> {
        self.intent
            .push_having_expr(expr)
            .map_err(QueryError::intent)?;

        Ok(self)
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

    /// Apply an offset to the current mode.
    ///
    /// Load mode uses this as a pagination offset. Delete mode uses this as an
    /// ordered delete window offset.
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
        self.build_plan_model_with_indexes(&VisibleIndexes::schema_owned(self.model.indexes()))
    }

    /// Build a model-level logical plan using one explicit planner-visible
    /// secondary-index set.
    #[inline(never)]
    pub(in crate::db::query::intent) fn build_plan_model_with_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        let normalized_predicate = self.prepare_normalized_scalar_predicate()?;

        self.build_plan_model_with_indexes_from_normalized_predicate(
            visible_indexes,
            normalized_predicate,
        )
    }

    pub(in crate::db::query::intent) fn build_plan_model_with_indexes_from_normalized_predicate(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
        normalized_predicate: Option<Predicate>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        // Phase 1: schema surface and intent validation.
        let schema_info = SchemaInfo::cached_for_entity_model(self.model);
        let access_inputs = self.intent.planning_access_inputs();

        // Phase 2: reuse the caller-provided normalized predicate so cache-key
        // construction and planner misses share the same canonical predicate.
        let access_plan_value = self.plan_access_from_normalized_predicate(
            visible_indexes,
            schema_info,
            normalized_predicate.as_ref(),
            access_inputs.order(),
            access_inputs.into_key_access_override(),
        )?;
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

        // Phase 4: freeze the planner-owned route profile before validation so
        // policy gates that depend on finalized access/order contracts, such as
        // expression ORDER BY support, see the accepted route semantics.
        plan.finalize_planner_route_profile_for_model(self.model);

        // Phase 5: validate the assembled plan against schema, access-shape,
        // and planner-policy contracts before projecting explain metadata.
        self.validate_plan_semantics(schema_info, &plan)?;

        // Phase 6: freeze planner-owned execution metadata only after semantic
        // validation succeeds so user-facing projection/order errors remain
        // planner-domain failures instead of executor invariant violations.
        plan.finalize_static_planning_shape_for_model(self.model)
            .map_err(QueryError::execute)?;

        Ok(plan)
    }

    pub(in crate::db::query::intent) fn prepare_normalized_scalar_predicate(
        &self,
    ) -> Result<Option<Predicate>, QueryError> {
        // Phase 1: validate query-intent policy shape before any cache or
        // planner work so compile attribution keeps policy failures honest.
        let schema_info = SchemaInfo::cached_for_entity_model(self.model);
        self.intent.validate_policy_shape()?;

        // Phase 2: normalize scalar predicate and fold constant predicates
        // before the lower cache key or planner consumes it.
        let access_inputs = self.intent.planning_access_inputs();

        Ok(fold_constant_predicate(normalize_query_predicate(
            schema_info,
            access_inputs.predicate(),
        )?))
    }

    // Reuse the caller-provided normalized predicate to choose one access path
    // without recomputing planner inputs or scattering the empty-window gates.
    fn plan_access_from_normalized_predicate(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
        schema_info: &SchemaInfo,
        normalized_predicate: Option<&Predicate>,
        order: Option<&OrderSpec>,
        key_access_override: Option<AccessPlan<Value>>,
    ) -> Result<AccessPlan<Value>, QueryError> {
        let limit_zero_window = is_limit_zero_load_window(self.intent.mode());
        let constant_false_predicate = predicate_is_constant_false(normalized_predicate);
        if limit_zero_window || constant_false_predicate {
            return Ok(AccessPlan::by_keys(Vec::new()));
        }

        plan_query_access(
            self.model,
            visible_indexes.as_slice(),
            schema_info,
            normalized_predicate,
            order,
            key_access_override,
        )
        .map_err(QueryError::from)
    }

    // Keep grouped and scalar semantic validation behind one owner-local gate
    // so planner handoff code does not duplicate the route-shape branch.
    fn validate_plan_semantics(
        &self,
        schema_info: &SchemaInfo,
        plan: &AccessPlannedQuery,
    ) -> Result<(), QueryError> {
        if plan.grouped_plan().is_some() {
            validate_group_query_semantics(schema_info, self.model, plan)?;
        } else {
            validate_query_semantics(schema_info, self.model, plan)?;
        }

        Ok(())
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

    if ExactPrimaryKeyAccess::from_access(access)
        .is_some_and(|access| access.matches_predicate(&predicate, model.primary_key.name))
    {
        return None;
    }

    Some(predicate)
}

///
/// ExactPrimaryKeyAccess
///
/// Local exact-primary-key access shape used by query intent planning to
/// decide whether one normalized predicate is already guaranteed by the chosen
/// authoritative access path.
///

enum ExactPrimaryKeyAccess<'a> {
    ByKey(&'a Value),
    ByKeys(&'a [Value]),
    HalfOpenRange { start: &'a Value, end: &'a Value },
}

impl<'a> ExactPrimaryKeyAccess<'a> {
    // Project one planner access path into the exact primary-key shapes that
    // can make a normalized predicate redundant.
    fn from_access(access: &'a AccessPlan<Value>) -> Option<Self> {
        if let Some(access_keys) = access.as_path().and_then(|path| path.as_by_keys())
            && !access_keys.is_empty()
        {
            return Some(Self::ByKeys(access_keys));
        }
        if let Some(access_key) = access.as_path().and_then(|path| path.as_by_key()) {
            return Some(Self::ByKey(access_key));
        }

        access
            .as_primary_key_range_path()
            .map(|(start, end)| Self::HalfOpenRange { start, end })
    }

    // Return whether one normalized predicate is exactly the same primary-key
    // contract already guaranteed by this authoritative access path.
    fn matches_predicate(self, predicate: &Predicate, primary_key_name: &str) -> bool {
        match self {
            Self::ByKey(access_key) => {
                matches_primary_key_eq_predicate(predicate, primary_key_name, access_key)
            }
            Self::ByKeys(access_keys) => {
                matches_primary_key_in_predicate(predicate, primary_key_name, access_keys)
            }
            Self::HalfOpenRange { start, end } => {
                matches_primary_key_half_open_range(predicate, primary_key_name, start, end)
            }
        }
    }
}

// Return whether one normalized predicate is exactly the same primary-key
// equality already guaranteed by one canonical `ByKey` access path.
fn matches_primary_key_eq_predicate(
    predicate: &Predicate,
    primary_key_name: &str,
    access_key: &Value,
) -> bool {
    let Predicate::Compare(cmp) = predicate else {
        return false;
    };
    cmp.field == primary_key_name && cmp.op == CompareOp::Eq && cmp.value == *access_key
}

// Return whether one normalized predicate is exactly the same primary-key IN
// set already guaranteed by one canonical `ByKeys` access path.
fn matches_primary_key_in_predicate(
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
fn matches_primary_key_half_open_range(
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
