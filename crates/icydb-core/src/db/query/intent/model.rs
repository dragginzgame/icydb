//! Module: query::intent::model
//! Responsibility: query-intent model state and intent mutation helpers.
//! Does not own: planner phase orchestration, executor runtime behavior, or execution routing.
//! Boundary: stores entity-bound query intent consumed by the planner pipeline.

use crate::db::query::intent::{StructuralQueryCacheKey, state::GroupedIntent};
use crate::{
    db::{
        predicate::{CompareOp, MissingRowPolicy, Predicate, normalize},
        query::{
            builder::aggregate::AggregateExpr,
            expr::{FilterExpr, OrderTerm as FluentOrderTerm},
            intent::{QueryError, QueryIntent},
            plan::{
                AccessPlannedQuery, AccessPlanningInputs, GroupAggregateSpec,
                LogicalPlanningInputs, OrderSpec, PreparedScalarPlanningState, QueryMode,
                VisibleIndexes, build_query_model_plan_for_model_only,
                build_query_model_plan_with_indexes_for_model_only,
                build_query_model_plan_with_indexes_from_scalar_planning_state,
                canonicalize_grouped_having_numeric_literal_for_field_kind,
                expr::{
                    Expr, FieldId, ProjectionSelection, is_normalized_bool_expr,
                    normalize_bool_expr,
                },
                group_aggregate_spec_expr, grouped_having_compare_expr,
                prepare_query_model_scalar_planning_state_with_schema_info,
                resolve_group_field_slot, resolve_group_field_slot_with_schema,
                try_build_trivial_scalar_load_plan_with_schema_info,
            },
        },
        schema::SchemaInfo,
    },
    model::entity::EntityModel,
    traits::KeyValueCodec,
    value::Value,
};

///
/// QueryModel
///
/// Model-level query intent and planning context.
/// Consumes an `EntityModel` derived from typed entity definitions.
///

#[derive(Clone, Debug)]
pub(in crate::db::query) struct QueryModel<'m, K> {
    model: &'m EntityModel,
    intent: QueryIntent<K>,
    consistency: MissingRowPolicy,
}

impl<'m, K: KeyValueCodec> QueryModel<'m, K> {
    #[must_use]
    pub(in crate::db::query) const fn new(
        model: &'m EntityModel,
        consistency: MissingRowPolicy,
    ) -> Self {
        Self {
            model,
            intent: QueryIntent::new(),
            consistency,
        }
    }

    pub(in crate::db::query) fn structural_cache_key_with_normalized_predicate_fingerprint(
        &self,
        predicate_fingerprint: Option<[u8; 32]>,
    ) -> StructuralQueryCacheKey {
        StructuralQueryCacheKey::from_query_model_with_normalized_predicate_fingerprint(
            self,
            predicate_fingerprint,
        )
    }

    /// Return the intent mode (load vs delete).
    #[must_use]
    pub(in crate::db::query) const fn mode(&self) -> QueryMode {
        self.intent.mode()
    }

    #[must_use]
    pub(in crate::db::query) const fn model(&self) -> &'m EntityModel {
        self.model
    }

    #[must_use]
    pub(in crate::db::query) const fn consistency(&self) -> MissingRowPolicy {
        self.consistency
    }

    #[must_use]
    pub(in crate::db::query) fn planning_access_inputs(&self) -> AccessPlanningInputs<'_> {
        self.intent.planning_access_inputs()
    }

    #[must_use]
    pub(in crate::db::query) fn planning_logical_inputs(&self) -> LogicalPlanningInputs {
        self.intent.planning_logical_inputs()
    }

    #[must_use]
    pub(in crate::db::query) const fn scalar_projection_selection(&self) -> &ProjectionSelection {
        &self.intent.scalar().projection_selection
    }

    #[must_use]
    pub(in crate::db::query) fn trivial_scalar_load_fast_path_eligible(&self) -> bool {
        let QueryMode::Load(load_spec) = self.intent.mode() else {
            return false;
        };
        let scalar = self.intent.scalar();
        if scalar.filter.is_some()
            || scalar.key_access.is_some()
            || scalar.key_access_conflict
            || scalar.distinct
            || self.intent.is_grouped()
            || !matches!(scalar.projection_selection, ProjectionSelection::All)
        {
            return false;
        }

        let Some(order) = scalar.order.as_ref() else {
            return load_spec.limit().is_none() && load_spec.offset() == 0;
        };

        order
            .primary_key_only_direction(self.model.primary_key.name)
            .is_some()
    }

    #[must_use]
    pub(in crate::db::query) fn scalar_order_for_trivial_fast_path(&self) -> Option<&OrderSpec> {
        debug_assert!(
            self.trivial_scalar_load_fast_path_eligible(),
            "trivial scalar fast-path order should only be read after eligibility"
        );

        self.intent.scalar().order.as_ref()
    }

    #[must_use]
    pub(in crate::db::query) const fn is_grouped(&self) -> bool {
        self.intent.is_grouped()
    }

    pub(in crate::db::query) fn validate_policy_shape(&self) -> Result<(), QueryError> {
        self.intent
            .validate_policy_shape()
            .map_err(QueryError::from)
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
    ) -> &crate::db::query::intent::state::ScalarIntent<K> {
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
    pub(in crate::db::query) fn filter_predicate(mut self, predicate: Predicate) -> Self {
        self.intent.append_predicate(normalize(&predicate));
        self
    }

    #[must_use]
    pub(in crate::db::query) fn filter(self, expr: impl Into<FilterExpr>) -> Self {
        let model = self.model;

        self.filter_expr(expr.into().lower_bool_expr_for_model(model))
    }

    #[must_use]
    pub(in crate::db::query) fn filter_expr(mut self, expr: Expr) -> Self {
        let expr = normalize_bool_expr(expr);

        debug_assert!(is_normalized_bool_expr(&expr));

        self.intent.append_filter_expr(expr);
        self
    }

    #[must_use]
    pub(in crate::db) fn filter_expr_with_normalized_predicate(
        mut self,
        expr: Expr,
        predicate: Predicate,
    ) -> Self {
        debug_assert!(is_normalized_bool_expr(&expr));

        self.intent
            .append_filter_with_predicate_subset(expr, normalize(&predicate));
        self
    }

    /// Append one typed fluent ORDER BY term.
    #[must_use]
    pub(in crate::db::query) fn order_term(mut self, term: FluentOrderTerm) -> Self {
        self.intent.push_order_term(term.lower());
        self
    }

    /// Set a fully-specified order spec (validated before reaching this boundary).
    pub(in crate::db::query) fn order_spec(mut self, order: OrderSpec) -> Self {
        self.intent.set_order_spec(order);
        self
    }

    /// Enable DISTINCT semantics for this query intent.
    #[must_use]
    pub(in crate::db::query) const fn distinct(mut self) -> Self {
        self.intent.set_distinct();
        self
    }

    /// Select one explicit scalar field projection list for internal SQL and
    /// planning tests that compare fluent and structural query shapes.
    #[cfg(all(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db::query) fn select_fields<I, S>(mut self, fields: I) -> Self
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

    /// Select one scalar field projection by canonical field id.
    ///
    /// SQL mutation selectors use this to ask the query-intent owner for a
    /// primary-key-only projection without reconstructing projection-selection
    /// variants in the session execution layer.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db::query) fn select_field_id(mut self, field: impl Into<String>) -> Self {
        self.intent
            .set_projection_selection(ProjectionSelection::Fields(vec![FieldId::new(field)]));

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

    // Resolve one grouped field through an explicit schema view and append it
    // to the grouped spec in declaration order.
    pub(in crate::db::query::intent) fn push_group_field_with_schema(
        mut self,
        field: &str,
        schema: &SchemaInfo,
    ) -> Result<Self, QueryError> {
        let field_slot = resolve_group_field_slot_with_schema(self.model, schema, field)
            .map_err(QueryError::from)?;
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

    // Append one grouped HAVING compare over one grouped key field.
    pub(in crate::db::query::intent) fn push_having_group_clause(
        self,
        field: &str,
        op: CompareOp,
        value: Value,
    ) -> Result<Self, QueryError> {
        if matches!(self.intent.mode(), QueryMode::Delete(_)) {
            return self.push_having_expr(Expr::Literal(Value::Bool(true)));
        }

        let field_slot = resolve_group_field_slot(self.model, field).map_err(QueryError::from)?;
        let value =
            canonicalize_grouped_having_numeric_literal_for_field_kind(field_slot.kind(), &value)
                .unwrap_or(value);
        let expr =
            grouped_having_compare_expr(Expr::Field(FieldId::new(field_slot.field())), op, value);

        self.push_having_expr(expr)
    }

    // Append one grouped HAVING compare over one grouped key field using an
    // explicit schema view for slot authority.
    pub(in crate::db::query::intent) fn push_having_group_clause_with_schema(
        self,
        field: &str,
        schema: &SchemaInfo,
        op: CompareOp,
        value: Value,
    ) -> Result<Self, QueryError> {
        if matches!(self.intent.mode(), QueryMode::Delete(_)) {
            return self.push_having_expr(Expr::Literal(Value::Bool(true)));
        }

        let field_slot = resolve_group_field_slot_with_schema(self.model, schema, field)
            .map_err(QueryError::from)?;
        let value =
            canonicalize_grouped_having_numeric_literal_for_field_kind(field_slot.kind(), &value)
                .unwrap_or(value);
        let expr =
            grouped_having_compare_expr(Expr::Field(FieldId::new(field_slot.field())), op, value);

        self.push_having_expr(expr)
    }

    // Append one grouped HAVING compare over one grouped aggregate output.
    pub(in crate::db::query::intent) fn push_having_aggregate_clause(
        self,
        aggregate_index: usize,
        op: CompareOp,
        value: Value,
    ) -> Result<Self, QueryError> {
        if matches!(self.intent.mode(), QueryMode::Delete(_)) {
            return self.push_having_expr(Expr::Literal(Value::Bool(true)));
        }

        let Some(grouped) = self.intent.grouped() else {
            return Err(QueryError::intent(
                crate::db::query::intent::IntentError::having_requires_group_by(),
            ));
        };
        let Some(aggregate) = grouped.group.aggregates.get(aggregate_index) else {
            return Err(QueryError::intent(
                crate::db::query::intent::IntentError::having_references_unknown_aggregate(),
            ));
        };
        let expr = grouped_having_compare_expr(
            Expr::Aggregate(group_aggregate_spec_expr(aggregate)),
            op,
            value,
        );

        self.push_having_expr(expr)
    }

    // Append one widened grouped HAVING expression after GROUP BY terminal declaration.
    pub(in crate::db::query::intent) fn push_having_expr(
        mut self,
        expr: Expr,
    ) -> Result<Self, QueryError> {
        self.intent
            .push_having_expr(expr)
            .map_err(QueryError::intent)?;

        Ok(self)
    }

    // Append one widened grouped HAVING expression while preserving the
    // caller-owned grouped semantic shape instead of re-running grouped
    // searched-CASE canonicalization at append time.
    pub(in crate::db::query::intent) fn push_having_expr_preserving_shape(
        mut self,
        expr: Expr,
    ) -> Result<Self, QueryError> {
        self.intent
            .push_having_expr_preserving_shape(expr)
            .map_err(QueryError::intent)?;

        Ok(self)
    }

    /// Set the access path to a single primary key lookup.
    pub(in crate::db::query) fn by_id(mut self, id: K) -> Self {
        self.intent.set_by_id(id);
        self
    }

    /// Set the access path to a primary key batch lookup.
    pub(in crate::db::query) fn by_ids<I>(mut self, ids: I) -> Self
    where
        I: IntoIterator<Item = K>,
    {
        self.intent.set_by_ids(ids);
        self
    }

    /// Set the access path to the singleton primary key.
    pub(in crate::db::query) fn only(mut self, id: K) -> Self {
        self.intent.set_only(id);
        self
    }

    /// Mark this intent as a delete query.
    #[must_use]
    pub(in crate::db::query) fn delete(mut self) -> Self {
        self.intent = self.intent.set_delete_mode();
        self
    }

    /// Apply a limit to the current mode.
    ///
    /// Load limits bound result size; delete limits bound mutation size.
    #[must_use]
    pub(in crate::db::query) fn limit(mut self, limit: u32) -> Self {
        self.intent = self.intent.apply_limit(limit);
        self
    }

    /// Apply an offset to the current mode.
    ///
    /// Load mode uses this as a pagination offset. Delete mode uses this as an
    /// ordered delete window offset.
    #[must_use]
    pub(in crate::db::query) fn offset(mut self, offset: u32) -> Self {
        self.intent = self.intent.apply_offset(offset);
        self
    }

    /// Build a standalone model-only logical plan using Value-based access keys.
    #[inline(never)]
    pub(in crate::db::query::intent) fn build_plan_model(
        &self,
    ) -> Result<AccessPlannedQuery, QueryError> {
        build_query_model_plan_for_model_only(self)
    }

    /// Build a standalone model-only logical plan using one explicit
    /// planner-visible secondary-index set.
    #[inline(never)]
    pub(in crate::db::query::intent) fn build_plan_model_with_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        build_query_model_plan_with_indexes_for_model_only(self, visible_indexes)
    }

    pub(in crate::db::query::intent) fn build_plan_model_with_indexes_from_scalar_planning_state(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
        planning_state: PreparedScalarPlanningState<'_>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        build_query_model_plan_with_indexes_from_scalar_planning_state(
            self,
            visible_indexes,
            planning_state,
        )
    }

    pub(in crate::db::query::intent) fn try_build_trivial_scalar_load_plan_with_schema_info(
        &self,
        schema_info: SchemaInfo,
    ) -> Result<Option<AccessPlannedQuery>, QueryError> {
        try_build_trivial_scalar_load_plan_with_schema_info(self, schema_info)
    }

    pub(in crate::db::query::intent) fn prepare_scalar_planning_state_with_schema_info(
        &self,
        schema_info: SchemaInfo,
    ) -> Result<PreparedScalarPlanningState<'_>, QueryError> {
        prepare_query_model_scalar_planning_state_with_schema_info(self, schema_info)
    }
}
