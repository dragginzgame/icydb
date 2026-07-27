//! Module: query::intent::model
//! Responsibility: query-intent model state and intent mutation helpers.
//! Does not own: planner phase orchestration, executor runtime behavior, or execution routing.
//! Boundary: stores entity-bound query intent consumed by the planner pipeline.

#[cfg(any(test, feature = "sql"))]
use crate::db::predicate::Predicate;
use crate::db::query::intent::{StructuralQueryCacheKey, state::GroupedIntent};
use crate::db::{
    predicate::MissingRowPolicy,
    query::{
        builder::aggregate::AggregateExpr,
        expr::{FilterExpr, OrderTerm as FluentOrderTerm},
        intent::{QueryError, QueryIntent},
        plan::{
            AccessPlannedQuery, AccessPlanningInputs, GroupAggregateSpec, LogicalPlanningInputs,
            OrderSpec, PreparedScalarPlanningState, QueryMode, VisibleIndexes,
            build_query_model_plan_with_indexes_from_scalar_planning_state,
            expr::{
                Expr, FieldId, ProjectionSelection, is_normalized_bool_expr, normalize_bool_expr,
            },
            prepare_query_model_scalar_planning_state_with_schema_info,
            resolve_group_field_slot_with_schema,
            try_build_trivial_scalar_load_plan_with_schema_info,
        },
    },
    schema::SchemaInfo,
};

///
/// QueryModel
///
/// Generic-free query intent and planning context.
///
/// Application-model metadata is consumed only by typed outer adapters while
/// accepted schema owns runtime planning.
///

#[derive(Clone, Debug)]
pub(in crate::db::query) struct QueryModel {
    intent: QueryIntent,
    consistency: MissingRowPolicy,
}

impl QueryModel {
    #[must_use]
    pub(in crate::db::query) const fn new(consistency: MissingRowPolicy) -> Self {
        Self {
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

    // Return the normalized predicate only when this complete query intent can
    // use direct COUNT prefix cardinality without changing visible semantics.
    #[cfg(feature = "sql")]
    pub(in crate::db::query) fn direct_count_cardinality_prefix_predicate(
        &self,
    ) -> Result<Option<&Predicate>, QueryError> {
        self.validate_policy_shape()?;

        let access_inputs = self.planning_access_inputs();
        let logical_inputs = self.planning_logical_inputs();
        let scalar_shape_supported = access_inputs.order().is_none()
            && !logical_inputs.distinct()
            && !logical_inputs.has_group()
            && !logical_inputs.has_having_expr();
        let visible_filter_fully_covered =
            !logical_inputs.has_filter_expr() || logical_inputs.filter_predicate_covers_expr();
        let unbounded_load_window = matches!(
            self.mode(),
            QueryMode::Load(load_spec)
                if load_spec.limit().is_none() && load_spec.offset() == 0
        );
        if !scalar_shape_supported || !visible_filter_fully_covered || !unbounded_load_window {
            return Ok(None);
        }

        Ok(access_inputs.predicate())
    }

    #[must_use]
    pub(in crate::db::query) const fn scalar_projection_selection(&self) -> &ProjectionSelection {
        &self.intent.scalar().projection_selection
    }

    #[must_use]
    pub(in crate::db::query) fn trivial_scalar_load_fast_path_eligible_with_schema(
        &self,
        schema_info: &SchemaInfo,
    ) -> bool {
        let QueryMode::Load(load_spec) = self.intent.mode() else {
            return false;
        };
        let scalar = self.intent.scalar();
        if scalar.filter.is_some()
            || scalar.distinct
            || self.intent.is_grouped()
            || !matches!(scalar.projection_selection, ProjectionSelection::All)
        {
            return false;
        }

        let Some(order) = scalar.order.as_ref() else {
            return load_spec.limit().is_none() && load_spec.offset() == 0;
        };

        let primary_key_names: Vec<&str> = schema_info
            .primary_key_names()
            .iter()
            .map(String::as_str)
            .collect();
        order
            .primary_key_only_direction_fields(primary_key_names.as_slice())
            .is_some()
    }

    #[must_use]
    pub(in crate::db::query) const fn scalar_order_for_trivial_fast_path(
        &self,
    ) -> Option<&OrderSpec> {
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
    pub(in crate::db::query::intent) const fn has_grouping(&self) -> bool {
        self.intent.is_grouped()
    }

    #[must_use]
    pub(in crate::db::query) const fn has_scalar_filter(&self) -> bool {
        self.intent.scalar().filter.is_some()
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn scalar_intent_for_cache_key(
        &self,
    ) -> &crate::db::query::intent::state::ScalarIntent {
        self.intent.scalar()
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn grouped_intent_for_cache_key(
        &self,
    ) -> Option<&GroupedIntent> {
        self.intent.grouped()
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn consistency_for_cache_key(&self) -> MissingRowPolicy {
        self.consistency
    }

    /// Append one predicate that has already been normalized by the caller.
    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db::query) fn filter_normalized_predicate(
        mut self,
        predicate: Predicate,
    ) -> Self {
        self.intent.append_predicate(predicate);
        self
    }

    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db::query) fn filter_for_schema(
        self,
        schema: &SchemaInfo,
        expr: impl Into<FilterExpr>,
    ) -> Self {
        self.filter_expr(expr.into().lower_bool_expr_for_schema(schema))
    }

    #[must_use]
    pub(in crate::db::query) fn filter_expr(mut self, expr: Expr) -> Self {
        let expr = normalize_bool_expr(expr);

        debug_assert!(is_normalized_bool_expr(&expr));

        self.intent.append_filter_expr(expr);
        self
    }

    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) fn filter_expr_with_normalized_predicate(
        mut self,
        expr: Expr,
        predicate: Predicate,
    ) -> Self {
        debug_assert!(is_normalized_bool_expr(&expr));

        self.intent
            .append_filter_with_predicate_subset(expr, predicate);
        self
    }

    /// Append one typed fluent ORDER BY term.
    #[must_use]
    pub(in crate::db::query) fn order_term(mut self, term: FluentOrderTerm) -> Self {
        self.intent.push_order_term(term.lower());
        self
    }

    /// Set a fully-specified order spec (validated before reaching this boundary).
    #[cfg(any(test, feature = "sql"))]
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
    #[cfg(feature = "sql")]
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

    // Resolve one grouped field through an explicit schema view and append it
    // to the grouped spec in declaration order.
    pub(in crate::db::query::intent) fn push_group_field_with_schema(
        mut self,
        field: &str,
        schema: &SchemaInfo,
    ) -> Result<Self, QueryError> {
        let field_slot =
            resolve_group_field_slot_with_schema(schema, field).map_err(QueryError::from)?;
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

    // Append one grouped HAVING compare over one grouped key field using an
    // explicit schema view for slot authority.

    // Append one grouped HAVING compare over one grouped aggregate output.

    // Append one widened grouped HAVING expression after GROUP BY terminal declaration.

    // Append one widened grouped HAVING expression while preserving the
    // caller-owned grouped semantic shape instead of re-running grouped
    // searched-CASE canonicalization at append time.
    #[cfg(feature = "sql")]
    pub(in crate::db::query::intent) fn push_having_expr_preserving_shape(
        mut self,
        expr: Expr,
    ) -> Result<Self, QueryError> {
        self.intent
            .push_having_expr_preserving_shape(expr)
            .map_err(QueryError::intent)?;

        Ok(self)
    }

    /// Mark this intent as a delete query.
    #[must_use]
    pub(in crate::db::query) fn delete(mut self) -> Self {
        self.intent = self.intent.set_delete_mode();
        self
    }

    /// Re-express a delete target as a load selection for structural mutation staging.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db::query) fn into_load_selection(mut self) -> Self {
        self.intent = self.intent.into_load_selection();
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

    pub(in crate::db::query::intent) fn build_plan_model_with_indexes_from_scalar_planning_state(
        &self,
        visible_indexes: &VisibleIndexes,
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
