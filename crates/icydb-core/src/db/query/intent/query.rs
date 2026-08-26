//! Module: query::intent::query
//! Responsibility: typed query-intent construction and planner handoff for entity queries.
//! Does not own: runtime execution semantics or access-path execution behavior.
//! Boundary: exposes query APIs and emits planner-owned compiled query contracts.

use crate::db::query::{expr::FilterExpr, plan::expr::ProjectionSelection};
use crate::db::{
    predicate::MissingRowPolicy,
    query::{
        builder::AggregateExpr,
        expr::OrderTerm as FluentOrderTerm,
        intent::{QueryError, QueryModel},
        plan::{
            AccessPlannedQuery, PreparedQueryParameterContract, PreparedScalarPlanningState,
            VisibleIndexes,
        },
    },
    schema::SchemaInfo,
};
use crate::db::{
    predicate::Predicate,
    query::plan::{OrderSpec, expr::Expr},
};
use std::sync::OnceLock;

///
/// StructuralQuery
///
/// Generic-free query intent shared by SQL, structural, and typed frontends.
/// Stores the query semantics consumed by the accepted-schema planner.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct StructuralQuery {
    intent: QueryModel,
    structural_cache_key: OnceLock<crate::db::query::intent::StructuralQueryCacheKey>,
}

impl StructuralQuery {
    #[must_use]
    pub(in crate::db) const fn new(consistency: MissingRowPolicy) -> Self {
        Self {
            intent: QueryModel::new(consistency),
            structural_cache_key: OnceLock::new(),
        }
    }

    // Rewrap one updated generic-free intent model back into the structural
    // query shell so local transformation helpers do not rebuild `Self`
    // ad hoc at each boundary method.
    const fn from_intent(intent: QueryModel) -> Self {
        Self {
            intent,
            structural_cache_key: OnceLock::new(),
        }
    }

    // Apply one infallible intent transformation while preserving the
    // structural query shell at this boundary.
    fn map_intent(self, map: impl FnOnce(QueryModel) -> QueryModel) -> Self {
        let Self { intent, .. } = self;

        Self::from_intent(map(intent))
    }

    // Apply one fallible intent transformation while keeping result wrapping
    // local to the structural query boundary.
    fn try_map_intent(
        self,
        map: impl FnOnce(QueryModel) -> Result<QueryModel, QueryError>,
    ) -> Result<Self, QueryError> {
        let Self { intent, .. } = self;

        map(intent).map(Self::from_intent)
    }

    #[must_use]
    pub(in crate::db) const fn has_grouping(&self) -> bool {
        self.intent.has_grouping()
    }

    #[must_use]
    pub(in crate::db) const fn has_scalar_filter(&self) -> bool {
        self.intent.has_scalar_filter()
    }

    #[must_use]
    pub(in crate::db) fn scalar_filter_expr(&self) -> Option<&Expr> {
        self.intent
            .scalar_intent_for_cache_key()
            .filter
            .as_ref()
            .and_then(|filter| filter.logical_filter_expr())
    }

    #[must_use]
    pub(in crate::db) fn direct_count_cardinality_entity_candidate(&self) -> bool {
        self.intent.direct_count_cardinality_entity_candidate()
    }

    #[must_use]
    pub(in crate::db) fn direct_count_cardinality_candidate(&self) -> bool {
        self.intent.direct_count_cardinality_candidate()
    }

    /// Append one predicate that has already been normalized by the caller.
    #[must_use]
    pub(in crate::db) fn filter_normalized_predicate(mut self, predicate: Predicate) -> Self {
        self.intent = self.intent.filter_normalized_predicate(predicate);
        self
    }

    #[must_use]
    pub(in crate::db) fn filter_for_schema(
        mut self,
        schema: &SchemaInfo,
        expr: impl Into<FilterExpr>,
    ) -> Self {
        self.intent = self.intent.filter_for_schema(schema, expr);
        self
    }

    #[must_use]
    pub(in crate::db) fn filter_expr_with_normalized_predicate(
        mut self,
        expr: Expr,
        predicate: Predicate,
    ) -> Self {
        self.intent = self
            .intent
            .filter_expr_with_normalized_predicate(expr, predicate);
        self
    }
    pub(in crate::db) fn order_term(mut self, term: FluentOrderTerm) -> Self {
        self.intent = self.intent.order_term(term);
        self
    }

    // Keep the exact expression-owned scalar filter lane available for
    // internal SQL lowering and parity callers that must preserve one planner
    // expression without routing through the public typed `FilterExpr` surface.
    #[must_use]
    pub(in crate::db) fn filter_expr(mut self, expr: Expr) -> Self {
        self.intent = self.intent.filter_expr(expr);
        self
    }

    #[must_use]
    pub(in crate::db) fn order_spec(mut self, order: OrderSpec) -> Self {
        self.intent = self.intent.order_spec(order);
        self
    }

    #[must_use]
    pub(in crate::db) fn distinct(mut self) -> Self {
        self.intent = self.intent.distinct();
        self
    }

    #[must_use]
    pub(in crate::db) fn select_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.intent = self.intent.select_fields(fields);
        self
    }

    #[must_use]
    pub(in crate::db) fn projection_selection(mut self, selection: ProjectionSelection) -> Self {
        self.intent = self.intent.projection_selection(selection);
        self
    }

    pub(in crate::db) fn group_by_with_schema(
        self,
        field: impl AsRef<str>,
        schema: &SchemaInfo,
    ) -> Result<Self, QueryError> {
        self.try_map_intent(|intent| intent.push_group_field_with_schema(field.as_ref(), schema))
    }

    #[must_use]
    pub(in crate::db) fn aggregate(mut self, aggregate: AggregateExpr) -> Self {
        self.intent = self.intent.push_group_aggregate(aggregate);
        self
    }

    /// Set explicit hard limits for grouped execution.
    #[must_use]
    pub(in crate::db) fn grouped_limits(mut self, max_groups: u64, max_group_bytes: u64) -> Self {
        self.intent = self.intent.grouped_limits(max_groups, max_group_bytes);
        self
    }

    pub(in crate::db) fn having_expr_preserving_shape(
        self,
        expr: Expr,
    ) -> Result<Self, QueryError> {
        self.try_map_intent(|intent| intent.push_having_expr_preserving_shape(expr))
    }

    #[must_use]
    pub(in crate::db) fn delete(mut self) -> Self {
        self.intent = self.intent.delete();
        self
    }

    /// Re-express a delete target as a load selection for structural mutation staging.
    #[must_use]
    pub(in crate::db) fn into_load_selection(self) -> Self {
        self.map_intent(QueryModel::into_load_selection)
    }

    #[must_use]
    pub(in crate::db) fn limit(mut self, limit: u32) -> Self {
        self.intent = self.intent.limit(limit);
        self
    }

    #[must_use]
    pub(in crate::db) fn offset(mut self, offset: u32) -> Self {
        self.intent = self.intent.offset(offset);
        self
    }

    pub(in crate::db) fn prepare_scalar_planning_state_with_schema_info(
        &self,
        schema_info: SchemaInfo,
    ) -> Result<PreparedScalarPlanningState<'_>, QueryError> {
        self.intent
            .prepare_scalar_planning_state_with_schema_info(schema_info)
    }

    pub(in crate::db) fn build_plan_with_visible_indexes_from_scalar_planning_state(
        &self,
        visible_indexes: &VisibleIndexes,
        planning_state: PreparedScalarPlanningState<'_>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        self.intent
            .build_plan_model_with_indexes_from_scalar_planning_state(
                visible_indexes,
                planning_state,
            )
    }

    pub(in crate::db) fn build_plan_from_parameterized_template(
        &self,
        template_indexes: &[crate::db::access::SemanticIndexAccessContract],
        planning_state: PreparedScalarPlanningState<'_>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        self.intent
            .build_plan_model_from_parameterized_template(template_indexes, planning_state)
    }

    pub(in crate::db) fn try_build_count_cardinality_prefix_access_with_schema_info(
        &self,
        visible_indexes: &VisibleIndexes,
        schema_info: &SchemaInfo,
    ) -> Result<Option<crate::db::query::plan::CountCardinalityPrefixAccess<'_>>, QueryError> {
        crate::db::query::plan::try_build_count_cardinality_prefix_access_from_query_model(
            &self.intent,
            visible_indexes,
            schema_info,
        )
    }

    pub(in crate::db) fn try_build_trivial_scalar_load_plan_with_schema_info(
        &self,
        schema_info: SchemaInfo,
    ) -> Result<Option<AccessPlannedQuery>, QueryError> {
        self.intent
            .try_build_trivial_scalar_load_plan_with_schema_info(schema_info)
    }

    #[must_use]
    pub(in crate::db) fn trivial_scalar_load_fast_path_eligible_with_schema(
        &self,
        schema_info: &SchemaInfo,
    ) -> bool {
        self.intent
            .trivial_scalar_load_fast_path_eligible_with_schema(schema_info)
    }

    #[must_use]
    pub(in crate::db) fn structural_cache_key_with_normalized_predicate_fingerprint(
        &self,
        predicate_fingerprint: Option<[u8; 32]>,
    ) -> crate::db::query::intent::StructuralQueryCacheKey {
        if predicate_fingerprint.is_none() {
            return self
                .structural_cache_key
                .get_or_init(|| {
                    self.intent
                        .structural_cache_key_with_normalized_predicate_fingerprint(None)
                })
                .clone();
        }

        self.intent
            .structural_cache_key_with_normalized_predicate_fingerprint(predicate_fingerprint)
    }

    pub(in crate::db) fn structural_cache_key_with_parameter_contract(
        &self,
        parameter_contract: PreparedQueryParameterContract,
    ) -> crate::db::query::intent::StructuralQueryCacheKey {
        self.intent
            .structural_cache_key_with_parameter_contract(parameter_contract)
    }

    #[must_use]
    pub(in crate::db) fn filter_predicate_fully_covers_expression(&self) -> bool {
        self.intent.filter_predicate_fully_covers_expression()
    }
}
