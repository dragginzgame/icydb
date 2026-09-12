//! Module: query::intent::query
//! Responsibility: typed query-intent construction and planner handoff for entity queries.
//! Does not own: runtime execution semantics or access-path execution behavior.
//! Boundary: exposes query APIs and emits planner-owned compiled query contracts.

use crate::db::query::{expr::FilterExpr, plan::expr::ProjectionSelection};
use crate::db::{
    predicate::MissingRowPolicy,
    query::{
        intent::{QueryError, QueryModel},
        plan::{
            AccessPlannedQuery, GroupAggregateSpec, PreparedQueryParameterContract,
            PreparedScalarPlanningState, VisibleIndexes,
        },
        preparation::PreparationWork,
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

    // Every intent change discards memoized identity. A cloned or previously
    // planned query must never keep the key of its pre-edit shape.
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
    pub(in crate::db) fn filter_normalized_predicate(self, predicate: Predicate) -> Self {
        self.map_intent(|intent| intent.filter_normalized_predicate(predicate))
    }

    pub(in crate::db) fn filter_for_schema(
        self,
        schema: &SchemaInfo,
        expr: &FilterExpr,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        self.try_map_intent(|intent| intent.filter_for_schema(schema, expr, work))
    }

    pub(in crate::db) fn filter_expr_with_normalized_predicate(
        self,
        expr: Expr,
        predicate: Predicate,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        self.try_map_intent(|intent| {
            intent.filter_expr_with_normalized_predicate(expr, predicate, work)
        })
    }
    // Keep the exact expression-owned scalar filter lane available for
    // internal SQL lowering and parity callers that must preserve one planner
    // expression without routing through the public typed `FilterExpr` surface.
    pub(in crate::db) fn filter_expr(
        self,
        expr: Expr,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        self.try_map_intent(|intent| intent.filter_expr(expr, work))
    }

    #[must_use]
    pub(in crate::db) fn order_spec(self, order: OrderSpec) -> Self {
        self.map_intent(|intent| intent.order_spec(order))
    }

    #[must_use]
    pub(in crate::db) fn distinct(self) -> Self {
        self.map_intent(QueryModel::distinct)
    }

    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db) fn select_fields<I, S>(self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.map_intent(|intent| intent.select_fields(fields))
    }

    #[must_use]
    pub(in crate::db) fn projection_selection(self, selection: ProjectionSelection) -> Self {
        self.map_intent(|intent| intent.projection_selection(selection))
    }

    pub(in crate::db) fn group_fields_with_schema(
        self,
        fields: &[String],
        schema: &SchemaInfo,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        self.try_map_intent(|intent| intent.group_fields_with_schema(fields, schema, work))
    }

    #[must_use]
    pub(in crate::db) fn group_aggregates(self, aggregates: Vec<GroupAggregateSpec>) -> Self {
        self.map_intent(|intent| intent.group_aggregates(aggregates))
    }

    /// Set explicit hard limits for grouped execution.
    #[must_use]
    pub(in crate::db) fn grouped_limits(self, max_groups: u64, max_group_bytes: u64) -> Self {
        self.map_intent(|intent| intent.grouped_limits(max_groups, max_group_bytes))
    }

    pub(in crate::db) fn having_expr_preserving_shape(
        self,
        expr: Expr,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        self.try_map_intent(|intent| intent.push_having_expr_preserving_shape(expr, work))
    }

    #[must_use]
    pub(in crate::db) fn delete(self) -> Self {
        self.map_intent(QueryModel::delete)
    }

    /// Re-express a delete target as a load selection for structural mutation staging.
    #[must_use]
    pub(in crate::db) fn into_load_selection(self) -> Self {
        self.map_intent(QueryModel::into_load_selection)
    }

    #[must_use]
    pub(in crate::db) fn limit(self, limit: u32) -> Self {
        self.map_intent(|intent| intent.limit(limit))
    }

    #[must_use]
    pub(in crate::db) fn offset(self, offset: u32) -> Self {
        self.map_intent(|intent| intent.offset(offset))
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
        work: &PreparationWork<'_>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        self.intent
            .build_plan_model_with_indexes_from_scalar_planning_state(
                visible_indexes,
                planning_state,
                work,
            )
    }

    pub(in crate::db) fn build_plan_from_parameterized_template(
        &self,
        template_indexes: &[crate::db::access::SemanticIndexAccessContract],
        planning_state: PreparedScalarPlanningState<'_>,
        work: &PreparationWork<'_>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        self.intent.build_plan_model_from_parameterized_template(
            template_indexes,
            planning_state,
            work,
        )
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
        work: &PreparationWork<'_>,
    ) -> Result<Option<AccessPlannedQuery>, QueryError> {
        self.intent
            .try_build_trivial_scalar_load_plan_with_schema_info(schema_info, work)
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
