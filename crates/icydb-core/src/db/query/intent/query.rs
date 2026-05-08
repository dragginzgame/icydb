//! Module: query::intent::query
//! Responsibility: typed query-intent construction and planner handoff for entity queries.
//! Does not own: runtime execution semantics or access-path execution behavior.
//! Boundary: exposes query APIs and emits planner-owned compiled query contracts.

#[cfg(feature = "sql")]
use crate::db::query::plan::expr::ProjectionSelection;
use crate::{
    db::{
        predicate::{CompareOp, MissingRowPolicy, Predicate},
        query::{
            builder::AggregateExpr,
            explain::ExplainPlan,
            expr::FilterExpr,
            expr::OrderTerm as FluentOrderTerm,
            intent::{QueryError, QueryModel},
            plan::{
                AccessPlannedQuery, LoadSpec, OrderSpec, PreparedScalarPlanningState, QueryMode,
                VisibleIndexes, expr::Expr,
            },
        },
        schema::SchemaInfo,
    },
    traits::{EntityKind, KeyValueCodec, SingletonEntity},
    value::{InputValue, Value},
};
use core::marker::PhantomData;

///
/// StructuralQuery
///
/// Generic-free query-intent core shared by typed `Query<E>` wrappers.
/// Stores model-level key access as `Value` so only typed key-entry helpers
/// remain entity-specific at the outer API boundary.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct StructuralQuery {
    intent: QueryModel<'static, Value>,
}

impl StructuralQuery {
    #[must_use]
    pub(in crate::db) const fn new(
        model: &'static crate::model::entity::EntityModel,
        consistency: MissingRowPolicy,
    ) -> Self {
        Self {
            intent: QueryModel::new(model, consistency),
        }
    }

    // Rewrap one updated generic-free intent model back into the structural
    // query shell so local transformation helpers do not rebuild `Self`
    // ad hoc at each boundary method.
    const fn from_intent(intent: QueryModel<'static, Value>) -> Self {
        Self { intent }
    }

    // Apply one infallible intent transformation while preserving the
    // structural query shell at this boundary.
    fn map_intent(
        self,
        map: impl FnOnce(QueryModel<'static, Value>) -> QueryModel<'static, Value>,
    ) -> Self {
        Self::from_intent(map(self.intent))
    }

    // Apply one fallible intent transformation while keeping result wrapping
    // local to the structural query boundary.
    fn try_map_intent(
        self,
        map: impl FnOnce(QueryModel<'static, Value>) -> Result<QueryModel<'static, Value>, QueryError>,
    ) -> Result<Self, QueryError> {
        map(self.intent).map(Self::from_intent)
    }

    #[must_use]
    const fn mode(&self) -> QueryMode {
        self.intent.mode()
    }

    #[must_use]
    fn has_explicit_order(&self) -> bool {
        self.intent.has_explicit_order()
    }

    #[must_use]
    pub(in crate::db) const fn has_grouping(&self) -> bool {
        self.intent.has_grouping()
    }

    #[must_use]
    const fn load_spec(&self) -> Option<LoadSpec> {
        match self.intent.mode() {
            QueryMode::Load(spec) => Some(spec),
            QueryMode::Delete(_) => None,
        }
    }

    #[must_use]
    pub(in crate::db) fn filter_predicate(mut self, predicate: Predicate) -> Self {
        self.intent = self.intent.filter_predicate(predicate);
        self
    }

    #[must_use]
    pub(in crate::db) fn filter(mut self, expr: impl Into<FilterExpr>) -> Self {
        self.intent = self.intent.filter(expr.into());
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

    #[cfg(all(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db) fn select_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.intent = self.intent.select_fields(fields);
        self
    }

    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db) fn projection_selection(mut self, selection: ProjectionSelection) -> Self {
        self.intent = self.intent.projection_selection(selection);
        self
    }

    /// Select one scalar field projection by canonical field id.
    ///
    /// This keeps SQL mutation execution from reconstructing projection shape
    /// variants after lowering has already selected the mutation target query.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db) fn select_field_id(mut self, field: impl Into<String>) -> Self {
        self.intent = self.intent.select_field_id(field);
        self
    }

    pub(in crate::db) fn group_by(self, field: impl AsRef<str>) -> Result<Self, QueryError> {
        self.try_map_intent(|intent| intent.push_group_field(field.as_ref()))
    }

    #[must_use]
    pub(in crate::db) fn aggregate(mut self, aggregate: AggregateExpr) -> Self {
        self.intent = self.intent.push_group_aggregate(aggregate);
        self
    }

    #[must_use]
    fn grouped_limits(mut self, max_groups: u64, max_group_bytes: u64) -> Self {
        self.intent = self.intent.grouped_limits(max_groups, max_group_bytes);
        self
    }

    pub(in crate::db) fn having_group(
        self,
        field: impl AsRef<str>,
        op: CompareOp,
        value: Value,
    ) -> Result<Self, QueryError> {
        let field = field.as_ref().to_owned();
        self.try_map_intent(|intent| intent.push_having_group_clause(&field, op, value))
    }

    pub(in crate::db) fn having_aggregate(
        self,
        aggregate_index: usize,
        op: CompareOp,
        value: Value,
    ) -> Result<Self, QueryError> {
        self.try_map_intent(|intent| {
            intent.push_having_aggregate_clause(aggregate_index, op, value)
        })
    }

    #[cfg(test)]
    pub(in crate::db) fn having_expr(self, expr: Expr) -> Result<Self, QueryError> {
        self.try_map_intent(|intent| intent.push_having_expr(expr))
    }

    pub(in crate::db) fn having_expr_preserving_shape(
        self,
        expr: Expr,
    ) -> Result<Self, QueryError> {
        self.try_map_intent(|intent| intent.push_having_expr_preserving_shape(expr))
    }

    #[must_use]
    fn by_id(self, id: Value) -> Self {
        self.map_intent(|intent| intent.by_id(id))
    }

    #[must_use]
    fn by_ids<I>(self, ids: I) -> Self
    where
        I: IntoIterator<Item = Value>,
    {
        self.map_intent(|intent| intent.by_ids(ids))
    }

    #[must_use]
    fn only(self, id: Value) -> Self {
        self.map_intent(|intent| intent.only(id))
    }

    #[must_use]
    pub(in crate::db) fn delete(mut self) -> Self {
        self.intent = self.intent.delete();
        self
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

    pub(in crate::db) fn build_plan(&self) -> Result<AccessPlannedQuery, QueryError> {
        self.intent.build_plan_model()
    }

    pub(in crate::db) fn build_plan_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        self.intent.build_plan_model_with_indexes(visible_indexes)
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
        visible_indexes: &VisibleIndexes<'_>,
        planning_state: PreparedScalarPlanningState<'_>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        self.intent
            .build_plan_model_with_indexes_from_scalar_planning_state(
                visible_indexes,
                planning_state,
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
    pub(in crate::db) fn trivial_scalar_load_fast_path_eligible(&self) -> bool {
        self.intent.trivial_scalar_load_fast_path_eligible()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn structural_cache_key(
        &self,
    ) -> crate::db::query::intent::StructuralQueryCacheKey {
        crate::db::query::intent::StructuralQueryCacheKey::from_query_model(&self.intent)
    }

    #[must_use]
    pub(in crate::db) fn structural_cache_key_with_normalized_predicate_fingerprint(
        &self,
        predicate_fingerprint: Option<[u8; 32]>,
    ) -> crate::db::query::intent::StructuralQueryCacheKey {
        self.intent
            .structural_cache_key_with_normalized_predicate_fingerprint(predicate_fingerprint)
    }

    // Build one access plan using either schema-owned indexes or the session
    // visibility slice already resolved at the caller boundary.
    fn build_plan_for_visibility(
        &self,
        visible_indexes: Option<&VisibleIndexes<'_>>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        match visible_indexes {
            Some(visible_indexes) => self.build_plan_with_visible_indexes(visible_indexes),
            None => self.build_plan(),
        }
    }

    #[must_use]
    pub(in crate::db) const fn model(&self) -> &'static crate::model::entity::EntityModel {
        self.intent.model()
    }
}

///
/// QueryPlanHandle
///
/// QueryPlanHandle stores the neutral access-planned query owned by the query
/// layer. Executor-specific prepared-plan caching remains outside this DTO, so
/// query values do not depend on executor runtime contracts.
///

#[derive(Clone, Debug)]
struct QueryPlanHandle {
    plan: Box<AccessPlannedQuery>,
}

impl QueryPlanHandle {
    #[must_use]
    fn from_plan(plan: AccessPlannedQuery) -> Self {
        Self {
            plan: Box::new(plan),
        }
    }

    #[must_use]
    const fn logical_plan(&self) -> &AccessPlannedQuery {
        &self.plan
    }

    #[must_use]
    #[cfg(test)]
    fn into_inner(self) -> AccessPlannedQuery {
        *self.plan
    }
}

///
/// PlannedQuery
///
/// PlannedQuery keeps the typed planning surface stable while allowing the
/// session boundary to reuse one shared prepared-plan artifact internally.
///

#[derive(Debug)]
pub struct PlannedQuery<E: EntityKind> {
    plan: QueryPlanHandle,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> PlannedQuery<E> {
    #[must_use]
    pub(in crate::db) fn from_plan(plan: AccessPlannedQuery) -> Self {
        Self {
            plan: QueryPlanHandle::from_plan(plan),
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub fn explain(&self) -> ExplainPlan {
        self.plan.logical_plan().explain()
    }

    /// Return the stable plan hash for this planned query.
    #[must_use]
    pub fn plan_hash_hex(&self) -> String {
        self.plan.logical_plan().fingerprint().to_string()
    }
}

///
/// CompiledQuery
///
/// Typed compiled-query shell over one structural planner contract.
/// The outer entity marker preserves executor handoff inference without
/// carrying a second adapter object, while session-owned paths can still reuse
/// the cached shared prepared plan directly.
///

#[derive(Clone, Debug)]
pub struct CompiledQuery<E: EntityKind> {
    plan: QueryPlanHandle,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> CompiledQuery<E> {
    #[must_use]
    pub(in crate::db) fn from_plan(plan: AccessPlannedQuery) -> Self {
        Self {
            plan: QueryPlanHandle::from_plan(plan),
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub fn explain(&self) -> ExplainPlan {
        self.plan.logical_plan().explain()
    }

    /// Return the stable plan hash for this compiled query.
    #[must_use]
    pub fn plan_hash_hex(&self) -> String {
        self.plan.logical_plan().fingerprint().to_string()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn projection_spec(&self) -> crate::db::query::plan::expr::ProjectionSpec {
        self.plan.logical_plan().projection_spec(E::MODEL)
    }

    /// Convert one compiled query back into the neutral planned-query contract.
    #[cfg(test)]
    pub(in crate::db) fn into_plan(self) -> AccessPlannedQuery {
        self.plan.into_inner()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn into_inner(self) -> AccessPlannedQuery {
        self.plan.into_inner()
    }
}

///
/// Query
///
/// Typed, declarative query intent for a specific entity type.
///
/// This intent is:
/// - schema-agnostic at construction
/// - normalized and validated only during planning
/// - free of access-path decisions
///

#[derive(Debug)]
pub struct Query<E: EntityKind> {
    inner: StructuralQuery,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> Query<E> {
    // Rebind one structural query core to the typed `Query<E>` surface.
    pub(in crate::db) const fn from_inner(inner: StructuralQuery) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    /// Create a new intent with an explicit missing-row policy.
    /// Ignore favors idempotency and may mask index/data divergence on deletes.
    /// Use Error to surface missing rows during scan/delete execution.
    #[must_use]
    pub const fn new(consistency: MissingRowPolicy) -> Self {
        Self::from_inner(StructuralQuery::new(E::MODEL, consistency))
    }

    /// Return the intent mode (load vs delete).
    #[must_use]
    pub const fn mode(&self) -> QueryMode {
        self.inner.mode()
    }

    #[cfg(test)]
    pub(in crate::db) fn explain_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<ExplainPlan, QueryError> {
        let plan = self.build_plan_for_visibility(Some(visible_indexes))?;

        Ok(plan.explain())
    }

    #[cfg(test)]
    pub(in crate::db) fn plan_hash_hex_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<String, QueryError> {
        let plan = self.build_plan_for_visibility(Some(visible_indexes))?;

        Ok(plan.fingerprint().to_string())
    }

    // Build one typed access plan using either schema-owned indexes or the
    // visibility slice already resolved at the session boundary.
    fn build_plan_for_visibility(
        &self,
        visible_indexes: Option<&VisibleIndexes<'_>>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        self.inner.build_plan_for_visibility(visible_indexes)
    }

    // Build one structural plan for the requested visibility lane and then
    // project it into one typed query-owned contract so planned vs compiled
    // outputs do not each duplicate the same plan handoff shape.
    fn map_plan_for_visibility<T>(
        &self,
        visible_indexes: Option<&VisibleIndexes<'_>>,
        map: impl FnOnce(AccessPlannedQuery) -> T,
    ) -> Result<T, QueryError> {
        let plan = self.build_plan_for_visibility(visible_indexes)?;

        Ok(map(plan))
    }

    // Wrap one built plan as the typed planned-query DTO.
    pub(in crate::db) fn planned_query_from_plan(plan: AccessPlannedQuery) -> PlannedQuery<E> {
        PlannedQuery::from_plan(plan)
    }

    // Wrap one built plan as the typed compiled-query DTO.
    pub(in crate::db) fn compiled_query_from_plan(plan: AccessPlannedQuery) -> CompiledQuery<E> {
        CompiledQuery::from_plan(plan)
    }

    #[must_use]
    pub(in crate::db::query) fn has_explicit_order(&self) -> bool {
        self.inner.has_explicit_order()
    }

    #[must_use]
    pub(in crate::db) const fn structural(&self) -> &StructuralQuery {
        &self.inner
    }

    #[must_use]
    pub const fn has_grouping(&self) -> bool {
        self.inner.has_grouping()
    }

    #[must_use]
    pub(in crate::db::query) const fn load_spec(&self) -> Option<LoadSpec> {
        self.inner.load_spec()
    }

    /// Add one typed filter expression, implicitly AND-ing with any existing filter.
    #[must_use]
    pub fn filter(mut self, expr: impl Into<FilterExpr>) -> Self {
        self.inner = self.inner.filter(expr);
        self
    }

    // Keep the internal fluent parity hook available for tests that need one
    // exact expression-owned scalar filter shape instead of the public typed
    // `FilterExpr` lowering path.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn filter_expr(mut self, expr: Expr) -> Self {
        self.inner = self.inner.filter_expr(expr);
        self
    }

    // Keep the internal predicate-owned filter hook available for convergence
    // tests without retaining the typed adapter in normal builds after SQL
    // UPDATE moved to structural lowering.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn filter_predicate(mut self, predicate: Predicate) -> Self {
        self.inner = self.inner.filter_predicate(predicate);
        self
    }

    /// Append one typed ORDER BY term.
    #[must_use]
    pub fn order_term(mut self, term: FluentOrderTerm) -> Self {
        self.inner = self.inner.order_term(term);
        self
    }

    /// Append multiple typed ORDER BY terms in declaration order.
    #[must_use]
    pub fn order_terms<I>(mut self, terms: I) -> Self
    where
        I: IntoIterator<Item = FluentOrderTerm>,
    {
        for term in terms {
            self.inner = self.inner.order_term(term);
        }

        self
    }

    /// Enable DISTINCT semantics for this query.
    #[must_use]
    pub fn distinct(mut self) -> Self {
        self.inner = self.inner.distinct();
        self
    }

    // Keep the internal fluent SQL parity hook available for lowering tests
    // without making generated SQL binding depend on the typed query shell.
    #[cfg(all(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db) fn select_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.inner = self.inner.select_fields(fields);
        self
    }

    /// Add one GROUP BY field.
    pub fn group_by(self, field: impl AsRef<str>) -> Result<Self, QueryError> {
        let Self { inner, .. } = self;
        let inner = inner.group_by(field)?;

        Ok(Self::from_inner(inner))
    }

    /// Add one aggregate terminal via composable aggregate expression.
    #[must_use]
    pub fn aggregate(mut self, aggregate: AggregateExpr) -> Self {
        self.inner = self.inner.aggregate(aggregate);
        self
    }

    /// Override grouped hard limits for grouped execution budget enforcement.
    #[must_use]
    pub fn grouped_limits(mut self, max_groups: u64, max_group_bytes: u64) -> Self {
        self.inner = self.inner.grouped_limits(max_groups, max_group_bytes);
        self
    }

    /// Add one grouped HAVING compare clause over one grouped key field.
    pub fn having_group(
        self,
        field: impl AsRef<str>,
        op: CompareOp,
        value: InputValue,
    ) -> Result<Self, QueryError> {
        let Self { inner, .. } = self;
        let inner = inner.having_group(field, op, value.into())?;

        Ok(Self::from_inner(inner))
    }

    /// Add one grouped HAVING compare clause over one grouped aggregate output.
    pub fn having_aggregate(
        self,
        aggregate_index: usize,
        op: CompareOp,
        value: InputValue,
    ) -> Result<Self, QueryError> {
        let Self { inner, .. } = self;
        let inner = inner.having_aggregate(aggregate_index, op, value.into())?;

        Ok(Self::from_inner(inner))
    }

    // Keep the internal fluent parity hook available for tests that need one
    // exact grouped HAVING expression shape instead of the public grouped
    // clause builders.
    #[cfg(test)]
    pub(in crate::db) fn having_expr(self, expr: Expr) -> Result<Self, QueryError> {
        let Self { inner, .. } = self;
        let inner = inner.having_expr(expr)?;

        Ok(Self::from_inner(inner))
    }

    /// Set the access path to a single primary key lookup.
    pub(in crate::db) fn by_id(self, id: E::Key) -> Self {
        let Self { inner, .. } = self;

        Self::from_inner(inner.by_id(id.to_key_value()))
    }

    /// Set the access path to a primary key batch lookup.
    pub(in crate::db) fn by_ids<I>(self, ids: I) -> Self
    where
        I: IntoIterator<Item = E::Key>,
    {
        let Self { inner, .. } = self;

        Self::from_inner(inner.by_ids(ids.into_iter().map(|id| id.to_key_value())))
    }

    /// Mark this intent as a delete query.
    #[must_use]
    pub fn delete(mut self) -> Self {
        self.inner = self.inner.delete();
        self
    }

    /// Apply a limit to the current mode.
    ///
    /// Load limits bound result size; delete limits bound mutation size.
    /// For scalar load queries, any use of `limit` or `offset` requires an
    /// explicit `order_term(...)` so pagination is deterministic.
    /// GROUP BY queries use canonical grouped-key order by default.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.inner = self.inner.limit(limit);
        self
    }

    /// Apply an offset to the current mode.
    ///
    /// Scalar load pagination requires an explicit `order_term(...)`.
    /// GROUP BY queries use canonical grouped-key order by default.
    /// Delete mode applies this after ordering and predicate filtering.
    #[must_use]
    pub fn offset(mut self, offset: u32) -> Self {
        self.inner = self.inner.offset(offset);
        self
    }

    /// Explain this intent without executing it.
    pub fn explain(&self) -> Result<ExplainPlan, QueryError> {
        let plan = self.planned()?;

        Ok(plan.explain())
    }

    /// Return a stable plan hash for this intent.
    ///
    /// The hash is derived from canonical planner contracts and is suitable
    /// for diagnostics, explain diffing, and cache key construction.
    pub fn plan_hash_hex(&self) -> Result<String, QueryError> {
        let plan = self.inner.build_plan()?;

        Ok(plan.fingerprint().to_string())
    }

    /// Plan this intent into a neutral planned query contract.
    pub fn planned(&self) -> Result<PlannedQuery<E>, QueryError> {
        self.map_plan_for_visibility(None, Self::planned_query_from_plan)
    }

    /// Compile this intent into query-owned handoff state.
    ///
    /// This boundary intentionally does not expose executor runtime shape.
    pub fn plan(&self) -> Result<CompiledQuery<E>, QueryError> {
        self.map_plan_for_visibility(None, Self::compiled_query_from_plan)
    }

    #[cfg(test)]
    pub(in crate::db) fn plan_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<CompiledQuery<E>, QueryError> {
        self.map_plan_for_visibility(Some(visible_indexes), Self::compiled_query_from_plan)
    }
}

impl<E> Query<E>
where
    E: EntityKind + SingletonEntity,
    E::Key: Default,
{
    /// Set the access path to the singleton primary key.
    pub(in crate::db) fn only(self) -> Self {
        let Self { inner, .. } = self;

        Self::from_inner(inner.only(E::Key::default().to_key_value()))
    }
}
