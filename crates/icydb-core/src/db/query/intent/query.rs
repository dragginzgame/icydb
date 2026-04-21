//! Module: query::intent::query
//! Responsibility: typed query-intent construction and planner handoff for entity queries.
//! Does not own: runtime execution semantics or access-path execution behavior.
//! Boundary: exposes query APIs and emits planner-owned compiled query contracts.

#[cfg(feature = "sql")]
use crate::db::query::plan::expr::ProjectionSelection;
use crate::{
    db::{
        TraceReuseEvent,
        executor::{
            BytesByProjectionMode, PreparedExecutionPlan, SharedPreparedExecutionPlan,
            assemble_aggregate_terminal_execution_descriptor,
            assemble_load_execution_node_descriptor, assemble_load_execution_verbose_diagnostics,
            planning::route::AggregateRouteShape,
        },
        predicate::{CoercionId, CompareOp, MissingRowPolicy, Predicate},
        query::{
            builder::{
                AggregateExpr, PreparedFluentAggregateExplainStrategy,
                PreparedFluentProjectionStrategy,
            },
            explain::{
                ExplainAccessPath, ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor,
                ExplainExecutionNodeType, ExplainOrderPushdown, ExplainPlan, ExplainPredicate,
                FinalizedQueryDiagnostics,
            },
            expr::FilterExpr,
            expr::OrderTerm as FluentOrderTerm,
            intent::{
                QueryError,
                model::{PreparedScalarPlanningState, QueryModel},
            },
            plan::{
                AccessPlannedQuery, LoadSpec, OrderSpec, QueryMode, VisibleIndexes, expr::Expr,
            },
        },
    },
    traits::{EntityKind, EntityValue, FieldValue, SingletonEntity},
    value::Value,
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

    #[cfg(feature = "sql")]
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

    pub(in crate::db) fn having_expr(self, expr: Expr) -> Result<Self, QueryError> {
        self.try_map_intent(|intent| intent.push_having_expr(expr))
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

    pub(in crate::db) fn prepare_scalar_planning_state(
        &self,
    ) -> Result<PreparedScalarPlanningState<'_>, QueryError> {
        self.intent.prepare_scalar_planning_state()
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

    // Assemble one canonical execution descriptor from a previously built
    // access plan so text/json/verbose explain surfaces do not each rebuild it.
    fn explain_execution_descriptor_from_plan(
        &self,
        plan: &AccessPlannedQuery,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        assemble_load_execution_node_descriptor(
            self.intent.model().fields(),
            self.intent.model().primary_key().name(),
            plan,
        )
        .map_err(QueryError::execute)
    }

    // Render one verbose execution explain payload from a single access plan,
    // freezing one immutable diagnostics artifact instead of returning one
    // wrapper-owned line list that callers still have to extend locally.
    fn finalized_execution_diagnostics_from_plan(
        &self,
        plan: &AccessPlannedQuery,
        reuse: Option<TraceReuseEvent>,
    ) -> Result<FinalizedQueryDiagnostics, QueryError> {
        let descriptor = self.explain_execution_descriptor_from_plan(plan)?;
        let route_diagnostics = assemble_load_execution_verbose_diagnostics(
            self.intent.model().fields(),
            self.intent.model().primary_key().name(),
            plan,
        )
        .map_err(QueryError::execute)?;
        let explain = plan.explain();

        // Phase 1: add descriptor-stage summaries for key execution operators.
        let mut logical_diagnostics = Vec::new();
        logical_diagnostics.push(format!(
            "diag.d.has_top_n_seek={}",
            contains_execution_node_type(&descriptor, ExplainExecutionNodeType::TopNSeek)
        ));
        logical_diagnostics.push(format!(
            "diag.d.has_index_range_limit_pushdown={}",
            contains_execution_node_type(
                &descriptor,
                ExplainExecutionNodeType::IndexRangeLimitPushdown,
            )
        ));
        logical_diagnostics.push(format!(
            "diag.d.has_index_predicate_prefilter={}",
            contains_execution_node_type(
                &descriptor,
                ExplainExecutionNodeType::IndexPredicatePrefilter,
            )
        ));
        logical_diagnostics.push(format!(
            "diag.d.has_residual_filter={}",
            contains_execution_node_type(&descriptor, ExplainExecutionNodeType::ResidualFilter,)
        ));

        // Phase 2: append logical-plan diagnostics relevant to verbose explain.
        logical_diagnostics.push(format!("diag.p.mode={:?}", explain.mode()));
        logical_diagnostics.push(format!(
            "diag.p.order_pushdown={}",
            plan_order_pushdown_label(explain.order_pushdown())
        ));
        logical_diagnostics.push(format!(
            "diag.p.predicate_pushdown={}",
            plan_predicate_pushdown_label(explain.predicate(), explain.access())
        ));
        logical_diagnostics.push(format!("diag.p.distinct={}", explain.distinct()));
        logical_diagnostics.push(format!("diag.p.page={:?}", explain.page()));
        logical_diagnostics.push(format!("diag.p.consistency={:?}", explain.consistency()));

        Ok(FinalizedQueryDiagnostics::new(
            descriptor,
            route_diagnostics,
            logical_diagnostics,
            reuse,
        ))
    }

    // Freeze one immutable diagnostics artifact while still allowing one
    // caller-owned descriptor mutation before rendering.
    pub(in crate::db) fn finalized_execution_diagnostics_from_plan_with_descriptor_mutator(
        &self,
        plan: &AccessPlannedQuery,
        reuse: Option<TraceReuseEvent>,
        mutate_descriptor: impl FnOnce(&mut ExplainExecutionNodeDescriptor),
    ) -> Result<FinalizedQueryDiagnostics, QueryError> {
        let mut diagnostics = self.finalized_execution_diagnostics_from_plan(plan, reuse)?;
        mutate_descriptor(&mut diagnostics.execution);

        Ok(diagnostics)
    }

    // Render one verbose execution explain payload using only the canonical
    // diagnostics artifact owned by this query boundary.
    fn explain_execution_verbose_from_plan(
        &self,
        plan: &AccessPlannedQuery,
    ) -> Result<String, QueryError> {
        self.finalized_execution_diagnostics_from_plan(plan, None)
            .map(|diagnostics| diagnostics.render_text_verbose())
    }

    // Freeze one explain-only access-choice snapshot from the effective
    // planner-visible index slice before building descriptor diagnostics.
    fn finalize_explain_access_choice_for_visibility(
        &self,
        plan: &mut AccessPlannedQuery,
        visible_indexes: Option<&VisibleIndexes<'_>>,
    ) {
        let visible_indexes = match visible_indexes {
            Some(visible_indexes) => visible_indexes.as_slice(),
            None => self.intent.model().indexes(),
        };

        plan.finalize_access_choice_for_model_with_indexes(self.intent.model(), visible_indexes);
    }

    // Build one execution descriptor after resolving the caller-visible index
    // slice so text/json explain surfaces do not each duplicate plan assembly.
    fn explain_execution_descriptor_for_visibility(
        &self,
        visible_indexes: Option<&VisibleIndexes<'_>>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        let mut plan = self.build_plan_for_visibility(visible_indexes)?;
        self.finalize_explain_access_choice_for_visibility(&mut plan, visible_indexes);

        self.explain_execution_descriptor_from_plan(&plan)
    }

    // Render one verbose execution payload after resolving the caller-visible
    // index slice exactly once at the structural query boundary.
    fn explain_execution_verbose_for_visibility(
        &self,
        visible_indexes: Option<&VisibleIndexes<'_>>,
    ) -> Result<String, QueryError> {
        let mut plan = self.build_plan_for_visibility(visible_indexes)?;
        self.finalize_explain_access_choice_for_visibility(&mut plan, visible_indexes);

        self.explain_execution_verbose_from_plan(&plan)
    }

    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db) const fn model(&self) -> &'static crate::model::entity::EntityModel {
        self.intent.model()
    }

    #[inline(never)]
    pub(in crate::db) fn explain_execution_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        self.explain_execution_descriptor_for_visibility(Some(visible_indexes))
    }

    // Explain one load execution shape through the structural query core.
    #[inline(never)]
    pub(in crate::db) fn explain_execution(
        &self,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        self.explain_execution_descriptor_for_visibility(None)
    }

    // Render one verbose scalar load execution payload through the shared
    // structural descriptor and route-diagnostics paths.
    #[inline(never)]
    pub(in crate::db) fn explain_execution_verbose(&self) -> Result<String, QueryError> {
        self.explain_execution_verbose_for_visibility(None)
    }

    #[inline(never)]
    pub(in crate::db) fn explain_execution_verbose_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<String, QueryError> {
        self.explain_execution_verbose_for_visibility(Some(visible_indexes))
    }

    #[inline(never)]
    pub(in crate::db) fn explain_aggregate_terminal_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
        aggregate: AggregateRouteShape<'_>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError> {
        let plan = self.build_plan_with_visible_indexes(visible_indexes)?;
        let query_explain = plan.explain();
        let terminal = aggregate.kind();
        let execution = assemble_aggregate_terminal_execution_descriptor(&plan, aggregate);

        Ok(ExplainAggregateTerminalPlan::new(
            query_explain,
            terminal,
            execution,
        ))
    }

    #[inline(never)]
    pub(in crate::db) fn explain_prepared_aggregate_terminal_with_visible_indexes<S>(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
        strategy: &S,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        S: PreparedFluentAggregateExplainStrategy,
    {
        let Some(kind) = strategy.explain_aggregate_kind() else {
            return Err(QueryError::invariant(
                "prepared fluent aggregate explain requires an explain-visible aggregate kind",
            ));
        };
        let aggregate = AggregateRouteShape::new_from_fields(
            kind,
            strategy.explain_projected_field(),
            self.intent.model().fields(),
            self.intent.model().primary_key().name(),
        );

        self.explain_aggregate_terminal_with_visible_indexes(visible_indexes, aggregate)
    }
}

///
/// QueryPlanHandle
///
/// QueryPlanHandle keeps typed query DTOs compatible with both direct planner
/// output and the shared prepared-plan cache boundary.
/// Session-owned paths can carry the prepared artifact directly, while direct
/// fluent builder calls can still wrap a raw logical plan without rebuilding.
///

#[derive(Clone, Debug)]
enum QueryPlanHandle {
    Plan(Box<AccessPlannedQuery>),
    Prepared(SharedPreparedExecutionPlan),
}

impl QueryPlanHandle {
    #[must_use]
    fn from_plan(plan: AccessPlannedQuery) -> Self {
        Self::Plan(Box::new(plan))
    }

    #[must_use]
    const fn from_prepared(prepared_plan: SharedPreparedExecutionPlan) -> Self {
        Self::Prepared(prepared_plan)
    }

    #[must_use]
    fn logical_plan(&self) -> &AccessPlannedQuery {
        match self {
            Self::Plan(plan) => plan,
            Self::Prepared(prepared_plan) => prepared_plan.logical_plan(),
        }
    }

    fn into_prepared_execution_plan<E: EntityKind>(self) -> PreparedExecutionPlan<E> {
        match self {
            Self::Plan(plan) => PreparedExecutionPlan::new(*plan),
            Self::Prepared(prepared_plan) => prepared_plan.typed_clone::<E>(),
        }
    }

    #[must_use]
    #[cfg(test)]
    fn into_inner(self) -> AccessPlannedQuery {
        match self {
            Self::Plan(plan) => *plan,
            Self::Prepared(prepared_plan) => prepared_plan.logical_plan().clone(),
        }
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
    fn from_plan(plan: AccessPlannedQuery) -> Self {
        Self {
            plan: QueryPlanHandle::from_plan(plan),
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub(in crate::db) const fn from_prepared_plan(
        prepared_plan: SharedPreparedExecutionPlan,
    ) -> Self {
        Self {
            plan: QueryPlanHandle::from_prepared(prepared_plan),
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
    fn from_plan(plan: AccessPlannedQuery) -> Self {
        Self {
            plan: QueryPlanHandle::from_plan(plan),
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub(in crate::db) const fn from_prepared_plan(
        prepared_plan: SharedPreparedExecutionPlan,
    ) -> Self {
        Self {
            plan: QueryPlanHandle::from_prepared(prepared_plan),
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

    /// Convert one structural compiled query into one prepared executor plan.
    pub(in crate::db) fn into_prepared_execution_plan(
        self,
    ) -> crate::db::executor::PreparedExecutionPlan<E> {
        self.plan.into_prepared_execution_plan::<E>()
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

    pub(in crate::db) fn explain_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<ExplainPlan, QueryError> {
        let plan = self.build_plan_for_visibility(Some(visible_indexes))?;

        Ok(plan.explain())
    }

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

    // Build one typed prepared execution plan directly from the requested
    // visibility lane so explain helpers that need executor-owned shape do not
    // rebuild that shell through `CompiledQuery<E>`.
    fn prepared_execution_plan_for_visibility(
        &self,
        visible_indexes: Option<&VisibleIndexes<'_>>,
    ) -> Result<PreparedExecutionPlan<E>, QueryError> {
        self.map_plan_for_visibility(visible_indexes, PreparedExecutionPlan::<E>::new)
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
    pub(crate) fn has_explicit_order(&self) -> bool {
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
    pub(crate) const fn load_spec(&self) -> Option<LoadSpec> {
        self.inner.load_spec()
    }

    /// Add one typed filter expression, implicitly AND-ing with any existing filter.
    #[must_use]
    pub fn filter(mut self, expr: impl Into<FilterExpr>) -> Self {
        self.inner = self.inner.filter(expr);
        self
    }

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
        value: Value,
    ) -> Result<Self, QueryError> {
        let Self { inner, .. } = self;
        let inner = inner.having_group(field, op, value)?;

        Ok(Self::from_inner(inner))
    }

    /// Add one grouped HAVING compare clause over one grouped aggregate output.
    pub fn having_aggregate(
        self,
        aggregate_index: usize,
        op: CompareOp,
        value: Value,
    ) -> Result<Self, QueryError> {
        let Self { inner, .. } = self;
        let inner = inner.having_aggregate(aggregate_index, op, value)?;

        Ok(Self::from_inner(inner))
    }

    /// Set the access path to a single primary key lookup.
    pub(crate) fn by_id(self, id: E::Key) -> Self {
        let Self { inner, .. } = self;

        Self::from_inner(inner.by_id(id.to_value()))
    }

    /// Set the access path to a primary key batch lookup.
    pub(crate) fn by_ids<I>(self, ids: I) -> Self
    where
        I: IntoIterator<Item = E::Key>,
    {
        let Self { inner, .. } = self;

        Self::from_inner(inner.by_ids(ids.into_iter().map(|id| id.to_value())))
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

    // Resolve the structural execution descriptor through either the default
    // schema-owned visibility lane or one caller-provided visible-index slice.
    fn explain_execution_descriptor_for_visibility(
        &self,
        visible_indexes: Option<&VisibleIndexes<'_>>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        match visible_indexes {
            Some(visible_indexes) => self
                .inner
                .explain_execution_with_visible_indexes(visible_indexes),
            None => self.inner.explain_execution(),
        }
    }

    // Render one descriptor-derived execution surface after resolving the
    // visibility slice once at the typed query boundary.
    fn render_execution_descriptor_for_visibility(
        &self,
        visible_indexes: Option<&VisibleIndexes<'_>>,
        render: impl FnOnce(ExplainExecutionNodeDescriptor) -> String,
    ) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        let descriptor = self.explain_execution_descriptor_for_visibility(visible_indexes)?;

        Ok(render(descriptor))
    }

    // Render one verbose execution explain payload after choosing the
    // appropriate structural visibility lane once.
    fn explain_execution_verbose_for_visibility(
        &self,
        visible_indexes: Option<&VisibleIndexes<'_>>,
    ) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        match visible_indexes {
            Some(visible_indexes) => self
                .inner
                .explain_execution_verbose_with_visible_indexes(visible_indexes),
            None => self.inner.explain_execution_verbose(),
        }
    }

    /// Explain executor-selected load execution shape without running it.
    pub fn explain_execution(&self) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.explain_execution_descriptor_for_visibility(None)
    }

    pub(in crate::db) fn explain_execution_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.explain_execution_descriptor_for_visibility(Some(visible_indexes))
    }

    /// Explain executor-selected load execution shape as deterministic text.
    pub fn explain_execution_text(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.render_execution_descriptor_for_visibility(None, |descriptor| {
            descriptor.render_text_tree()
        })
    }

    /// Explain executor-selected load execution shape as canonical JSON.
    pub fn explain_execution_json(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.render_execution_descriptor_for_visibility(None, |descriptor| {
            descriptor.render_json_canonical()
        })
    }

    /// Explain executor-selected load execution shape with route diagnostics.
    #[inline(never)]
    pub fn explain_execution_verbose(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.explain_execution_verbose_for_visibility(None)
    }

    // Build one aggregate-terminal explain payload without executing the query.
    #[cfg(test)]
    #[inline(never)]
    pub(in crate::db) fn explain_aggregate_terminal(
        &self,
        aggregate: AggregateExpr,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.inner.explain_aggregate_terminal_with_visible_indexes(
            &VisibleIndexes::schema_owned(E::MODEL.indexes()),
            AggregateRouteShape::new_from_fields(
                aggregate.kind(),
                aggregate.target_field(),
                E::MODEL.fields(),
                E::MODEL.primary_key().name(),
            ),
        )
    }

    pub(in crate::db) fn explain_prepared_aggregate_terminal_with_visible_indexes<S>(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
        strategy: &S,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
        S: PreparedFluentAggregateExplainStrategy,
    {
        self.inner
            .explain_prepared_aggregate_terminal_with_visible_indexes(visible_indexes, strategy)
    }

    pub(in crate::db) fn explain_bytes_by_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
        target_field: &str,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        let executable = self.prepared_execution_plan_for_visibility(Some(visible_indexes))?;
        let mut descriptor = executable
            .explain_load_execution_node_descriptor()
            .map_err(QueryError::execute)?;
        let projection_mode = executable.bytes_by_projection_mode(target_field);
        let projection_mode_label =
            PreparedExecutionPlan::<E>::bytes_by_projection_mode_label(projection_mode);

        descriptor
            .node_properties
            .insert("terminal", Value::from("bytes_by"));
        descriptor
            .node_properties
            .insert("terminal_field", Value::from(target_field.to_string()));
        descriptor.node_properties.insert(
            "terminal_projection_mode",
            Value::from(projection_mode_label),
        );
        descriptor.node_properties.insert(
            "terminal_index_only",
            Value::from(matches!(
                projection_mode,
                BytesByProjectionMode::CoveringIndex | BytesByProjectionMode::CoveringConstant
            )),
        );

        Ok(descriptor)
    }

    pub(in crate::db) fn explain_prepared_projection_terminal_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
        strategy: &PreparedFluentProjectionStrategy,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        let executable = self.prepared_execution_plan_for_visibility(Some(visible_indexes))?;
        let mut descriptor = executable
            .explain_load_execution_node_descriptor()
            .map_err(QueryError::execute)?;
        let projection_descriptor = strategy.explain_descriptor();

        descriptor.node_properties.insert(
            "terminal",
            Value::from(projection_descriptor.terminal_label()),
        );
        descriptor.node_properties.insert(
            "terminal_field",
            Value::from(projection_descriptor.field_label().to_string()),
        );
        descriptor.node_properties.insert(
            "terminal_output",
            Value::from(projection_descriptor.output_label()),
        );

        Ok(descriptor)
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

fn contains_execution_node_type(
    descriptor: &ExplainExecutionNodeDescriptor,
    target: ExplainExecutionNodeType,
) -> bool {
    descriptor.node_type() == target
        || descriptor
            .children()
            .iter()
            .any(|child| contains_execution_node_type(child, target))
}

fn plan_order_pushdown_label(order_pushdown: &ExplainOrderPushdown) -> String {
    match order_pushdown {
        ExplainOrderPushdown::MissingModelContext => "missing_model_context".to_string(),
        ExplainOrderPushdown::EligibleSecondaryIndex { index, prefix_len } => {
            format!("eligible(index={index},prefix_len={prefix_len})")
        }
        ExplainOrderPushdown::Rejected(reason) => format!("rejected({reason:?})"),
    }
}

fn plan_predicate_pushdown_label(
    predicate: &ExplainPredicate,
    access: &ExplainAccessPath,
) -> String {
    let access_label = match access {
        ExplainAccessPath::ByKey { .. } => "by_key",
        ExplainAccessPath::ByKeys { keys } if keys.is_empty() => "empty_access_contract",
        ExplainAccessPath::ByKeys { .. } => "by_keys",
        ExplainAccessPath::KeyRange { .. } => "key_range",
        ExplainAccessPath::IndexPrefix { .. } => "index_prefix",
        ExplainAccessPath::IndexMultiLookup { .. } => "index_multi_lookup",
        ExplainAccessPath::IndexRange { .. } => "index_range",
        ExplainAccessPath::FullScan => "full_scan",
        ExplainAccessPath::Union(_) => "union",
        ExplainAccessPath::Intersection(_) => "intersection",
    };
    if matches!(predicate, ExplainPredicate::None) {
        return "none".to_string();
    }
    if matches!(access, ExplainAccessPath::FullScan) {
        if explain_predicate_contains_non_strict_compare(predicate) {
            return "fallback(non_strict_compare_coercion)".to_string();
        }
        if explain_predicate_contains_empty_prefix_starts_with(predicate) {
            return "fallback(starts_with_empty_prefix)".to_string();
        }
        if explain_predicate_contains_is_null(predicate) {
            return "fallback(is_null_full_scan)".to_string();
        }
        if explain_predicate_contains_text_scan_operator(predicate) {
            return "fallback(text_operator_full_scan)".to_string();
        }

        return format!("fallback({access_label})");
    }

    format!("applied({access_label})")
}

fn explain_predicate_contains_non_strict_compare(predicate: &ExplainPredicate) -> bool {
    match predicate {
        ExplainPredicate::Compare { coercion, .. }
        | ExplainPredicate::CompareFields { coercion, .. } => coercion.id != CoercionId::Strict,
        ExplainPredicate::And(children) | ExplainPredicate::Or(children) => children
            .iter()
            .any(explain_predicate_contains_non_strict_compare),
        ExplainPredicate::Not(inner) => explain_predicate_contains_non_strict_compare(inner),
        ExplainPredicate::None
        | ExplainPredicate::True
        | ExplainPredicate::False
        | ExplainPredicate::IsNull { .. }
        | ExplainPredicate::IsNotNull { .. }
        | ExplainPredicate::IsMissing { .. }
        | ExplainPredicate::IsEmpty { .. }
        | ExplainPredicate::IsNotEmpty { .. }
        | ExplainPredicate::TextContains { .. }
        | ExplainPredicate::TextContainsCi { .. } => false,
    }
}

fn explain_predicate_contains_is_null(predicate: &ExplainPredicate) -> bool {
    match predicate {
        ExplainPredicate::IsNull { .. } => true,
        ExplainPredicate::And(children) | ExplainPredicate::Or(children) => {
            children.iter().any(explain_predicate_contains_is_null)
        }
        ExplainPredicate::Not(inner) => explain_predicate_contains_is_null(inner),
        ExplainPredicate::None
        | ExplainPredicate::True
        | ExplainPredicate::False
        | ExplainPredicate::Compare { .. }
        | ExplainPredicate::CompareFields { .. }
        | ExplainPredicate::IsNotNull { .. }
        | ExplainPredicate::IsMissing { .. }
        | ExplainPredicate::IsEmpty { .. }
        | ExplainPredicate::IsNotEmpty { .. }
        | ExplainPredicate::TextContains { .. }
        | ExplainPredicate::TextContainsCi { .. } => false,
    }
}

fn explain_predicate_contains_empty_prefix_starts_with(predicate: &ExplainPredicate) -> bool {
    match predicate {
        ExplainPredicate::Compare {
            op: CompareOp::StartsWith,
            value: Value::Text(prefix),
            ..
        } => prefix.is_empty(),
        ExplainPredicate::And(children) | ExplainPredicate::Or(children) => children
            .iter()
            .any(explain_predicate_contains_empty_prefix_starts_with),
        ExplainPredicate::Not(inner) => explain_predicate_contains_empty_prefix_starts_with(inner),
        ExplainPredicate::None
        | ExplainPredicate::True
        | ExplainPredicate::False
        | ExplainPredicate::Compare { .. }
        | ExplainPredicate::CompareFields { .. }
        | ExplainPredicate::IsNull { .. }
        | ExplainPredicate::IsNotNull { .. }
        | ExplainPredicate::IsMissing { .. }
        | ExplainPredicate::IsEmpty { .. }
        | ExplainPredicate::IsNotEmpty { .. }
        | ExplainPredicate::TextContains { .. }
        | ExplainPredicate::TextContainsCi { .. } => false,
    }
}

fn explain_predicate_contains_text_scan_operator(predicate: &ExplainPredicate) -> bool {
    match predicate {
        ExplainPredicate::Compare {
            op: CompareOp::EndsWith,
            ..
        }
        | ExplainPredicate::TextContains { .. }
        | ExplainPredicate::TextContainsCi { .. } => true,
        ExplainPredicate::And(children) | ExplainPredicate::Or(children) => children
            .iter()
            .any(explain_predicate_contains_text_scan_operator),
        ExplainPredicate::Not(inner) => explain_predicate_contains_text_scan_operator(inner),
        ExplainPredicate::Compare { .. }
        | ExplainPredicate::CompareFields { .. }
        | ExplainPredicate::None
        | ExplainPredicate::True
        | ExplainPredicate::False
        | ExplainPredicate::IsNull { .. }
        | ExplainPredicate::IsNotNull { .. }
        | ExplainPredicate::IsMissing { .. }
        | ExplainPredicate::IsEmpty { .. }
        | ExplainPredicate::IsNotEmpty { .. } => false,
    }
}

impl<E> Query<E>
where
    E: EntityKind + SingletonEntity,
    E::Key: Default,
{
    /// Set the access path to the singleton primary key.
    pub(crate) fn only(self) -> Self {
        let Self { inner, .. } = self;

        Self::from_inner(inner.only(E::Key::default().to_value()))
    }
}
