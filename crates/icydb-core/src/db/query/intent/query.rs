//! Module: query::intent::query
//! Responsibility: typed query-intent construction and planner handoff for entity queries.
//! Does not own: runtime execution semantics or access-path execution behavior.
//! Boundary: exposes query APIs and emits planner-owned compiled query contracts.

use crate::{
    db::{
        executor::{
            BytesByProjectionMode, ExecutablePlan,
            assemble_aggregate_terminal_execution_descriptor,
            assemble_load_execution_node_descriptor,
            assemble_load_execution_node_descriptor_with_visible_indexes,
            assemble_load_execution_verbose_diagnostics,
            assemble_load_execution_verbose_diagnostics_with_visible_indexes,
            route::AggregateRouteShape,
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
            },
            expr::{FilterExpr, SortExpr},
            intent::{QueryError, model::QueryModel},
            plan::{AccessPlannedQuery, LoadSpec, QueryMode, VisibleIndexes},
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

    #[must_use]
    const fn mode(&self) -> QueryMode {
        self.intent.mode()
    }

    #[must_use]
    fn has_explicit_order(&self) -> bool {
        self.intent.has_explicit_order()
    }

    #[must_use]
    const fn has_grouping(&self) -> bool {
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
    pub(in crate::db) fn filter(mut self, predicate: Predicate) -> Self {
        self.intent = self.intent.filter(predicate);
        self
    }

    fn filter_expr(self, expr: FilterExpr) -> Result<Self, QueryError> {
        let Self { intent } = self;
        let intent = intent.filter_expr(expr)?;

        Ok(Self { intent })
    }

    fn sort_expr(self, expr: SortExpr) -> Result<Self, QueryError> {
        let Self { intent } = self;
        let intent = intent.sort_expr(expr)?;

        Ok(Self { intent })
    }

    #[must_use]
    pub(in crate::db) fn order_by(mut self, field: impl AsRef<str>) -> Self {
        self.intent = self.intent.order_by(field);
        self
    }

    #[must_use]
    pub(in crate::db) fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
        self.intent = self.intent.order_by_desc(field);
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

    pub(in crate::db) fn group_by(self, field: impl AsRef<str>) -> Result<Self, QueryError> {
        let Self { intent } = self;
        let intent = intent.push_group_field(field.as_ref())?;

        Ok(Self { intent })
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
        let Self { intent } = self;
        let intent = intent.push_having_group_clause(&field, op, value)?;

        Ok(Self { intent })
    }

    pub(in crate::db) fn having_aggregate(
        self,
        aggregate_index: usize,
        op: CompareOp,
        value: Value,
    ) -> Result<Self, QueryError> {
        let Self { intent } = self;
        let intent = intent.push_having_aggregate_clause(aggregate_index, op, value)?;

        Ok(Self { intent })
    }

    #[must_use]
    fn by_id(self, id: Value) -> Self {
        let Self { intent } = self;
        Self {
            intent: intent.by_id(id),
        }
    }

    #[must_use]
    fn by_ids<I>(self, ids: I) -> Self
    where
        I: IntoIterator<Item = Value>,
    {
        let Self { intent } = self;
        Self {
            intent: intent.by_ids(ids),
        }
    }

    #[must_use]
    fn only(self, id: Value) -> Self {
        let Self { intent } = self;

        Self {
            intent: intent.only(id),
        }
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
        let plan = self.build_plan_with_visible_indexes(visible_indexes)?;

        assemble_load_execution_node_descriptor_with_visible_indexes(
            self.intent.model().fields(),
            self.intent.model().primary_key().name(),
            visible_indexes,
            &plan,
        )
        .map_err(QueryError::execute)
    }

    // Explain one load execution shape through the structural query core.
    #[inline(never)]
    pub(in crate::db) fn explain_execution(
        &self,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError> {
        let plan = self.build_plan()?;

        assemble_load_execution_node_descriptor(
            self.intent.model().fields(),
            self.intent.model().primary_key().name(),
            &plan,
        )
        .map_err(QueryError::execute)
    }

    // Render one deterministic scalar load execution tree through the shared
    // structural descriptor path.
    pub(in crate::db) fn explain_execution_text(&self) -> Result<String, QueryError> {
        Ok(self.explain_execution()?.render_text_tree())
    }

    pub(in crate::db) fn explain_execution_text_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<String, QueryError> {
        Ok(self
            .explain_execution_with_visible_indexes(visible_indexes)?
            .render_text_tree())
    }

    // Render one canonical scalar load execution JSON payload through the shared
    // structural descriptor path.
    pub(in crate::db) fn explain_execution_json(&self) -> Result<String, QueryError> {
        Ok(self.explain_execution()?.render_json_canonical())
    }

    pub(in crate::db) fn explain_execution_json_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<String, QueryError> {
        Ok(self
            .explain_execution_with_visible_indexes(visible_indexes)?
            .render_json_canonical())
    }

    // Render one verbose scalar load execution payload through the shared
    // structural descriptor and route-diagnostics paths.
    #[inline(never)]
    pub(in crate::db) fn explain_execution_verbose(&self) -> Result<String, QueryError> {
        let plan = self.build_plan()?;
        let descriptor = assemble_load_execution_node_descriptor(
            self.intent.model().fields(),
            self.intent.model().primary_key().name(),
            &plan,
        )
        .map_err(QueryError::execute)?;
        let route_diagnostics = assemble_load_execution_verbose_diagnostics(
            self.intent.model().fields(),
            self.intent.model().primary_key().name(),
            &plan,
        )
        .map_err(QueryError::execute)?;
        let explain = plan.explain_with_model(self.intent.model());

        // Phase 1: render descriptor tree with node-local metadata.
        let mut lines = vec![descriptor.render_text_tree_verbose()];
        lines.extend(route_diagnostics);

        // Phase 2: add descriptor-stage summaries for key execution operators.
        lines.push(format!(
            "diag.d.has_top_n_seek={}",
            contains_execution_node_type(&descriptor, ExplainExecutionNodeType::TopNSeek)
        ));
        lines.push(format!(
            "diag.d.has_index_range_limit_pushdown={}",
            contains_execution_node_type(
                &descriptor,
                ExplainExecutionNodeType::IndexRangeLimitPushdown,
            )
        ));
        lines.push(format!(
            "diag.d.has_index_predicate_prefilter={}",
            contains_execution_node_type(
                &descriptor,
                ExplainExecutionNodeType::IndexPredicatePrefilter,
            )
        ));
        lines.push(format!(
            "diag.d.has_residual_predicate_filter={}",
            contains_execution_node_type(
                &descriptor,
                ExplainExecutionNodeType::ResidualPredicateFilter,
            )
        ));

        // Phase 3: append logical-plan diagnostics relevant to verbose explain.
        lines.push(format!("diag.p.mode={:?}", explain.mode()));
        lines.push(format!(
            "diag.p.order_pushdown={}",
            plan_order_pushdown_label(explain.order_pushdown())
        ));
        lines.push(format!(
            "diag.p.predicate_pushdown={}",
            plan_predicate_pushdown_label(explain.predicate(), explain.access())
        ));
        lines.push(format!("diag.p.distinct={}", explain.distinct()));
        lines.push(format!("diag.p.page={:?}", explain.page()));
        lines.push(format!("diag.p.consistency={:?}", explain.consistency()));

        Ok(lines.join("\n"))
    }

    #[inline(never)]
    pub(in crate::db) fn explain_execution_verbose_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<String, QueryError> {
        let plan = self.build_plan_with_visible_indexes(visible_indexes)?;
        let descriptor = assemble_load_execution_node_descriptor_with_visible_indexes(
            self.intent.model().fields(),
            self.intent.model().primary_key().name(),
            visible_indexes,
            &plan,
        )
        .map_err(QueryError::execute)?;
        let route_diagnostics = assemble_load_execution_verbose_diagnostics_with_visible_indexes(
            self.intent.model().fields(),
            self.intent.model().primary_key().name(),
            visible_indexes,
            &plan,
        )
        .map_err(QueryError::execute)?;
        let explain = plan.explain_with_model(self.intent.model());

        // Phase 1: render descriptor tree with node-local metadata.
        let mut lines = vec![descriptor.render_text_tree_verbose()];
        lines.extend(route_diagnostics);

        // Phase 2: add descriptor-stage summaries for key execution operators.
        lines.push(format!(
            "diag.d.has_top_n_seek={}",
            contains_execution_node_type(&descriptor, ExplainExecutionNodeType::TopNSeek)
        ));
        lines.push(format!(
            "diag.d.has_index_range_limit_pushdown={}",
            contains_execution_node_type(
                &descriptor,
                ExplainExecutionNodeType::IndexRangeLimitPushdown,
            )
        ));
        lines.push(format!(
            "diag.d.has_index_predicate_prefilter={}",
            contains_execution_node_type(
                &descriptor,
                ExplainExecutionNodeType::IndexPredicatePrefilter,
            )
        ));
        lines.push(format!(
            "diag.d.has_residual_predicate_filter={}",
            contains_execution_node_type(
                &descriptor,
                ExplainExecutionNodeType::ResidualPredicateFilter,
            )
        ));

        // Phase 3: append logical-plan diagnostics relevant to verbose explain.
        lines.push(format!("diag.p.mode={:?}", explain.mode()));
        lines.push(format!(
            "diag.p.order_pushdown={}",
            plan_order_pushdown_label(explain.order_pushdown())
        ));
        lines.push(format!(
            "diag.p.predicate_pushdown={}",
            plan_predicate_pushdown_label(explain.predicate(), explain.access())
        ));
        lines.push(format!("diag.p.distinct={}", explain.distinct()));
        lines.push(format!("diag.p.page={:?}", explain.page()));
        lines.push(format!("diag.p.consistency={:?}", explain.consistency()));

        Ok(lines.join("\n"))
    }

    #[inline(never)]
    pub(in crate::db) fn explain_aggregate_terminal_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
        aggregate: AggregateRouteShape<'_>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError> {
        let plan = self.build_plan_with_visible_indexes(visible_indexes)?;
        let query_explain = plan.explain_with_model(self.intent.model());
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
/// PlannedQueryCore
///
/// Generic-free planned-query payload shared by typed planned-query wrappers
/// so explain and plan-hash logic stay structural while public callers retain
/// entity-specific type inference.
///

#[derive(Debug)]
struct PlannedQueryCore {
    model: &'static crate::model::entity::EntityModel,
    plan: AccessPlannedQuery,
}

impl PlannedQueryCore {
    #[must_use]
    const fn new(
        model: &'static crate::model::entity::EntityModel,
        plan: AccessPlannedQuery,
    ) -> Self {
        Self { model, plan }
    }

    #[must_use]
    fn explain(&self) -> ExplainPlan {
        self.plan.explain_with_model(self.model)
    }

    /// Return the stable plan hash for this planned query.
    #[must_use]
    fn plan_hash_hex(&self) -> String {
        self.plan.fingerprint().to_string()
    }
}

///
/// PlannedQuery
///
/// Typed planned-query shell over one generic-free planner contract.
/// This preserves caller-side entity inference while keeping the stored plan
/// payload and explain/hash logic structural.
///

#[derive(Debug)]
pub struct PlannedQuery<E: EntityKind> {
    inner: PlannedQueryCore,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> PlannedQuery<E> {
    #[must_use]
    const fn from_inner(inner: PlannedQueryCore) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub fn explain(&self) -> ExplainPlan {
        self.inner.explain()
    }

    /// Return the stable plan hash for this planned query.
    #[must_use]
    pub fn plan_hash_hex(&self) -> String {
        self.inner.plan_hash_hex()
    }
}

///
/// CompiledQueryCore
///
/// Generic-free compiled-query payload shared by typed compiled-query wrappers
/// so executor handoff state remains structural until the final typed adapter
/// boundary.
///

#[derive(Clone, Debug)]
struct CompiledQueryCore {
    model: &'static crate::model::entity::EntityModel,
    entity_path: &'static str,
    plan: AccessPlannedQuery,
}

impl CompiledQueryCore {
    #[must_use]
    const fn new(
        model: &'static crate::model::entity::EntityModel,
        entity_path: &'static str,
        plan: AccessPlannedQuery,
    ) -> Self {
        Self {
            model,
            entity_path,
            plan,
        }
    }

    #[must_use]
    fn explain(&self) -> ExplainPlan {
        self.plan.explain_with_model(self.model)
    }

    /// Return the stable plan hash for this compiled query.
    #[must_use]
    fn plan_hash_hex(&self) -> String {
        self.plan.fingerprint().to_string()
    }

    #[must_use]
    #[cfg(test)]
    fn projection_spec(&self) -> crate::db::query::plan::expr::ProjectionSpec {
        self.plan.projection_spec(self.model)
    }

    #[must_use]
    fn into_inner(self) -> AccessPlannedQuery {
        self.plan
    }
}

///
/// CompiledQuery
///
/// Typed compiled-query shell over one generic-free planner contract.
/// The outer entity marker restores inference for executor handoff sites
/// while the stored execution payload remains structural.
///

#[derive(Clone, Debug)]
pub struct CompiledQuery<E: EntityKind> {
    inner: CompiledQueryCore,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> CompiledQuery<E> {
    #[must_use]
    const fn from_inner(inner: CompiledQueryCore) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub fn explain(&self) -> ExplainPlan {
        self.inner.explain()
    }

    /// Return the stable plan hash for this compiled query.
    #[must_use]
    pub fn plan_hash_hex(&self) -> String {
        self.inner.plan_hash_hex()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn projection_spec(&self) -> crate::db::query::plan::expr::ProjectionSpec {
        self.inner.projection_spec()
    }

    /// Convert one structural compiled query into an executor-ready typed plan.
    pub(in crate::db) fn into_executable(self) -> crate::db::executor::ExecutablePlan<E> {
        assert!(
            self.inner.entity_path == E::PATH,
            "compiled query entity mismatch: compiled for '{}', requested '{}'",
            self.inner.entity_path,
            E::PATH,
        );

        crate::db::executor::ExecutablePlan::new(self.into_inner())
    }

    #[must_use]
    pub(in crate::db) fn into_inner(self) -> AccessPlannedQuery {
        self.inner.into_inner()
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
        let plan = self
            .inner
            .build_plan_with_visible_indexes(visible_indexes)?;

        Ok(plan.explain_with_model(E::MODEL))
    }

    pub(in crate::db) fn plan_hash_hex_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<String, QueryError> {
        let plan = self
            .inner
            .build_plan_with_visible_indexes(visible_indexes)?;

        Ok(plan.fingerprint().to_string())
    }

    #[must_use]
    pub(crate) fn has_explicit_order(&self) -> bool {
        self.inner.has_explicit_order()
    }

    #[must_use]
    pub(crate) const fn has_grouping(&self) -> bool {
        self.inner.has_grouping()
    }

    #[must_use]
    pub(crate) const fn load_spec(&self) -> Option<LoadSpec> {
        self.inner.load_spec()
    }

    /// Add a predicate, implicitly AND-ing with any existing predicate.
    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.inner = self.inner.filter(predicate);
        self
    }

    /// Apply a dynamic filter expression.
    pub fn filter_expr(self, expr: FilterExpr) -> Result<Self, QueryError> {
        let Self { inner, .. } = self;
        let inner = inner.filter_expr(expr)?;

        Ok(Self::from_inner(inner))
    }

    /// Apply a dynamic sort expression.
    pub fn sort_expr(self, expr: SortExpr) -> Result<Self, QueryError> {
        let Self { inner, .. } = self;
        let inner = inner.sort_expr(expr)?;

        Ok(Self::from_inner(inner))
    }

    /// Append an ascending sort key.
    #[must_use]
    pub fn order_by(mut self, field: impl AsRef<str>) -> Self {
        self.inner = self.inner.order_by(field);
        self
    }

    /// Append a descending sort key.
    #[must_use]
    pub fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
        self.inner = self.inner.order_by_desc(field);
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
    #[cfg(feature = "sql")]
    #[cfg_attr(not(test), allow(dead_code))]
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
    /// explicit `order_by(...)` so pagination is deterministic.
    /// GROUP BY queries use canonical grouped-key order by default.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.inner = self.inner.limit(limit);
        self
    }

    /// Apply an offset to a load intent.
    ///
    /// Scalar pagination requires an explicit `order_by(...)`.
    /// GROUP BY queries use canonical grouped-key order by default.
    /// Delete intents reject `offset(...)` during planning.
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

    /// Explain executor-selected load execution shape without running it.
    pub fn explain_execution(&self) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.inner.explain_execution()
    }

    pub(in crate::db) fn explain_execution_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.inner
            .explain_execution_with_visible_indexes(visible_indexes)
    }

    /// Explain executor-selected load execution shape as deterministic text.
    pub fn explain_execution_text(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.inner.explain_execution_text()
    }

    pub(in crate::db) fn explain_execution_text_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.inner
            .explain_execution_text_with_visible_indexes(visible_indexes)
    }

    /// Explain executor-selected load execution shape as canonical JSON.
    pub fn explain_execution_json(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.inner.explain_execution_json()
    }

    pub(in crate::db) fn explain_execution_json_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.inner
            .explain_execution_json_with_visible_indexes(visible_indexes)
    }

    /// Explain executor-selected load execution shape with route diagnostics.
    #[inline(never)]
    pub fn explain_execution_verbose(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.inner.explain_execution_verbose()
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

    pub(in crate::db) fn explain_execution_verbose_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.inner
            .explain_execution_verbose_with_visible_indexes(visible_indexes)
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
        let executable = self
            .plan_with_visible_indexes(visible_indexes)?
            .into_executable();
        let mut descriptor = executable
            .explain_load_execution_node_descriptor()
            .map_err(QueryError::execute)?;
        let projection_mode = executable.bytes_by_projection_mode(target_field);
        let projection_mode_label =
            ExecutablePlan::<E>::bytes_by_projection_mode_label(projection_mode);

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
        let executable = self
            .plan_with_visible_indexes(visible_indexes)?
            .into_executable();
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
        let plan = self.inner.build_plan()?;
        let _projection = plan.projection_spec(E::MODEL);

        Ok(PlannedQuery::from_inner(PlannedQueryCore::new(
            E::MODEL,
            plan,
        )))
    }

    pub(in crate::db) fn planned_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<PlannedQuery<E>, QueryError> {
        let plan = self
            .inner
            .build_plan_with_visible_indexes(visible_indexes)?;
        let _projection = plan.projection_spec(E::MODEL);

        Ok(PlannedQuery::from_inner(PlannedQueryCore::new(
            E::MODEL,
            plan,
        )))
    }

    /// Compile this intent into query-owned handoff state.
    ///
    /// This boundary intentionally does not expose executor runtime shape.
    pub fn plan(&self) -> Result<CompiledQuery<E>, QueryError> {
        let plan = self.inner.build_plan()?;
        let _projection = plan.projection_spec(E::MODEL);

        Ok(CompiledQuery::from_inner(CompiledQueryCore::new(
            E::MODEL,
            E::PATH,
            plan,
        )))
    }

    pub(in crate::db) fn plan_with_visible_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<CompiledQuery<E>, QueryError> {
        let plan = self
            .inner
            .build_plan_with_visible_indexes(visible_indexes)?;
        let _projection = plan.projection_spec(E::MODEL);

        Ok(CompiledQuery::from_inner(CompiledQueryCore::new(
            E::MODEL,
            E::PATH,
            plan,
        )))
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
            format!("eligible(index={index},prefix_len={prefix_len})",)
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
        ExplainPredicate::Compare { coercion, .. } => coercion.id != CoercionId::Strict,
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
