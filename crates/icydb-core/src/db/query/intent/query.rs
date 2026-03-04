use crate::{
    db::{
        predicate::{CompareOp, MissingRowPolicy, Predicate},
        query::{
            builder::aggregate::AggregateExpr,
            explain::ExplainPlan,
            expr::{FilterExpr, SortExpr},
            intent::{QueryError, access_plan_to_entity_keys, model::QueryModel},
            plan::{AccessPlannedQuery, LoadSpec, QueryMode},
        },
    },
    traits::{EntityKind, SingletonEntity},
    value::Value,
};

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
    intent: QueryModel<'static, E::Key>,
}

impl<E: EntityKind> Query<E> {
    /// Create a new intent with an explicit missing-row policy.
    /// Ignore favors idempotency and may mask index/data divergence on deletes.
    /// Use Error to surface missing rows during scan/delete execution.
    #[must_use]
    pub const fn new(consistency: MissingRowPolicy) -> Self {
        Self {
            intent: QueryModel::new(E::MODEL, consistency),
        }
    }

    /// Return the intent mode (load vs delete).
    #[must_use]
    pub const fn mode(&self) -> QueryMode {
        self.intent.mode()
    }

    #[must_use]
    pub(crate) fn has_explicit_order(&self) -> bool {
        self.intent.has_explicit_order()
    }

    #[must_use]
    pub(crate) const fn has_grouping(&self) -> bool {
        self.intent.has_grouping()
    }

    #[must_use]
    pub(crate) const fn load_spec(&self) -> Option<LoadSpec> {
        match self.intent.mode() {
            QueryMode::Load(spec) => Some(spec),
            QueryMode::Delete(_) => None,
        }
    }

    /// Add a predicate, implicitly AND-ing with any existing predicate.
    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.intent = self.intent.filter(predicate);
        self
    }

    /// Apply a dynamic filter expression.
    pub fn filter_expr(self, expr: FilterExpr) -> Result<Self, QueryError> {
        let Self { intent } = self;
        let intent = intent.filter_expr(expr)?;

        Ok(Self { intent })
    }

    /// Apply a dynamic sort expression.
    pub fn sort_expr(self, expr: SortExpr) -> Result<Self, QueryError> {
        let Self { intent } = self;
        let intent = intent.sort_expr(expr)?;

        Ok(Self { intent })
    }

    /// Append an ascending sort key.
    #[must_use]
    pub fn order_by(mut self, field: impl AsRef<str>) -> Self {
        self.intent = self.intent.order_by(field);
        self
    }

    /// Append a descending sort key.
    #[must_use]
    pub fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
        self.intent = self.intent.order_by_desc(field);
        self
    }

    /// Enable DISTINCT semantics for this query.
    #[must_use]
    pub fn distinct(mut self) -> Self {
        self.intent = self.intent.distinct();
        self
    }

    /// Add one GROUP BY field.
    pub fn group_by(self, field: impl AsRef<str>) -> Result<Self, QueryError> {
        let Self { intent } = self;
        let intent = intent.push_group_field(field.as_ref())?;

        Ok(Self { intent })
    }

    /// Add one aggregate terminal via composable aggregate expression.
    #[must_use]
    pub fn aggregate(mut self, aggregate: AggregateExpr) -> Self {
        self.intent = self.intent.push_group_aggregate(aggregate);
        self
    }

    /// Override grouped hard limits for grouped execution budget enforcement.
    #[must_use]
    pub fn grouped_limits(mut self, max_groups: u64, max_group_bytes: u64) -> Self {
        self.intent = self.intent.grouped_limits(max_groups, max_group_bytes);
        self
    }

    /// Add one grouped HAVING compare clause over one grouped key field.
    pub fn having_group(
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

    /// Add one grouped HAVING compare clause over one grouped aggregate output.
    pub fn having_aggregate(
        self,
        aggregate_index: usize,
        op: CompareOp,
        value: Value,
    ) -> Result<Self, QueryError> {
        let Self { intent } = self;
        let intent = intent.push_having_aggregate_clause(aggregate_index, op, value)?;

        Ok(Self { intent })
    }

    /// Set the access path to a single primary key lookup.
    pub(crate) fn by_id(self, id: E::Key) -> Self {
        let Self { intent } = self;
        Self {
            intent: intent.by_id(id),
        }
    }

    /// Set the access path to a primary key batch lookup.
    pub(crate) fn by_ids<I>(self, ids: I) -> Self
    where
        I: IntoIterator<Item = E::Key>,
    {
        let Self { intent } = self;
        Self {
            intent: intent.by_ids(ids),
        }
    }

    /// Mark this intent as a delete query.
    #[must_use]
    pub fn delete(mut self) -> Self {
        self.intent = self.intent.delete();
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
        self.intent = self.intent.limit(limit);
        self
    }

    /// Apply an offset to a load intent.
    ///
    /// Scalar pagination requires an explicit `order_by(...)`.
    /// GROUP BY queries use canonical grouped-key order by default.
    /// Delete intents reject `offset(...)` during planning.
    #[must_use]
    pub fn offset(mut self, offset: u32) -> Self {
        self.intent = self.intent.offset(offset);
        self
    }

    /// Explain this intent without executing it.
    pub fn explain(&self) -> Result<ExplainPlan, QueryError> {
        let plan = self.planned()?;

        Ok(plan.explain())
    }

    /// Plan this intent into a neutral planned query contract.
    pub fn planned(&self) -> Result<PlannedQuery<E>, QueryError> {
        let plan = self.build_plan()?;
        let _projection = plan.projection_spec(E::MODEL);

        Ok(PlannedQuery::new(plan))
    }

    /// Compile this intent into query-owned handoff state.
    ///
    /// This boundary intentionally does not expose executor runtime shape.
    pub fn plan(&self) -> Result<CompiledQuery<E>, QueryError> {
        let plan = self.build_plan()?;
        let _projection = plan.projection_spec(E::MODEL);

        Ok(CompiledQuery::new(plan))
    }

    // Build a logical plan for the current intent.
    fn build_plan(&self) -> Result<AccessPlannedQuery<E::Key>, QueryError> {
        let plan_value = self.intent.build_plan_model()?;
        let (logical, access) = plan_value.into_parts();
        let access = access_plan_to_entity_keys::<E>(E::MODEL, access)?;
        let plan = AccessPlannedQuery::from_parts(logical, access);

        Ok(plan)
    }
}

impl<E> Query<E>
where
    E: EntityKind + SingletonEntity,
    E::Key: Default,
{
    /// Set the access path to the singleton primary key.
    pub(crate) fn only(self) -> Self {
        let Self { intent } = self;

        Self {
            intent: intent.only(E::Key::default()),
        }
    }
}

///
/// PlannedQuery
///
/// Neutral query-owned planned contract produced by query planning.
/// Stores logical + access shape without executor compilation state.
///

#[derive(Debug)]
pub struct PlannedQuery<E: EntityKind> {
    plan: AccessPlannedQuery<E::Key>,
}

impl<E: EntityKind> PlannedQuery<E> {
    #[must_use]
    pub(in crate::db) const fn new(plan: AccessPlannedQuery<E::Key>) -> Self {
        Self { plan }
    }

    #[must_use]
    pub fn explain(&self) -> ExplainPlan {
        self.plan.explain_with_model(E::MODEL)
    }
}

///
/// CompiledQuery
///
/// Query-owned compiled handoff produced by `Query::plan()`.
/// This type intentionally carries only logical/access query semantics.
/// Executor runtime shape is derived explicitly at the executor boundary.
///

#[derive(Clone, Debug)]
pub struct CompiledQuery<E: EntityKind> {
    plan: AccessPlannedQuery<E::Key>,
}

impl<E: EntityKind> CompiledQuery<E> {
    #[must_use]
    pub(in crate::db) const fn new(plan: AccessPlannedQuery<E::Key>) -> Self {
        Self { plan }
    }

    #[must_use]
    pub fn explain(&self) -> ExplainPlan {
        self.plan.explain_with_model(E::MODEL)
    }

    /// Borrow planner-lowered projection semantics for this compiled query.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn projection_spec(&self) -> crate::db::query::plan::expr::ProjectionSpec {
        self.plan.projection_spec(E::MODEL)
    }

    #[must_use]
    pub(in crate::db) fn into_inner(self) -> AccessPlannedQuery<E::Key> {
        self.plan
    }
}
