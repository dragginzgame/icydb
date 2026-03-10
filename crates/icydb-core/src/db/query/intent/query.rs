//! Module: query::intent::query
//! Responsibility: typed query-intent construction and planner handoff for entity queries.
//! Does not own: runtime execution semantics or access-path execution behavior.
//! Boundary: exposes query APIs and emits planner-owned compiled query contracts.

use crate::{
    db::{
        predicate::{CoercionId, CompareOp, MissingRowPolicy, Predicate},
        query::{
            builder::aggregate::AggregateExpr,
            explain::{
                ExplainAccessPath, ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
                ExplainOrderPushdown, ExplainPlan, ExplainPredicate,
            },
            expr::{FilterExpr, SortExpr},
            intent::{QueryError, access_plan_to_entity_keys, model::QueryModel},
            plan::{AccessPlannedQuery, LoadSpec, QueryMode},
        },
    },
    traits::{EntityKind, EntityValue, SingletonEntity},
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

    /// Return a stable plan hash for this intent.
    ///
    /// The hash is derived from canonical planner contracts and is suitable
    /// for diagnostics, explain diffing, and cache key construction.
    pub fn plan_hash_hex(&self) -> Result<String, QueryError> {
        let plan = self.build_plan()?;

        Ok(plan.fingerprint().to_string())
    }

    /// Explain executor-selected scalar load execution shape without running it.
    pub fn explain_execution(&self) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        let executable = self.plan()?.into_executable();

        executable
            .explain_load_execution_node_descriptor()
            .map_err(QueryError::execute)
    }

    /// Explain executor-selected scalar load execution shape as deterministic text.
    pub fn explain_execution_text(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        Ok(self.explain_execution()?.render_text_tree())
    }

    /// Explain executor-selected scalar load execution shape as canonical JSON.
    pub fn explain_execution_json(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        Ok(self.explain_execution()?.render_json_canonical())
    }

    /// Explain executor-selected scalar load execution shape with route diagnostics.
    pub fn explain_execution_verbose(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        let executable = self.plan()?.into_executable();
        let descriptor = executable
            .explain_load_execution_node_descriptor()
            .map_err(QueryError::execute)?;
        let route_diagnostics = executable
            .explain_load_execution_verbose_diagnostics()
            .map_err(QueryError::execute)?;
        let explain = self.explain()?;

        // Phase 1: render descriptor tree with node-local metadata.
        let mut lines = vec![descriptor.render_text_tree_verbose()];
        lines.extend(route_diagnostics);

        // Phase 2: add descriptor-stage summaries for key execution operators.
        lines.push(format!(
            "diagnostic.descriptor.has_top_n_seek={}",
            contains_execution_node_type(&descriptor, ExplainExecutionNodeType::TopNSeek)
        ));
        lines.push(format!(
            "diagnostic.descriptor.has_index_range_limit_pushdown={}",
            contains_execution_node_type(
                &descriptor,
                ExplainExecutionNodeType::IndexRangeLimitPushdown,
            )
        ));
        lines.push(format!(
            "diagnostic.descriptor.has_index_predicate_prefilter={}",
            contains_execution_node_type(
                &descriptor,
                ExplainExecutionNodeType::IndexPredicatePrefilter,
            )
        ));
        lines.push(format!(
            "diagnostic.descriptor.has_residual_predicate_filter={}",
            contains_execution_node_type(
                &descriptor,
                ExplainExecutionNodeType::ResidualPredicateFilter,
            )
        ));

        // Phase 3: append logical-plan diagnostics relevant to verbose explain.
        lines.push(format!("diagnostic.plan.mode={:?}", explain.mode()));
        lines.push(format!(
            "diagnostic.plan.order_pushdown={}",
            plan_order_pushdown_label(explain.order_pushdown())
        ));
        lines.push(format!(
            "diagnostic.plan.predicate_pushdown={}",
            plan_predicate_pushdown_label(explain.predicate(), explain.access())
        ));
        lines.push(format!("diagnostic.plan.distinct={}", explain.distinct()));
        lines.push(format!("diagnostic.plan.page={:?}", explain.page()));
        lines.push(format!(
            "diagnostic.plan.consistency={:?}",
            explain.consistency()
        ));

        Ok(lines.join("\n"))
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

    /// Return the stable plan hash for this planned query.
    #[must_use]
    pub fn plan_hash_hex(&self) -> String {
        self.plan.fingerprint().to_string()
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

    /// Return the stable plan hash for this compiled query.
    #[must_use]
    pub fn plan_hash_hex(&self) -> String {
        self.plan.fingerprint().to_string()
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::predicate::CoercionSpec, types::Ulid};

    fn strict_compare(field: &str, op: CompareOp, value: Value) -> ExplainPredicate {
        ExplainPredicate::Compare {
            field: field.to_string(),
            op,
            value,
            coercion: CoercionSpec::new(CoercionId::Strict),
        }
    }

    #[test]
    fn predicate_pushdown_label_prefix_like_and_equivalent_range_share_label() {
        let starts_with_predicate = strict_compare(
            "name",
            CompareOp::StartsWith,
            Value::Text("foo".to_string()),
        );
        let equivalent_range_predicate = ExplainPredicate::And(vec![
            strict_compare("name", CompareOp::Gte, Value::Text("foo".to_string())),
            strict_compare("name", CompareOp::Lt, Value::Text("fop".to_string())),
        ]);
        let access = ExplainAccessPath::IndexRange {
            name: "idx_name",
            fields: vec!["name"],
            prefix_len: 0,
            prefix: Vec::new(),
            lower: std::ops::Bound::Included(Value::Text("foo".to_string())),
            upper: std::ops::Bound::Excluded(Value::Text("fop".to_string())),
        };

        assert_eq!(
            plan_predicate_pushdown_label(&starts_with_predicate, &access),
            plan_predicate_pushdown_label(&equivalent_range_predicate, &access),
            "equivalent prefix-like and bounded-range shapes should report identical pushdown reason labels",
        );
        assert_eq!(
            plan_predicate_pushdown_label(&starts_with_predicate, &access),
            "applied(index_range)"
        );
    }

    #[test]
    fn predicate_pushdown_label_distinguishes_is_null_and_non_strict_full_scan_fallbacks() {
        let is_null_predicate = ExplainPredicate::IsNull {
            field: "group".to_string(),
        };
        let non_strict_predicate = ExplainPredicate::Compare {
            field: "group".to_string(),
            op: CompareOp::Eq,
            value: Value::Uint(7),
            coercion: CoercionSpec::new(CoercionId::NumericWiden),
        };
        let access = ExplainAccessPath::FullScan;

        assert_eq!(
            plan_predicate_pushdown_label(&is_null_predicate, &access),
            "fallback(is_null_full_scan)"
        );
        assert_eq!(
            plan_predicate_pushdown_label(&non_strict_predicate, &access),
            "fallback(non_strict_compare_coercion)"
        );
    }

    #[test]
    fn predicate_pushdown_label_reports_none_when_no_predicate_is_present() {
        let predicate = ExplainPredicate::None;
        let access = ExplainAccessPath::ByKey {
            key: Value::Ulid(Ulid::from_u128(7)),
        };

        assert_eq!(plan_predicate_pushdown_label(&predicate, &access), "none");
    }

    #[test]
    fn predicate_pushdown_label_reports_empty_access_contract_for_impossible_shapes() {
        let predicate = ExplainPredicate::Or(vec![
            ExplainPredicate::IsNull {
                field: "id".to_string(),
            },
            ExplainPredicate::And(vec![
                ExplainPredicate::Compare {
                    field: "id".to_string(),
                    op: CompareOp::In,
                    value: Value::List(Vec::new()),
                    coercion: CoercionSpec::new(CoercionId::Strict),
                },
                ExplainPredicate::True,
            ]),
        ]);
        let access = ExplainAccessPath::ByKeys { keys: Vec::new() };

        assert_eq!(
            plan_predicate_pushdown_label(&predicate, &access),
            "applied(empty_access_contract)"
        );
    }

    #[test]
    fn predicate_pushdown_label_distinguishes_empty_prefix_starts_with_full_scan_fallback() {
        let empty_prefix_predicate = ExplainPredicate::Compare {
            field: "label".to_string(),
            op: CompareOp::StartsWith,
            value: Value::Text(String::new()),
            coercion: CoercionSpec::new(CoercionId::Strict),
        };
        let non_empty_prefix_predicate = ExplainPredicate::Compare {
            field: "label".to_string(),
            op: CompareOp::StartsWith,
            value: Value::Text("l".to_string()),
            coercion: CoercionSpec::new(CoercionId::Strict),
        };
        let access = ExplainAccessPath::FullScan;

        assert_eq!(
            plan_predicate_pushdown_label(&empty_prefix_predicate, &access),
            "fallback(starts_with_empty_prefix)"
        );
        assert_eq!(
            plan_predicate_pushdown_label(&non_empty_prefix_predicate, &access),
            "fallback(full_scan)"
        );
    }

    #[test]
    fn predicate_pushdown_label_reports_text_operator_full_scan_fallback() {
        let text_contains = ExplainPredicate::TextContainsCi {
            field: "label".to_string(),
            value: Value::Text("needle".to_string()),
        };
        let ends_with = ExplainPredicate::Compare {
            field: "label".to_string(),
            op: CompareOp::EndsWith,
            value: Value::Text("fix".to_string()),
            coercion: CoercionSpec::new(CoercionId::Strict),
        };
        let access = ExplainAccessPath::FullScan;

        assert_eq!(
            plan_predicate_pushdown_label(&text_contains, &access),
            "fallback(text_operator_full_scan)"
        );
        assert_eq!(
            plan_predicate_pushdown_label(&ends_with, &access),
            "fallback(text_operator_full_scan)"
        );
    }

    #[test]
    fn predicate_pushdown_label_keeps_collection_contains_on_generic_full_scan_fallback() {
        let collection_contains = ExplainPredicate::Compare {
            field: "tags".to_string(),
            op: CompareOp::Contains,
            value: Value::Uint(7),
            coercion: CoercionSpec::new(CoercionId::CollectionElement),
        };
        let access = ExplainAccessPath::FullScan;

        assert_eq!(
            plan_predicate_pushdown_label(&collection_contains, &access),
            "fallback(non_strict_compare_coercion)"
        );
        assert_ne!(
            plan_predicate_pushdown_label(&collection_contains, &access),
            "fallback(text_operator_full_scan)"
        );
    }

    #[test]
    fn predicate_pushdown_label_non_strict_ends_with_uses_non_strict_fallback_precedence() {
        let non_strict_ends_with = ExplainPredicate::Compare {
            field: "label".to_string(),
            op: CompareOp::EndsWith,
            value: Value::Text("fix".to_string()),
            coercion: CoercionSpec::new(CoercionId::TextCasefold),
        };
        let access = ExplainAccessPath::FullScan;

        assert_eq!(
            plan_predicate_pushdown_label(&non_strict_ends_with, &access),
            "fallback(non_strict_compare_coercion)"
        );
        assert_ne!(
            plan_predicate_pushdown_label(&non_strict_ends_with, &access),
            "fallback(text_operator_full_scan)"
        );
    }
}
