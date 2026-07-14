//! Module: db::executor::explain::descriptor::shared
//! Responsibility: shared EXPLAIN descriptor helpers for access-path, route,
//! predicate, and node-property projection across load and aggregate surfaces.
//! Does not own: top-level descriptor assembly or final explain rendering formats.
//! Boundary: keeps reusable descriptor fragments and annotations under one executor-owned helper surface.

mod predicate;

use crate::{
    db::{
        direction::Direction,
        executor::{
            aggregate::AggregateFoldMode,
            route::{
                AggregateSeekSpec, ContinuationMode, ExecutionRoutePlan, FastPathOrder,
                LoadOrderRouteMode, LoadOrderRouteReason, PushdownApplicability, RouteShapeKind,
                TopNSeekSpec,
            },
        },
        query::{
            explain::{
                ExplainAccessPath as ExplainAccessRoute, ExplainExecutionMode,
                ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
                ExplainExecutionOrderingSource, ExplainPropertyMap, explain_projection_field_name,
                property_keys, property_values,
            },
            plan::{
                AccessChoiceExplainSnapshot, AccessChoiceRejectedIndex, AccessPlanProjection,
                AccessPlannedQuery, AggregateKind, DistinctExecutionStrategy, OrderDirection,
                OrderSpec, explain_access_strategy_label, project_explain_access_path,
            },
        },
    },
    error::InternalError,
    value::Value,
};
use std::fmt::{Debug, Write};

pub(in crate::db::executor::explain::descriptor) use self::predicate::{
    PredicateStageObservability, aggregate_covering_projection_for_terminal,
    execution_preparation_predicate_index_capability, explain_filter_expr_for_plan,
    explain_predicate_for_plan, explain_residual_filter_expr_for_plan,
    predicate_index_capability_label, predicate_stage_descriptors,
};

pub(in crate::db::executor::explain::descriptor) const fn empty_execution_node_descriptor(
    node_type: ExplainExecutionNodeType,
    execution_mode: ExplainExecutionMode,
) -> ExplainExecutionNodeDescriptor {
    ExplainExecutionNodeDescriptor {
        node_type,
        execution_mode,
        access_strategy: None,
        predicate_pushdown: None,
        filter_expr: None,
        residual_filter_expr: None,
        residual_filter_predicate: None,
        projection: None,
        ordering_source: None,
        limit: None,
        cursor: None,
        covering_scan: None,
        rows_expected: None,
        children: Vec::new(),
        node_properties: ExplainPropertyMap::new(),
    }
}

pub(in crate::db::executor::explain::descriptor) fn access_execution_node_descriptor(
    access_strategy: ExplainAccessRoute,
    execution_mode: ExplainExecutionMode,
) -> ExplainExecutionNodeDescriptor {
    // Build the execution-node tree through the shared access projection
    // contract so executor descriptor assembly does not keep its own
    // recursive `ExplainAccessPath` walker beside explain/fingerprint users.
    let mut node = project_explain_access_path(
        &access_strategy,
        &mut ExplainAccessNodeDescriptorProjection { execution_mode },
    );
    node.access_strategy = Some(access_strategy);

    node
}

///
/// ExplainAccessNodeDescriptorProjection
///
/// Executor-side projection from canonical explain-access DTOs into execution
/// descriptor trees.
/// This keeps the descriptor builder on the shared access traversal contract
/// instead of maintaining another local recursive access-path walker.
///
struct ExplainAccessNodeDescriptorProjection {
    execution_mode: ExplainExecutionMode,
}

impl AccessPlanProjection<Value> for ExplainAccessNodeDescriptorProjection {
    type Output = ExplainExecutionNodeDescriptor;

    fn by_key(&mut self, _key: &Value) -> Self::Output {
        empty_execution_node_descriptor(ExplainExecutionNodeType::ByKeyLookup, self.execution_mode)
    }

    fn by_keys(&mut self, _keys: &[Value]) -> Self::Output {
        empty_execution_node_descriptor(ExplainExecutionNodeType::ByKeysLookup, self.execution_mode)
    }

    fn key_range(&mut self, _start: &Value, _end: &Value) -> Self::Output {
        empty_execution_node_descriptor(
            ExplainExecutionNodeType::PrimaryKeyRangeScan,
            self.execution_mode,
        )
    }

    fn index_prefix(
        &mut self,
        _index_name: &str,
        _index_fields: &[String],
        _prefix_len: usize,
        _values: &[Value],
    ) -> Self::Output {
        empty_execution_node_descriptor(
            ExplainExecutionNodeType::IndexPrefixScan,
            self.execution_mode,
        )
    }

    fn index_multi_lookup(
        &mut self,
        _index_name: &str,
        _index_fields: &[String],
        _values: &[Value],
    ) -> Self::Output {
        empty_execution_node_descriptor(
            ExplainExecutionNodeType::IndexMultiLookup,
            self.execution_mode,
        )
    }

    fn index_branch_set(
        &mut self,
        _index_name: &str,
        _index_fields: &[String],
        _fixed_values: &[Value],
        _branch_values: &[Value],
        _ordered_suffix: crate::db::access::IndexBranchSetOrderedSuffix,
    ) -> Self::Output {
        empty_execution_node_descriptor(
            ExplainExecutionNodeType::IndexBranchSet,
            self.execution_mode,
        )
    }

    fn index_range(
        &mut self,
        _index_name: &str,
        _index_fields: &[String],
        _prefix_len: usize,
        _prefix: &[Value],
        _lower: &std::ops::Bound<Value>,
        _upper: &std::ops::Bound<Value>,
    ) -> Self::Output {
        empty_execution_node_descriptor(
            ExplainExecutionNodeType::IndexRangeScan,
            self.execution_mode,
        )
    }

    fn full_scan(&mut self) -> Self::Output {
        empty_execution_node_descriptor(ExplainExecutionNodeType::FullScan, self.execution_mode)
    }

    fn union(&mut self, children: Vec<Self::Output>) -> Self::Output {
        let mut node =
            empty_execution_node_descriptor(ExplainExecutionNodeType::Union, self.execution_mode);
        node.children = children;
        node
    }

    fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output {
        let mut node = empty_execution_node_descriptor(
            ExplainExecutionNodeType::Intersection,
            self.execution_mode,
        );
        node.children = children;
        node
    }
}

pub(in crate::db::executor::explain::descriptor) fn annotate_access_root_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    route_plan: &ExecutionRoutePlan,
) {
    if let Some(prefix_len) = access_prefix_len(node.access_strategy.as_ref()) {
        node.node_properties.insert(
            property_keys::PREFIX_LEN,
            Value::from(u64_from_usize(prefix_len)),
        );
    }
    if let Some(prefix_values) = access_prefix_values(node.access_strategy.as_ref()) {
        node.node_properties
            .insert(property_keys::PREFIX_VALUES, Value::List(prefix_values));
    }
    if let Some(fetch) = scan_fetch_pushdown(route_plan) {
        insert_fetch_node_property(node, fetch);
    }
    if let Some(limit_stop_after) = route_limit_stop_after_node_property(route_plan) {
        insert_node_property(node, property_keys::LIMIT_STOP_AFTER, limit_stop_after);
    }
    annotate_route_class_node_properties(node, route_plan);
    annotate_continuation_node_properties(
        node,
        route_plan.direction(),
        route_plan.continuation().mode(),
    );
}

fn annotate_route_class_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    route_plan: &ExecutionRoutePlan,
) {
    if !matches!(route_plan.route_shape_kind(), RouteShapeKind::LoadScalar) {
        return;
    }

    let route_class = route_class_for_access(
        node.access_strategy.as_ref(),
        route_plan,
        scan_fetch_pushdown(route_plan),
    );
    insert_node_property(node, property_keys::ROUTE_FAMILY, route_class.family);
    insert_node_property(node, property_keys::ROUTE_OUTCOME, route_class.outcome);
    insert_node_property(node, property_keys::ROUTE_REASON, route_class.reason);
}

pub(in crate::db::executor::explain::descriptor) fn annotate_projection_pushdown_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    plan: &AccessPlannedQuery,
    covering_scan: bool,
) -> Result<(), InternalError> {
    node.node_properties.insert(
        property_keys::PROJECTION_FIELDS,
        value_list(
            plan.frozen_projection_spec()?
                .fields()
                .map(explain_projection_field_name),
        ),
    );
    node.node_properties.insert(
        property_keys::PROJECTION_PUSHDOWN,
        Value::from(covering_scan),
    );

    Ok(())
}

pub(in crate::db::executor::explain::descriptor) fn annotate_access_choice_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    access_choice: &AccessChoiceExplainSnapshot,
) -> Result<(), InternalError> {
    let access_strategy = node
        .access_strategy
        .as_ref()
        .ok_or_else(InternalError::query_executor_invariant)?;
    let chosen_label = explain_access_strategy_label(access_strategy);
    node.node_properties
        .insert(property_keys::ACCESS_CHOICE, Value::from(chosen_label));
    node.node_properties.insert(
        property_keys::ACCESS_REASON,
        Value::from(access_choice.chosen_reason.code()),
    );
    node.node_properties.insert(
        property_keys::ACCESS_ALTERNATIVES,
        value_list(access_choice.alternatives.iter().cloned()),
    );
    node.node_properties.insert(
        property_keys::ACCESS_REJECTIONS,
        value_list(
            access_choice
                .rejected
                .iter()
                .map(AccessChoiceRejectedIndex::label),
        ),
    );

    Ok(())
}

pub(in crate::db::executor::explain::descriptor) fn descriptor_route_property_line(
    line_key: &str,
    property_value: &str,
) -> String {
    let mut out = String::with_capacity(line_key.len() + property_value.len() + 1);
    out.push_str(line_key);
    out.push('=');
    out.push_str(property_value);
    out
}

pub(in crate::db::executor::explain::descriptor) fn route_diagnostic_line_bool(
    label: &str,
    value: bool,
) -> String {
    let mut out = route_diagnostic_prefix(label);
    out.push_str(if value { "true" } else { "false" });
    out
}

pub(in crate::db::executor::explain::descriptor) fn route_diagnostic_line_debug(
    label: &str,
    value: &impl Debug,
) -> String {
    let mut out = route_diagnostic_prefix(label);
    let _ = write!(out, "{value:?}");
    out
}

fn access_prefix_len(access_strategy: Option<&ExplainAccessRoute>) -> Option<usize> {
    let access_strategy = access_strategy?;
    if let ExplainAccessRoute::IndexPrefix { prefix_len, .. } = access_strategy {
        return Some(*prefix_len);
    }
    if let ExplainAccessRoute::IndexRange { prefix_len, .. } = access_strategy {
        return Some(*prefix_len);
    }
    if let ExplainAccessRoute::IndexBranchSet { fixed_values, .. } = access_strategy {
        return Some(fixed_values.len().saturating_add(1));
    }

    None
}

fn access_prefix_values(access_strategy: Option<&ExplainAccessRoute>) -> Option<Vec<Value>> {
    let access_strategy = access_strategy?;
    if let ExplainAccessRoute::IndexPrefix { values, .. } = access_strategy {
        return Some(values.clone());
    }
    if let ExplainAccessRoute::IndexMultiLookup { values, .. } = access_strategy {
        return Some(values.clone());
    }
    if let ExplainAccessRoute::IndexBranchSet {
        fixed_values,
        branch_values,
        ..
    } = access_strategy
    {
        let mut values = fixed_values.clone();
        values.extend_from_slice(branch_values);
        return Some(values);
    }
    if let ExplainAccessRoute::IndexRange { prefix, .. } = access_strategy {
        return Some(prefix.clone());
    }

    None
}

fn scan_fetch_pushdown(route_plan: &ExecutionRoutePlan) -> Option<usize> {
    route_plan
        .top_n_seek_spec()
        .map(TopNSeekSpec::fetch)
        .or_else(|| route_plan.index_range_limit_spec.map(|spec| spec.fetch))
}

#[derive(Clone, Copy)]
struct RouteClass {
    family: &'static str,
    outcome: &'static str,
    reason: &'static str,
}

impl RouteClass {
    const fn new(family: &'static str, outcome: &'static str, reason: &'static str) -> Self {
        Self {
            family,
            outcome,
            reason,
        }
    }
}

const fn route_class_for_access(
    access_strategy: Option<&ExplainAccessRoute>,
    route_plan: &ExecutionRoutePlan,
    fetch: Option<usize>,
) -> RouteClass {
    if route_plan.continuation().limit().is_none() {
        return RouteClass::new(
            "not_ordered_or_not_paginated",
            "unchanged_or_not_applicable",
            "no_limit",
        );
    }
    if route_plan.continuation().applied() && fetch.is_none() {
        return RouteClass::new("post_access_cursor", "post_access", "continuation_applied");
    }
    if route_plan.pk_order_fast_path_eligible()
        || access_is_primary_order_candidate(access_strategy)
    {
        return route_class_for_ordered_family(
            "primary_order",
            "primary_order_candidate",
            "primary_order_limit_stop_proven",
            route_plan,
            fetch,
        );
    }
    if route_plan.index_prefix_child_expansion().is_some() {
        return route_class_for_ordered_family(
            "equality_prefix_ordered_suffix",
            "equality_prefix_ordered_suffix_candidate",
            "equality_prefix_ordered_suffix_limit_stop_proven",
            route_plan,
            fetch,
        );
    }
    if access_is_secondary_order_candidate(access_strategy) {
        return route_class_for_ordered_family(
            secondary_route_family(route_plan),
            "secondary_order_candidate",
            "secondary_order_limit_stop_proven",
            route_plan,
            fetch,
        );
    }
    if matches!(access_strategy, Some(ExplainAccessRoute::FullScan)) {
        return route_class_for_ordered_family(
            "materialized_order",
            "full_scan_order_candidate",
            "full_scan_limit_stop_proven",
            route_plan,
            fetch,
        );
    }

    RouteClass::new(
        "unsupported_access_kind",
        "unsupported",
        "access_kind_not_classified",
    )
}

const fn route_class_for_ordered_family(
    family: &'static str,
    candidate_reason: &'static str,
    pushed_reason: &'static str,
    route_plan: &ExecutionRoutePlan,
    fetch: Option<usize>,
) -> RouteClass {
    match route_plan.load_order_route_mode() {
        LoadOrderRouteMode::DirectStreaming => {
            if fetch.is_some() {
                RouteClass::new(family, "pushed", pushed_reason)
            } else {
                RouteClass::new(family, "eligible_but_not_pushed", candidate_reason)
            }
        }
        LoadOrderRouteMode::MaterializedBoundary | LoadOrderRouteMode::MaterializedFallback => {
            RouteClass::new(
                family,
                materialized_route_outcome(route_plan.load_order_route_reason()),
                route_plan.load_order_route_reason().code(),
            )
        }
    }
}

const fn materialized_route_outcome(reason: LoadOrderRouteReason) -> &'static str {
    match reason {
        LoadOrderRouteReason::ResidualFilterBlocksDirectStreaming => "residual_unbounded",
        LoadOrderRouteReason::DescendingNonUniqueSecondaryPrefixNotAdmitted => {
            "missing_tie_breaker"
        }
        LoadOrderRouteReason::None
        | LoadOrderRouteReason::RequiresMaterializedSort
        | LoadOrderRouteReason::DistinctRequiresMaterialization => "materialized",
    }
}

const fn secondary_route_family(route_plan: &ExecutionRoutePlan) -> &'static str {
    match route_plan.load_order_route_reason() {
        LoadOrderRouteReason::ResidualFilterBlocksDirectStreaming => "residual_filter_ordered_scan",
        LoadOrderRouteReason::RequiresMaterializedSort
        | LoadOrderRouteReason::DistinctRequiresMaterialization => "materialized_order",
        LoadOrderRouteReason::None
        | LoadOrderRouteReason::DescendingNonUniqueSecondaryPrefixNotAdmitted => "secondary_order",
    }
}

const fn access_is_primary_order_candidate(access_strategy: Option<&ExplainAccessRoute>) -> bool {
    matches!(
        access_strategy,
        Some(
            ExplainAccessRoute::ByKey { .. }
                | ExplainAccessRoute::ByKeys { .. }
                | ExplainAccessRoute::KeyRange { .. }
        )
    )
}

const fn access_is_secondary_order_candidate(access_strategy: Option<&ExplainAccessRoute>) -> bool {
    matches!(
        access_strategy,
        Some(
            ExplainAccessRoute::IndexPrefix { .. }
                | ExplainAccessRoute::IndexMultiLookup { .. }
                | ExplainAccessRoute::IndexBranchSet { .. }
                | ExplainAccessRoute::IndexRange { .. }
        )
    )
}

pub(in crate::db::executor::explain::descriptor) fn annotate_cursor_resume_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    route_plan: &ExecutionRoutePlan,
) {
    annotate_continuation_node_properties(
        node,
        route_plan.direction(),
        route_plan.continuation().mode(),
    );
}

pub(in crate::db::executor::explain::descriptor) fn annotate_fast_path_reason_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    route_plan: &ExecutionRoutePlan,
) {
    let (selected_label, selected_reason, rejections) = fast_path_property_values(route_plan);
    node.node_properties
        .insert(property_keys::FAST_PATH, Value::from(selected_label));
    node.node_properties
        .insert(property_keys::FAST_REASON, Value::from(selected_reason));
    node.node_properties
        .insert(property_keys::FAST_REJECTIONS, rejections);
}

// Convert one iterator of route/explain-facing scalar values into the
// canonical `Value::List` payload used by descriptor node properties.
fn value_list<T>(values: impl IntoIterator<Item = T>) -> Value
where
    Value: From<T>,
{
    Value::List(values.into_iter().map(Value::from).collect())
}

// Derive the selected fast-path label/reason plus the rejected candidate list
// once so descriptor annotation does not open-code fast-path observability.
fn fast_path_property_values(
    route_plan: &ExecutionRoutePlan,
) -> (&'static str, &'static str, Value) {
    let mut selected: Option<FastPathOrder> = None;
    let mut rejections = Vec::new();
    for route in route_plan.fast_path_order() {
        if route_plan.load_fast_path_route_eligible(*route) {
            if selected.is_none() {
                selected = Some(*route);
            }
        } else {
            let mut rejection = String::new();
            write_fast_path_rejection_entry(&mut rejection, *route, route_plan);
            rejections.push(rejection);
        }
    }

    let (selected_label, selected_reason) = if let Some(route) = selected {
        (
            fast_path_label(route),
            fast_path_selected_reason(route, route_plan),
        )
    } else {
        (property_values::NONE, "mat_fallback")
    };

    (selected_label, selected_reason, value_list(rejections))
}

const fn fast_path_label(route: FastPathOrder) -> &'static str {
    match route {
        FastPathOrder::PrimaryKey => "primary_key",
        FastPathOrder::SecondaryPrefix => "secondary_prefix",
        FastPathOrder::PrimaryScan => "primary_scan",
        FastPathOrder::IndexRange => "index_range",
        FastPathOrder::Composite => "composite",
    }
}

const fn fast_path_selected_reason(
    route: FastPathOrder,
    route_plan: &ExecutionRoutePlan,
) -> &'static str {
    match route {
        FastPathOrder::PrimaryKey => "pk_fast_ok",
        FastPathOrder::SecondaryPrefix => {
            if route_plan.secondary_fast_path_eligible() {
                "sec_order_ok"
            } else if route_plan.field_min_fast_path_eligible()
                || route_plan.field_max_fast_path_eligible()
            {
                "extrema_ok"
            } else {
                "sec_prefix_ok"
            }
        }
        FastPathOrder::IndexRange => "idx_limit_ok",
        FastPathOrder::PrimaryScan => "prim_scan_ok",
        FastPathOrder::Composite => "comp_ok",
    }
}

const fn fast_path_rejection_reason(
    route: FastPathOrder,
    route_plan: &ExecutionRoutePlan,
) -> &'static str {
    match route {
        FastPathOrder::PrimaryKey => "pk_fast_no",
        FastPathOrder::SecondaryPrefix => {
            let applicability = &route_plan.secondary_pushdown_applicability;
            match applicability {
                PushdownApplicability::NotApplicable => "sec_order_na",
                PushdownApplicability::Rejected(_) => "sec_order_no",
                PushdownApplicability::Eligible { .. } => "sec_prefix_no",
            }
        }
        FastPathOrder::IndexRange => {
            if route_plan
                .continuation()
                .index_range_limit_pushdown_allowed()
            {
                "idx_limit_no"
            } else {
                "cont_blocks_idx_limit"
            }
        }
        FastPathOrder::PrimaryScan => "prim_scan_no",
        FastPathOrder::Composite => "comp_no",
    }
}

fn write_fast_path_rejection_entry(
    out: &mut String,
    route: FastPathOrder,
    route_plan: &ExecutionRoutePlan,
) {
    out.push_str(fast_path_label(route));
    out.push('=');
    out.push_str(fast_path_rejection_reason(route, route_plan));
}

const fn direction_code(direction: Direction) -> &'static str {
    match direction {
        Direction::Asc => "asc",
        Direction::Desc => "desc",
    }
}

const fn continuation_mode_code(mode: ContinuationMode) -> &'static str {
    match mode {
        ContinuationMode::Initial => "initial",
        ContinuationMode::CursorBoundary => "cursor_boundary",
        ContinuationMode::IndexRangeAnchor => "index_range_anchor",
    }
}

const fn resume_from_label(mode: ContinuationMode) -> &'static str {
    match mode {
        ContinuationMode::Initial => "none",
        ContinuationMode::CursorBoundary => "cursor_boundary",
        ContinuationMode::IndexRangeAnchor => "index_range_anchor",
    }
}

pub(in crate::db::executor::explain::descriptor) fn secondary_order_pushdown_descriptor(
    route_plan: &ExecutionRoutePlan,
    execution_mode: ExplainExecutionMode,
) -> Option<ExplainExecutionNodeDescriptor> {
    let (index, prefix_len) = route_plan
        .secondary_pushdown_applicability
        .eligible_secondary_index()?;

    let mut node = empty_execution_node_descriptor(
        ExplainExecutionNodeType::SecondaryOrderPushdown,
        execution_mode,
    );
    insert_node_property(&mut node, property_keys::INDEX, index);
    insert_node_property(
        &mut node,
        property_keys::PREFIX_LEN,
        u64_from_usize(prefix_len),
    );

    Some(node)
}

pub(in crate::db::executor::explain::descriptor) fn order_by_execution_node_descriptor(
    order: Option<&OrderSpec>,
    access_order_satisfied: bool,
    execution_mode: ExplainExecutionMode,
) -> Option<ExplainExecutionNodeDescriptor> {
    let order = order?;

    // EXPLAIN should describe whether the chosen access route already preserves
    // final ORDER BY semantics, even when some outer boundary still materializes
    // rows for projection, DISTINCT, or page shaping.
    let node_type = if access_order_satisfied {
        ExplainExecutionNodeType::OrderByAccessSatisfied
    } else {
        ExplainExecutionNodeType::OrderByMaterializedSort
    };
    let mut node = empty_execution_node_descriptor(node_type, execution_mode);
    insert_node_property(
        &mut node,
        property_keys::ORDER_BY_INDEX,
        matches!(node_type, ExplainExecutionNodeType::OrderByAccessSatisfied),
    );
    if matches!(node_type, ExplainExecutionNodeType::OrderByMaterializedSort)
        && let Some(hint) = materialized_order_index_hint(order)
    {
        insert_node_property(&mut node, property_keys::ORDER_BY_INDEX_HINT, hint);
    }

    Some(node)
}

pub(in crate::db::executor::explain::descriptor) fn materialized_order_index_hint(
    order: &OrderSpec,
) -> Option<String> {
    if order.fields.is_empty() {
        return None;
    }

    Some(
        order
            .fields
            .iter()
            .map(|term| {
                format!(
                    "{} {}",
                    term.rendered_label(),
                    order_direction_hint_label(term.direction())
                )
            })
            .collect::<Vec<_>>()
            .join(", "),
    )
}

const fn order_direction_hint_label(direction: OrderDirection) -> &'static str {
    match direction {
        OrderDirection::Asc => "ASC",
        OrderDirection::Desc => "DESC",
    }
}

pub(in crate::db::executor::explain::descriptor) const fn distinct_execution_node_descriptor(
    strategy: DistinctExecutionStrategy,
    execution_mode: ExplainExecutionMode,
) -> Option<ExplainExecutionNodeDescriptor> {
    match strategy {
        DistinctExecutionStrategy::None => None,
        DistinctExecutionStrategy::PreOrdered => Some(empty_execution_node_descriptor(
            ExplainExecutionNodeType::DistinctPreOrdered,
            execution_mode,
        )),
        DistinctExecutionStrategy::HashMaterialize => Some(empty_execution_node_descriptor(
            ExplainExecutionNodeType::DistinctMaterialized,
            ExplainExecutionMode::Materialized,
        )),
    }
}

pub(in crate::db::executor::explain::descriptor) fn limit_offset_execution_node_descriptor(
    page: &crate::db::query::plan::PageSpec,
    route_plan: &ExecutionRoutePlan,
    execution_mode: ExplainExecutionMode,
) -> ExplainExecutionNodeDescriptor {
    let mut node =
        empty_execution_node_descriptor(ExplainExecutionNodeType::LimitOffset, execution_mode);
    node.limit = page.limit;
    node.cursor = Some(route_plan.continuation().applied());
    node.node_properties.insert(
        property_keys::OFFSET,
        Value::from(u64_from_usize(page.offset as usize)),
    );

    node
}

pub(in crate::db::executor::explain::descriptor) fn cursor_resume_execution_node_descriptor(
    route_plan: &ExecutionRoutePlan,
    execution_mode: ExplainExecutionMode,
) -> Option<ExplainExecutionNodeDescriptor> {
    if !route_plan.continuation().applied() {
        return None;
    }

    let mut node =
        empty_execution_node_descriptor(ExplainExecutionNodeType::CursorResume, execution_mode);
    node.cursor = Some(true);
    annotate_cursor_resume_node_properties(&mut node, route_plan);

    Some(node)
}

pub(in crate::db::executor::explain::descriptor) fn secondary_order_pushdown_verbose_line(
    route_plan: &ExecutionRoutePlan,
) -> String {
    format!(
        "diag.r.secondary_order_pushdown={}",
        route_plan
            .secondary_pushdown_applicability
            .diagnostic_label()
    )
}

pub(in crate::db::executor::explain::descriptor) fn index_range_limit_pushdown_descriptor(
    route_plan: &ExecutionRoutePlan,
    execution_mode: ExplainExecutionMode,
) -> Option<ExplainExecutionNodeDescriptor> {
    let spec = route_plan.index_range_limit_spec?;
    Some(fetch_pushdown_execution_node_descriptor(
        ExplainExecutionNodeType::IndexRangeLimitPushdown,
        execution_mode,
        spec.fetch,
    ))
}

pub(in crate::db::executor::explain::descriptor) fn top_n_seek_descriptor(
    route_plan: &ExecutionRoutePlan,
    execution_mode: ExplainExecutionMode,
) -> Option<ExplainExecutionNodeDescriptor> {
    let spec = route_plan.top_n_seek_spec()?;
    Some(fetch_pushdown_execution_node_descriptor(
        ExplainExecutionNodeType::TopNSeek,
        execution_mode,
        spec.fetch(),
    ))
}

pub(in crate::db::executor::explain::descriptor) const fn explain_execution_mode(
    route_plan: &ExecutionRoutePlan,
) -> ExplainExecutionMode {
    if route_plan.is_streaming() {
        ExplainExecutionMode::Streaming
    } else {
        ExplainExecutionMode::Materialized
    }
}

pub(in crate::db::executor::explain::descriptor) const fn explain_aggregate_ordering_source(
    route_plan: &ExecutionRoutePlan,
) -> ExplainExecutionOrderingSource {
    match route_plan.aggregate_seek_spec() {
        Some(AggregateSeekSpec::First { fetch }) => {
            ExplainExecutionOrderingSource::IndexSeekFirst { fetch }
        }
        Some(AggregateSeekSpec::Last { fetch }) => {
            ExplainExecutionOrderingSource::IndexSeekLast { fetch }
        }
        None if route_plan.is_materialized() => ExplainExecutionOrderingSource::Materialized,
        None => ExplainExecutionOrderingSource::AccessOrder,
    }
}

pub(in crate::db::executor::explain::descriptor) fn explain_node_properties_for_route(
    route_plan: &ExecutionRoutePlan,
    aggregation: AggregateKind,
    projected_field: Option<&str>,
    covering_projection: bool,
) -> ExplainPropertyMap {
    let mut node_properties = ExplainPropertyMap::new();

    // Keep seek metadata additive and node-local so explain schema can evolve
    // without introducing new top-level descriptor fields for each route hint.
    if let Some(fetch) = route_plan.aggregate_seek_fetch_hint() {
        node_properties.insert(property_keys::FETCH, Value::from(u64_from_usize(fetch)));
    }
    if aggregation.is_count() {
        node_properties.insert(
            property_keys::COUNT_FOLD,
            Value::from(match route_plan.aggregate_fold_mode {
                AggregateFoldMode::ExistingRows => "rows",
                AggregateFoldMode::KeysOnly => "keys",
            }),
        );
    }
    node_properties.insert(
        property_keys::PROJECTION_FIELD,
        Value::from(projected_field.unwrap_or(property_values::NONE)),
    );
    node_properties.insert(
        property_keys::PROJECTION_MODE,
        Value::from(
            aggregation
                .explain_projection_mode_label(projected_field.is_some(), covering_projection),
        ),
    );

    node_properties
}

pub(in crate::db::executor::explain::descriptor) fn route_fetch_diagnostic_line(
    label: &str,
    fetch: Option<usize>,
) -> String {
    let mut out = route_diagnostic_prefix(label);
    if let Some(fetch) = fetch {
        let _ = write!(out, "fetch({})", u64_from_usize(fetch));
    } else {
        out.push_str("disabled");
    }

    out
}

pub(in crate::db::executor::explain::descriptor) fn route_limit_stop_after_diagnostic_line(
    route_plan: &ExecutionRoutePlan,
) -> String {
    let mut out = route_diagnostic_prefix("limit_stop_after");
    let Some(label) = route_limit_stop_after_node_property(route_plan) else {
        write_disabled_limit_stop_after(&mut out, limit_stop_after_disabled_reason(route_plan));
        return out;
    };

    out.push_str(&label);
    out
}

fn route_limit_stop_after_node_property(route_plan: &ExecutionRoutePlan) -> Option<String> {
    let fetch = scan_fetch_pushdown(route_plan)?;
    if !route_plan.load_order_route_mode().allows_streaming_load() {
        return Some(format!(
            "disabled({})",
            route_plan.load_order_route_reason().code()
        ));
    }

    let keep = route_plan
        .continuation()
        .keep_access_window()
        .fetch_limit()
        .unwrap_or(fetch);
    let lookahead = fetch.saturating_sub(keep);
    let limit = route_plan
        .continuation()
        .limit()
        .map_or_else(|| "none".to_string(), |limit| limit.to_string());
    Some(format!(
        "possible(limit={limit},lookahead={},fetch={})",
        u64_from_usize(lookahead),
        u64_from_usize(fetch),
    ))
}

fn write_disabled_limit_stop_after(out: &mut String, reason: &str) {
    let _ = write!(out, "disabled({reason})");
}

const fn limit_stop_after_disabled_reason(route_plan: &ExecutionRoutePlan) -> &'static str {
    if route_plan.continuation().limit().is_none() {
        return "no_limit";
    }
    if route_plan.continuation().applied() {
        return "continuation_applied";
    }
    if !route_plan.load_order_route_mode().allows_streaming_load() {
        return route_plan.load_order_route_reason().code();
    }

    "no_bounded_fetch"
}

fn route_diagnostic_prefix(label: &str) -> String {
    let mut out = String::with_capacity("diag.r.".len() + label.len() + 1);
    out.push_str("diag.r.");
    out.push_str(label);
    out.push('=');
    out
}

fn annotate_continuation_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    direction: Direction,
    continuation_mode: ContinuationMode,
) {
    node.node_properties.insert(
        property_keys::SCAN_DIRECTION,
        Value::from(direction_code(direction)),
    );
    node.node_properties.insert(
        property_keys::CONTINUATION_MODE,
        Value::from(continuation_mode_code(continuation_mode)),
    );
    node.node_properties.insert(
        property_keys::RESUME_FROM,
        Value::from(resume_from_label(continuation_mode)),
    );
}

fn insert_fetch_node_property(node: &mut ExplainExecutionNodeDescriptor, fetch: usize) {
    insert_node_property(node, property_keys::FETCH, u64_from_usize(fetch));
}

fn insert_node_property<T>(node: &mut ExplainExecutionNodeDescriptor, key: &'static str, value: T)
where
    Value: From<T>,
{
    node.node_properties.insert(key, Value::from(value));
}

fn fetch_pushdown_execution_node_descriptor(
    node_type: ExplainExecutionNodeType,
    execution_mode: ExplainExecutionMode,
    fetch: usize,
) -> ExplainExecutionNodeDescriptor {
    let mut node = empty_execution_node_descriptor(node_type, execution_mode);
    insert_fetch_node_property(&mut node, fetch);
    node
}

const fn u64_from_usize(value: usize) -> u64 {
    value as u64
}
