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
                PushdownApplicability, TopNSeekSpec,
            },
        },
        query::{
            explain::{
                ExplainAccessPath as ExplainAccessRoute, ExplainExecutionMode,
                ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
                ExplainExecutionOrderingSource, ExplainPropertyMap, explain_projection_field_name,
            },
            plan::{
                AccessChoiceExplainSnapshot, AccessPlanProjection, AccessPlannedQuery,
                AggregateKind, DistinctExecutionStrategy, explain_access_strategy_label,
                project_explain_access_path,
            },
        },
    },
    value::Value,
};
use std::fmt::{Debug, Write};

pub(in crate::db::executor::explain::descriptor) use self::predicate::{
    aggregate_covering_projection_for_terminal, execution_preparation_predicate_index_capability,
    explain_filter_expr_for_plan, explain_predicate_for_plan,
    explain_residual_filter_expr_for_plan, fallback_explain_predicate_index_capability_for_plan,
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
        _index_name: &'static str,
        _index_fields: &[&'static str],
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
        _index_name: &'static str,
        _index_fields: &[&'static str],
        _values: &[Value],
    ) -> Self::Output {
        empty_execution_node_descriptor(
            ExplainExecutionNodeType::IndexMultiLookup,
            self.execution_mode,
        )
    }

    fn index_range(
        &mut self,
        _index_name: &'static str,
        _index_fields: &[&'static str],
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
        node.node_properties
            .insert("prefix_len", Value::from(u64_from_usize(prefix_len)));
    }
    if let Some(prefix_values) = access_prefix_values(node.access_strategy.as_ref()) {
        node.node_properties
            .insert("prefix_values", Value::List(prefix_values));
    }
    if let Some(fetch) = scan_fetch_pushdown(route_plan) {
        insert_fetch_node_property(node, fetch);
    }
    annotate_continuation_node_properties(
        node,
        route_plan.direction(),
        route_plan.continuation().mode(),
    );
}

pub(in crate::db::executor::explain::descriptor) fn annotate_projection_pushdown_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    plan: &AccessPlannedQuery,
    covering_scan: bool,
) {
    node.node_properties.insert(
        "proj_fields",
        value_list(
            plan.frozen_projection_spec()
                .fields()
                .map(explain_projection_field_name),
        ),
    );
    node.node_properties
        .insert("proj_pushdown", Value::from(covering_scan));
}

pub(in crate::db::executor::explain::descriptor) fn annotate_access_choice_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    access_choice: &AccessChoiceExplainSnapshot,
) {
    let chosen_label = explain_access_strategy_label(
        node.access_strategy
            .as_ref()
            .expect("access root must carry an access strategy"),
    );
    node.node_properties
        .insert("acc_choice", Value::from(chosen_label));
    node.node_properties.insert(
        "acc_reason",
        Value::from(access_choice.chosen_reason.code()),
    );
    node.node_properties.insert(
        "acc_alts",
        value_list(access_choice.alternatives.iter().copied()),
    );
    node.node_properties.insert(
        "acc_reject",
        value_list(access_choice.rejected.iter().cloned()),
    );
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

const fn access_prefix_len(access_strategy: Option<&ExplainAccessRoute>) -> Option<usize> {
    if let Some(
        ExplainAccessRoute::IndexPrefix { prefix_len, .. }
        | ExplainAccessRoute::IndexRange { prefix_len, .. },
    ) = access_strategy
    {
        Some(*prefix_len)
    } else {
        None
    }
}

fn access_prefix_values(access_strategy: Option<&ExplainAccessRoute>) -> Option<Vec<Value>> {
    match access_strategy {
        Some(
            ExplainAccessRoute::IndexPrefix { values, .. }
            | ExplainAccessRoute::IndexMultiLookup { values, .. },
        ) => Some(values.clone()),
        Some(ExplainAccessRoute::IndexRange { prefix, .. }) => Some(prefix.clone()),
        _ => None,
    }
}

fn scan_fetch_pushdown(route_plan: &ExecutionRoutePlan) -> Option<usize> {
    route_plan
        .top_n_seek_spec()
        .map(TopNSeekSpec::fetch)
        .or_else(|| route_plan.index_range_limit_spec.map(|spec| spec.fetch))
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
        .insert("fast_path", Value::from(selected_label));
    node.node_properties
        .insert("fast_reason", Value::from(selected_reason));
    node.node_properties.insert("fast_reject", rejections);
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
        ("none", "mat_fallback")
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
    insert_node_property(&mut node, "index", index);
    insert_node_property(&mut node, "prefix_len", u64_from_usize(prefix_len));

    Some(node)
}

pub(in crate::db::executor::explain::descriptor) fn order_by_execution_node_descriptor(
    has_order_by: bool,
    access_order_satisfied: bool,
    execution_mode: ExplainExecutionMode,
) -> Option<ExplainExecutionNodeDescriptor> {
    if !has_order_by {
        return None;
    }

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
        "order_by_idx",
        matches!(node_type, ExplainExecutionNodeType::OrderByAccessSatisfied),
    );

    Some(node)
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
    node.node_properties
        .insert("offset", Value::from(u64_from_usize(page.offset as usize)));

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
        node_properties.insert("fetch", Value::from(u64_from_usize(fetch)));
    }
    if aggregation.is_count() {
        node_properties.insert(
            "count_fold",
            Value::from(match route_plan.aggregate_fold_mode {
                AggregateFoldMode::ExistingRows => "rows",
                AggregateFoldMode::KeysOnly => "keys",
            }),
        );
    }
    node_properties.insert("proj_field", Value::from(projected_field.unwrap_or("none")));
    node_properties.insert(
        "proj_mode",
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
    node.node_properties
        .insert("scan_dir", Value::from(direction_code(direction)));
    node.node_properties.insert(
        "cont_mode",
        Value::from(continuation_mode_code(continuation_mode)),
    );
    node.node_properties.insert(
        "resume_from",
        Value::from(resume_from_label(continuation_mode)),
    );
}

fn insert_fetch_node_property(node: &mut ExplainExecutionNodeDescriptor, fetch: usize) {
    insert_node_property(node, "fetch", u64_from_usize(fetch));
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
