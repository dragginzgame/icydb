//! Module: db::executor::explain::descriptor::shared
//! Responsibility: module-local ownership and contracts for db::executor::explain::descriptor::shared.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        access::{
            PushdownApplicability, SecondaryOrderPushdownEligibility,
            SecondaryOrderPushdownRejection,
        },
        direction::Direction,
        executor::{
            ExecutionPreparation,
            aggregate::AggregateFoldMode,
            route::{
                AggregateSeekSpec, ContinuationMode, ExecutionRoutePlan, ExecutionRouteShape,
                FastPathOrder, TopNSeekSpec,
            },
        },
        predicate::{IndexPredicateCapability, PredicateCapabilityProfile},
        query::{
            explain::{
                ExplainAccessPath as ExplainAccessRoute, ExplainExecutionMode,
                ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
                ExplainExecutionOrderingSource, ExplainPredicate, ExplainPropertyMap,
                write_access_strategy_label,
            },
            plan::{
                AccessChoiceExplainSnapshot, AccessPlannedQuery, AggregateKind,
                DistinctExecutionStrategy,
                expr::{Expr, ProjectionField},
                index_covering_existing_rows_terminal_eligible,
            },
        },
    },
    value::Value,
};
use std::{
    borrow::Cow,
    fmt::{Debug, Write},
    ops::Bound,
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
        residual_predicate: None,
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
    // Preserve the owned explain access route on the node itself, then recurse
    // through child unions/intersections without cloning simple leaf routes.
    let mut node =
        empty_execution_node_descriptor(access_node_type(&access_strategy), execution_mode);
    node.access_strategy = Some(access_strategy);

    if let Some(ExplainAccessRoute::Union(children) | ExplainAccessRoute::Intersection(children)) =
        node.access_strategy.as_ref()
    {
        for child in children {
            node.children.push(access_execution_node_descriptor(
                child.clone(),
                execution_mode,
            ));
        }
    }

    node
}

pub(in crate::db::executor::explain::descriptor) fn annotate_access_root_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    route_plan: &ExecutionRoutePlan,
) {
    let continuation_capabilities = route_plan.continuation().capabilities();
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
        continuation_capabilities.mode(),
    );
}

pub(in crate::db::executor::explain::descriptor) fn annotate_projection_pushdown_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    model: &'static crate::model::entity::EntityModel,
    plan: &AccessPlannedQuery,
    covering_scan: bool,
) {
    let projected_fields = plan
        .projection_spec(model)
        .fields()
        .map(projection_field_label)
        .map(|field| Value::from(field.into_owned()))
        .collect();
    node.node_properties
        .insert("proj_fields", Value::List(projected_fields));
    node.node_properties
        .insert("proj_pushdown", Value::from(covering_scan));
}

pub(in crate::db::executor::explain::descriptor) fn projection_field_label(
    field: &ProjectionField,
) -> Cow<'_, str> {
    match field {
        ProjectionField::Scalar { expr, .. } => projection_expr_label(expr),
    }
}

// Keep projection metadata deterministic and planner-owned by reducing each
// expression to one stable field-like label for explain projection output.
fn projection_expr_label(expr: &Expr) -> Cow<'_, str> {
    match expr {
        Expr::Field(field) => Cow::Borrowed(field.as_str()),
        Expr::Alias { expr, .. } | Expr::Unary { expr, .. } => projection_expr_label(expr),
        Expr::Aggregate(aggregate) => aggregate
            .target_field()
            .map_or_else(|| Cow::Borrowed("aggregate"), Cow::Borrowed),
        Expr::Literal(_) => Cow::Borrowed("literal"),
        Expr::Binary { .. } => Cow::Borrowed("expr"),
    }
}

pub(in crate::db::executor::explain::descriptor) fn annotate_access_choice_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    access_choice: AccessChoiceExplainSnapshot,
) {
    let mut chosen_label = String::new();
    write_access_strategy_label(
        &mut chosen_label,
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
    let alternatives = access_choice
        .alternatives
        .into_iter()
        .map(Value::from)
        .collect();
    node.node_properties
        .insert("acc_alts", Value::List(alternatives));
    let rejected = access_choice
        .rejected
        .into_iter()
        .map(Value::from)
        .collect();
    node.node_properties
        .insert("acc_reject", Value::List(rejected));
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
        route_plan.continuation().capabilities().mode(),
    );
}

pub(in crate::db::executor::explain::descriptor) fn annotate_fast_path_reason_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    route_plan: &ExecutionRoutePlan,
) {
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
            rejections.push(Value::from(rejection));
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
    node.node_properties
        .insert("fast_path", Value::from(selected_label));
    node.node_properties
        .insert("fast_reason", Value::from(selected_reason));
    node.node_properties
        .insert("fast_reject", Value::List(rejections));
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
        FastPathOrder::SecondaryPrefix => match &route_plan.secondary_pushdown_applicability {
            PushdownApplicability::NotApplicable => "sec_order_na",
            PushdownApplicability::Applicable(SecondaryOrderPushdownEligibility::Rejected(_)) => {
                "sec_order_no"
            }
            PushdownApplicability::Applicable(SecondaryOrderPushdownEligibility::Eligible {
                ..
            }) => "sec_prefix_no",
        },
        FastPathOrder::IndexRange => {
            if route_plan
                .continuation()
                .capabilities()
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

const fn access_node_type(access: &ExplainAccessRoute) -> ExplainExecutionNodeType {
    match access {
        ExplainAccessRoute::ByKey { .. } => ExplainExecutionNodeType::ByKeyLookup,
        ExplainAccessRoute::ByKeys { .. } => ExplainExecutionNodeType::ByKeysLookup,
        ExplainAccessRoute::KeyRange { .. } => ExplainExecutionNodeType::PrimaryKeyRangeScan,
        ExplainAccessRoute::IndexPrefix { .. } => ExplainExecutionNodeType::IndexPrefixScan,
        ExplainAccessRoute::IndexMultiLookup { .. } => ExplainExecutionNodeType::IndexMultiLookup,
        ExplainAccessRoute::IndexRange { .. } => ExplainExecutionNodeType::IndexRangeScan,
        ExplainAccessRoute::FullScan => ExplainExecutionNodeType::FullScan,
        ExplainAccessRoute::Union(_) => ExplainExecutionNodeType::Union,
        ExplainAccessRoute::Intersection(_) => ExplainExecutionNodeType::Intersection,
    }
}

pub(in crate::db::executor::explain::descriptor) fn secondary_order_pushdown_descriptor(
    route_plan: &ExecutionRoutePlan,
    execution_mode: ExplainExecutionMode,
) -> Option<ExplainExecutionNodeDescriptor> {
    let PushdownApplicability::Applicable(eligibility) =
        &route_plan.secondary_pushdown_applicability
    else {
        return None;
    };
    let SecondaryOrderPushdownEligibility::Eligible { index, prefix_len } = eligibility else {
        return None;
    };

    let mut node = empty_execution_node_descriptor(
        ExplainExecutionNodeType::SecondaryOrderPushdown,
        execution_mode,
    );
    node.node_properties.insert("index", Value::from(*index));
    node.node_properties
        .insert("prefix_len", Value::from(u64_from_usize(*prefix_len)));

    Some(node)
}

pub(in crate::db::executor::explain::descriptor) fn order_by_execution_node_descriptor(
    has_order_by: bool,
    route_shape: ExecutionRouteShape,
    execution_mode: ExplainExecutionMode,
) -> Option<ExplainExecutionNodeDescriptor> {
    if !has_order_by {
        return None;
    }

    let node_type = if route_shape.is_streaming() {
        ExplainExecutionNodeType::OrderByAccessSatisfied
    } else {
        ExplainExecutionNodeType::OrderByMaterializedSort
    };
    let mut node = empty_execution_node_descriptor(node_type, execution_mode);
    node.node_properties.insert(
        "order_by_idx",
        Value::from(matches!(
            node_type,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )),
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
    node.cursor = Some(route_plan.continuation().capabilities().applied());
    node.node_properties
        .insert("offset", Value::from(u64_from_usize(page.offset as usize)));

    node
}

pub(in crate::db::executor::explain::descriptor) fn cursor_resume_execution_node_descriptor(
    route_plan: &ExecutionRoutePlan,
    execution_mode: ExplainExecutionMode,
) -> Option<ExplainExecutionNodeDescriptor> {
    if !route_plan.continuation().capabilities().applied() {
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
    match &route_plan.secondary_pushdown_applicability {
        PushdownApplicability::NotApplicable => {
            "diag.r.secondary_order_pushdown=not_applicable".to_string()
        }
        PushdownApplicability::Applicable(SecondaryOrderPushdownEligibility::Eligible {
            index,
            prefix_len,
        }) => format!(
            "diag.r.secondary_order_pushdown=eligible(index={index},prefix_len={})",
            u64_from_usize(*prefix_len)
        ),
        PushdownApplicability::Applicable(SecondaryOrderPushdownEligibility::Rejected(reason)) => {
            let mut out = "diag.r.secondary_order_pushdown=rejected(".to_string();
            write_secondary_order_pushdown_rejection_label(&mut out, reason);
            out.push(')');
            out
        }
    }
}

fn write_secondary_order_pushdown_rejection_label(
    out: &mut String,
    reason: &SecondaryOrderPushdownRejection,
) {
    match reason {
        SecondaryOrderPushdownRejection::NoOrderBy => out.push_str("NoOrderBy"),
        SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix => {
            out.push_str("AccessPathNotSingleIndexPrefix");
        }
        SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported { index, prefix_len } => {
            let _ = write!(
                out,
                "AccessPathIndexRangeUnsupported(index={index},prefix_len={})",
                u64_from_usize(*prefix_len)
            );
        }
        SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
            prefix_len,
            index_field_len,
        } => {
            let _ = write!(
                out,
                "InvalidIndexPrefixBounds(prefix_len={},index_field_len={})",
                u64_from_usize(*prefix_len),
                u64_from_usize(*index_field_len)
            );
        }
        SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak { field } => {
            let _ = write!(out, "MissingPrimaryKeyTieBreak(field={field})");
        }
        SecondaryOrderPushdownRejection::PrimaryKeyDirectionNotAscending { field } => {
            let _ = write!(out, "PrimaryKeyDirectionNotAscending(field={field})");
        }
        SecondaryOrderPushdownRejection::MixedDirectionNotEligible { field } => {
            let _ = write!(out, "MixedDirectionNotEligible(field={field})");
        }
        SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
            index,
            prefix_len,
            expected_suffix,
            expected_full,
            actual,
        } => {
            let _ = write!(
                out,
                "OrderFieldsDoNotMatchIndex(index={index},prefix_len={},expected_suffix={expected_suffix:?},expected_full={expected_full:?},actual={actual:?})",
                u64_from_usize(*prefix_len)
            );
        }
    }
}

pub(in crate::db::executor::explain::descriptor) fn index_range_limit_pushdown_descriptor(
    route_plan: &ExecutionRoutePlan,
    execution_mode: ExplainExecutionMode,
) -> Option<ExplainExecutionNodeDescriptor> {
    let spec = route_plan.index_range_limit_spec?;
    Some(fetch_execution_node_descriptor(
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
    Some(fetch_execution_node_descriptor(
        ExplainExecutionNodeType::TopNSeek,
        execution_mode,
        spec.fetch(),
    ))
}

pub(in crate::db::executor::explain::descriptor) fn predicate_stage_descriptors(
    explain_predicate: Option<ExplainPredicate>,
    access_strategy: Option<&ExplainAccessRoute>,
    strict_prefilter_compiled: bool,
    execution_mode: ExplainExecutionMode,
) -> Vec<ExplainExecutionNodeDescriptor> {
    let Some(explain_predicate) = explain_predicate else {
        return Vec::new();
    };

    if strict_prefilter_compiled {
        let mut node = empty_execution_node_descriptor(
            ExplainExecutionNodeType::IndexPredicatePrefilter,
            execution_mode,
        );
        node.predicate_pushdown = Some("strict_all_or_none".to_string());
        let pushdown_predicate = access_strategy
            .and_then(pushdown_predicate_from_access_strategy)
            .unwrap_or_else(|| format!("{explain_predicate:?}"));
        node.node_properties
            .insert("pushdown", Value::from(pushdown_predicate));
        return vec![node];
    }

    let mut node = empty_execution_node_descriptor(
        ExplainExecutionNodeType::ResidualPredicateFilter,
        execution_mode,
    );
    node.predicate_pushdown = access_strategy.and_then(pushdown_predicate_from_access_strategy);
    node.residual_predicate = Some(explain_predicate);

    vec![node]
}

pub(in crate::db::executor::explain::descriptor) fn execution_preparation_predicate_index_capability(
    execution_preparation: &ExecutionPreparation,
) -> Option<IndexPredicateCapability> {
    execution_preparation
        .predicate_capability_profile()
        .map(PredicateCapabilityProfile::index)
}

pub(in crate::db::executor::explain::descriptor) const fn predicate_index_capability_label(
    capability: IndexPredicateCapability,
) -> &'static str {
    match capability {
        IndexPredicateCapability::FullyIndexable => "fully_indexable",
        IndexPredicateCapability::PartiallyIndexable => "partially_indexable",
        IndexPredicateCapability::RequiresFullScan => "requires_full_scan",
    }
}

fn pushdown_predicate_from_access_strategy(access: &ExplainAccessRoute) -> Option<String> {
    match access {
        ExplainAccessRoute::IndexPrefix {
            fields,
            prefix_len,
            values,
            ..
        } => prefix_predicate_text(fields, values, *prefix_len),
        ExplainAccessRoute::IndexRange {
            fields,
            prefix_len,
            prefix,
            lower,
            upper,
            ..
        } => index_range_pushdown_predicate_text(fields, *prefix_len, prefix, lower, upper),
        ExplainAccessRoute::IndexMultiLookup { fields, values, .. } => {
            let field = fields.first()?;
            if values.is_empty() {
                None
            } else {
                Some(format!("{field} IN {values:?}"))
            }
        }
        _ => None,
    }
}

fn prefix_predicate_text(fields: &[&str], values: &[Value], prefix_len: usize) -> Option<String> {
    let applied_len = prefix_len.min(fields.len()).min(values.len());
    if applied_len == 0 {
        return None;
    }

    let mut out = String::new();
    for idx in 0..applied_len {
        if idx > 0 {
            out.push_str(" AND ");
        }
        let _ = write!(out, "{}={:?}", fields[idx], values[idx]);
    }

    Some(out)
}

fn index_range_pushdown_predicate_text(
    fields: &[&str],
    prefix_len: usize,
    prefix: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> Option<String> {
    let mut out = String::new();
    if let Some(prefix_text) = prefix_predicate_text(fields, prefix, prefix_len) {
        out.push_str(&prefix_text);
    }

    let range_field = fields.get(prefix_len).copied().unwrap_or("index_range");
    match lower {
        Bound::Included(value) => {
            if !out.is_empty() {
                out.push_str(" AND ");
            }
            let _ = write!(out, "{range_field}>={value:?}");
        }
        Bound::Excluded(value) => {
            if !out.is_empty() {
                out.push_str(" AND ");
            }
            let _ = write!(out, "{range_field}>{value:?}");
        }
        Bound::Unbounded => {}
    }
    match upper {
        Bound::Included(value) => {
            if !out.is_empty() {
                out.push_str(" AND ");
            }
            let _ = write!(out, "{range_field}<={value:?}");
        }
        Bound::Excluded(value) => {
            if !out.is_empty() {
                out.push_str(" AND ");
            }
            let _ = write!(out, "{range_field}<{value:?}");
        }
        Bound::Unbounded => {}
    }

    if out.is_empty() { None } else { Some(out) }
}

pub(in crate::db::executor::explain::descriptor) fn explain_predicate_for_plan(
    plan: &AccessPlannedQuery,
) -> Option<ExplainPredicate> {
    plan.effective_execution_predicate()
        .as_ref()
        .map(ExplainPredicate::from_predicate)
}

pub(in crate::db::executor::explain::descriptor) const fn explain_execution_mode(
    route_shape: ExecutionRouteShape,
) -> ExplainExecutionMode {
    if route_shape.is_streaming() {
        ExplainExecutionMode::Streaming
    } else {
        ExplainExecutionMode::Materialized
    }
}

pub(in crate::db::executor::explain::descriptor) const fn explain_aggregate_ordering_source(
    route_plan: &ExecutionRoutePlan,
    route_shape: ExecutionRouteShape,
) -> ExplainExecutionOrderingSource {
    match route_plan.aggregate_seek_spec() {
        Some(AggregateSeekSpec::First { fetch }) => {
            ExplainExecutionOrderingSource::IndexSeekFirst { fetch }
        }
        Some(AggregateSeekSpec::Last { fetch }) => {
            ExplainExecutionOrderingSource::IndexSeekLast { fetch }
        }
        None if route_shape.is_materialized() => ExplainExecutionOrderingSource::Materialized,
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
            Value::from(aggregate_fold_mode_label(route_plan.aggregate_fold_mode)),
        );
    }
    node_properties.insert("proj_field", Value::from(projected_field.unwrap_or("none")));
    node_properties.insert(
        "proj_mode",
        Value::from(aggregate_projection_mode_label(
            aggregation,
            projected_field.is_some(),
            covering_projection,
        )),
    );

    node_properties
}

const fn aggregate_projection_mode_label(
    aggregation: AggregateKind,
    has_projected_field: bool,
    covering_projection: bool,
) -> &'static str {
    aggregation.explain_projection_mode_label(has_projected_field, covering_projection)
}

const fn aggregate_fold_mode_label(mode: AggregateFoldMode) -> &'static str {
    match mode {
        AggregateFoldMode::ExistingRows => "rows",
        AggregateFoldMode::KeysOnly => "keys",
    }
}

// Return whether one scalar aggregate terminal can remain index-only under the
// current plan and executor preparation contracts.
pub(in crate::db::executor::explain::descriptor) fn aggregate_covering_projection_for_terminal(
    plan: &AccessPlannedQuery,
    aggregation: AggregateKind,
    execution_preparation: &ExecutionPreparation,
) -> bool {
    let strict_predicate_compatible = crate::db::query::plan::covering_strict_predicate_compatible(
        plan,
        execution_preparation_predicate_index_capability(execution_preparation),
    );

    if aggregation.supports_covering_existing_rows_terminal() {
        index_covering_existing_rows_terminal_eligible(plan, strict_predicate_compatible)
    } else {
        false
    }
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
    node.node_properties
        .insert("fetch", Value::from(u64_from_usize(fetch)));
}

fn fetch_execution_node_descriptor(
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
