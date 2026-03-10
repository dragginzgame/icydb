//! Module: db::executor::explain::descriptor
//! Responsibility: canonical assembly for executor EXPLAIN descriptor payloads.
//! Does not own: route-capability derivation or explain rendering output.
//! Boundary: project immutable execution contracts into stable descriptor fields.

use crate::{
    db::{
        access::{
            PushdownApplicability, SecondaryOrderPushdownEligibility,
            SecondaryOrderPushdownRejection,
        },
        executor::{
            ExecutionPreparation, LoadExecutor,
            aggregate::AggregateFoldMode,
            continuation::ScalarContinuationContext,
            route::{AggregateSeekSpec, ExecutionRoutePlan, ExecutionRouteShape, TopNSeekSpec},
        },
        query::{
            builder::AggregateExpr,
            explain::{
                ExplainAccessPath as ExplainAccessRoute, ExplainExecutionDescriptor,
                ExplainExecutionMode, ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
                ExplainExecutionOrderingSource, ExplainPredicate,
            },
            plan::{
                AccessPlannedQuery, AggregateKind, DistinctExecutionStrategy,
                index_covering_existing_rows_terminal_eligible,
                project_access_choice_explain_snapshot,
            },
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue, FieldValue},
    value::Value,
};
use std::{collections::BTreeMap, ops::Bound};

// Assemble one canonical scalar load execution descriptor tree through route authority.
pub(in crate::db::executor) fn assemble_load_execution_node_descriptor<E>(
    plan: &AccessPlannedQuery<E::Key>,
) -> Result<ExplainExecutionNodeDescriptor, InternalError>
where
    E: EntityKind + EntityValue,
{
    // Phase 1: build canonical reusable preparation and route contracts for load mode.
    let execution_preparation = ExecutionPreparation::for_plan::<E>(plan);
    let continuation = ScalarContinuationContext::initial();
    let route_plan =
        LoadExecutor::<E>::build_execution_route_plan_for_load(plan, &continuation, None)?;
    let route_shape = route_plan.shape();

    // Phase 2: seed one root access node from the canonical access plan projection.
    let execution_mode = explain_execution_mode(route_shape);
    let access_strategy = ExplainAccessRoute::from_access_plan(&plan.access);
    let mut root = access_execution_node_descriptor(access_strategy, execution_mode);
    annotate_access_root_node_properties(&mut root, &route_plan);
    annotate_access_choice_node_properties(&mut root, E::MODEL, plan);
    root.covering_scan = Some(load_covering_scan_eligible());

    // Phase 3: project route/planner modifiers in execution order as descriptor children.
    let explain_predicate = explain_predicate_for_plan::<E>(plan);
    for predicate_stage in predicate_stage_descriptors(
        explain_predicate,
        root.access_strategy.as_ref(),
        execution_preparation.strict_mode().is_some(),
        execution_mode,
    ) {
        root.children.push(predicate_stage);
    }

    if let Some(node) = secondary_order_pushdown_descriptor(&route_plan, execution_mode) {
        root.children.push(node);
    }

    if let Some(node) = index_range_limit_pushdown_descriptor(&route_plan, execution_mode) {
        root.children.push(node);
    }

    if let Some(node) = top_n_seek_descriptor(&route_plan, execution_mode) {
        root.children.push(node);
    }

    if plan.scalar_plan().order.is_some() {
        let order_node_type = if route_shape.is_streaming() {
            ExplainExecutionNodeType::OrderByAccessSatisfied
        } else {
            ExplainExecutionNodeType::OrderByMaterializedSort
        };
        let mut order_node = empty_execution_node_descriptor(order_node_type, execution_mode);
        order_node.node_properties.insert(
            "order_satisfied_by_index".to_string(),
            Value::from(matches!(
                order_node_type,
                ExplainExecutionNodeType::OrderByAccessSatisfied
            )),
        );
        root.children.push(order_node);
    }

    match plan.distinct_execution_strategy() {
        DistinctExecutionStrategy::None => {}
        DistinctExecutionStrategy::PreOrdered => {
            root.children.push(empty_execution_node_descriptor(
                ExplainExecutionNodeType::DistinctPreOrdered,
                execution_mode,
            ));
        }
        DistinctExecutionStrategy::HashMaterialize => {
            root.children.push(empty_execution_node_descriptor(
                ExplainExecutionNodeType::DistinctMaterialized,
                ExplainExecutionMode::Materialized,
            ));
        }
    }

    if let Some(page) = plan.scalar_plan().page.as_ref() {
        let mut node =
            empty_execution_node_descriptor(ExplainExecutionNodeType::LimitOffset, execution_mode);
        node.limit = page.limit;
        node.cursor = Some(route_plan.continuation().capabilities().applied());
        node.node_properties.insert(
            "offset".to_string(),
            Value::from(u64_from_usize(page.offset as usize)),
        );
        root.children.push(node);
    }

    if route_plan.continuation().capabilities().applied() {
        let mut node =
            empty_execution_node_descriptor(ExplainExecutionNodeType::CursorResume, execution_mode);
        node.cursor = Some(true);
        root.children.push(node);
    }

    Ok(root)
}

/// Assemble canonical verbose diagnostics for one scalar load execution route.
pub(in crate::db::executor) fn assemble_load_execution_verbose_diagnostics<E>(
    plan: &AccessPlannedQuery<E::Key>,
) -> Result<Vec<String>, InternalError>
where
    E: EntityKind + EntityValue,
{
    // Phase 1: build canonical route authority inputs for load mode.
    let execution_preparation = ExecutionPreparation::for_plan::<E>(plan);
    let continuation = ScalarContinuationContext::initial();
    let route_plan =
        LoadExecutor::<E>::build_execution_route_plan_for_load(plan, &continuation, None)?;
    let route_shape = route_plan.shape();

    // Phase 2: emit deterministic route-level diagnostics used by verbose surfaces.
    let mut lines = vec![
        format!(
            "diagnostic.route.execution_mode={:?}",
            route_shape.execution_mode()
        ),
        format!(
            "diagnostic.route.fast_path_order={:?}",
            route_plan.fast_path_order()
        ),
        format!(
            "diagnostic.route.continuation_applied={}",
            route_plan.continuation().capabilities().applied()
        ),
        format!(
            "diagnostic.route.limit={:?}",
            route_plan.continuation().limit()
        ),
        secondary_order_pushdown_verbose_line(&route_plan),
    ];

    if let Some(spec) = route_plan.top_n_seek_spec() {
        lines.push(format!(
            "diagnostic.route.top_n_seek=fetch({})",
            u64_from_usize(spec.fetch())
        ));
    } else {
        lines.push("diagnostic.route.top_n_seek=disabled".to_string());
    }

    if let Some(spec) = route_plan.index_range_limit_spec {
        lines.push(format!(
            "diagnostic.route.index_range_limit_pushdown=fetch({})",
            u64_from_usize(spec.fetch)
        ));
    } else {
        lines.push("diagnostic.route.index_range_limit_pushdown=disabled".to_string());
    }

    let predicate_stage = if plan.scalar_plan().predicate.is_none() {
        "none".to_string()
    } else if execution_preparation.strict_mode().is_some() {
        "index_prefilter(strict_all_or_none)".to_string()
    } else {
        "residual_post_access".to_string()
    };
    lines.push(format!(
        "diagnostic.route.predicate_stage={predicate_stage}"
    ));

    Ok(lines)
}

// Assemble one canonical scalar aggregate execution descriptor through route authority.
pub(in crate::db::executor) fn assemble_aggregate_terminal_execution_descriptor<E>(
    plan: &AccessPlannedQuery<E::Key>,
    aggregate: AggregateExpr,
) -> ExplainExecutionDescriptor
where
    E: EntityKind + EntityValue,
{
    let aggregation = aggregate.kind();

    // Phase 1: derive one aggregate route plan using precomputed execution preparation.
    let execution_preparation = ExecutionPreparation::for_plan::<E>(plan);
    let route_plan =
        LoadExecutor::<E>::build_execution_route_plan_for_aggregate_spec_with_preparation(
            plan,
            aggregate,
            &execution_preparation,
        );
    let route_shape = route_plan.shape();

    // Phase 2: project route-owned ordering + execution semantics into explain fields.
    let ordering_source = match route_plan.aggregate_seek_spec() {
        Some(AggregateSeekSpec::First { fetch }) => {
            ExplainExecutionOrderingSource::IndexSeekFirst { fetch }
        }
        Some(AggregateSeekSpec::Last { fetch }) => {
            ExplainExecutionOrderingSource::IndexSeekLast { fetch }
        }
        None if route_shape.is_materialized() => ExplainExecutionOrderingSource::Materialized,
        None => ExplainExecutionOrderingSource::AccessOrder,
    };
    let execution_mode = explain_execution_mode(route_shape);
    let covering_projection =
        aggregate_covering_projection_for_terminal(plan, aggregation, &execution_preparation);
    let node_properties = explain_node_properties_for_route(&route_plan, aggregation);

    // Phase 3: emit one stable descriptor payload consumed by explain surfaces.
    ExplainExecutionDescriptor {
        access_strategy: ExplainAccessRoute::from_access_plan(&plan.access),
        // Covering flag reflects index-only aggregate fast-path eligibility for
        // scalar aggregate terminals.
        covering_projection,
        aggregation,
        execution_mode,
        ordering_source,
        limit: route_plan.continuation().limit(),
        cursor: route_plan.continuation().capabilities().applied(),
        node_properties,
    }
}

const fn empty_execution_node_descriptor(
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
        node_properties: BTreeMap::new(),
    }
}

fn access_execution_node_descriptor(
    access_strategy: ExplainAccessRoute,
    execution_mode: ExplainExecutionMode,
) -> ExplainExecutionNodeDescriptor {
    let mut node =
        empty_execution_node_descriptor(access_node_type(&access_strategy), execution_mode);
    node.access_strategy = Some(access_strategy.clone());

    match access_strategy {
        ExplainAccessRoute::Union(children) | ExplainAccessRoute::Intersection(children) => {
            for child in children {
                node.children
                    .push(access_execution_node_descriptor(child, execution_mode));
            }
        }
        ExplainAccessRoute::ByKey { .. }
        | ExplainAccessRoute::ByKeys { .. }
        | ExplainAccessRoute::KeyRange { .. }
        | ExplainAccessRoute::IndexPrefix { .. }
        | ExplainAccessRoute::IndexMultiLookup { .. }
        | ExplainAccessRoute::IndexRange { .. }
        | ExplainAccessRoute::FullScan => {}
    }

    node
}

fn annotate_access_root_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    route_plan: &ExecutionRoutePlan,
) {
    if let Some(prefix_len) = access_prefix_len(node.access_strategy.as_ref()) {
        node.node_properties.insert(
            "prefix_len".to_string(),
            Value::from(u64_from_usize(prefix_len)),
        );
    }
    if let Some(fetch) = scan_fetch_pushdown(route_plan) {
        node.node_properties
            .insert("fetch".to_string(), Value::from(u64_from_usize(fetch)));
    }
}

// Scalar load routes currently materialize entity rows after access-key
// discovery, so execution is not index-only. Keep this explicit in explain
// output so future index-only load paths can flip this contract intentionally.
const fn load_covering_scan_eligible() -> bool {
    false
}

fn annotate_access_choice_node_properties<K>(
    node: &mut ExplainExecutionNodeDescriptor,
    model: &crate::model::entity::EntityModel,
    plan: &AccessPlannedQuery<K>,
) where
    K: FieldValue,
{
    let access_choice = project_access_choice_explain_snapshot(model, plan);
    node.node_properties.insert(
        "access_choice_chosen".to_string(),
        Value::from(access_choice.chosen_label),
    );
    node.node_properties.insert(
        "access_choice_chosen_reason".to_string(),
        Value::from(access_choice.chosen_reason.code()),
    );

    let alternatives = access_choice
        .alternatives
        .into_iter()
        .map(Value::from)
        .collect();
    node.node_properties.insert(
        "access_choice_alternatives".to_string(),
        Value::List(alternatives),
    );
    let rejected = access_choice
        .rejected
        .into_iter()
        .map(|entry| Value::from(entry.render()))
        .collect();
    node.node_properties.insert(
        "access_choice_rejections".to_string(),
        Value::List(rejected),
    );
}

const fn access_prefix_len(access_strategy: Option<&ExplainAccessRoute>) -> Option<usize> {
    match access_strategy {
        Some(
            ExplainAccessRoute::IndexPrefix { prefix_len, .. }
            | ExplainAccessRoute::IndexRange { prefix_len, .. },
        ) => Some(*prefix_len),
        Some(
            ExplainAccessRoute::ByKey { .. }
            | ExplainAccessRoute::ByKeys { .. }
            | ExplainAccessRoute::KeyRange { .. }
            | ExplainAccessRoute::IndexMultiLookup { .. }
            | ExplainAccessRoute::FullScan
            | ExplainAccessRoute::Union(_)
            | ExplainAccessRoute::Intersection(_),
        )
        | None => None,
    }
}

fn scan_fetch_pushdown(route_plan: &ExecutionRoutePlan) -> Option<usize> {
    route_plan
        .top_n_seek_spec()
        .map(TopNSeekSpec::fetch)
        .or_else(|| route_plan.index_range_limit_spec.map(|spec| spec.fetch))
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

fn secondary_order_pushdown_descriptor(
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
    node.node_properties
        .insert("index".to_string(), Value::from(*index));
    node.node_properties.insert(
        "prefix_len".to_string(),
        Value::from(u64_from_usize(*prefix_len)),
    );

    Some(node)
}

fn secondary_order_pushdown_verbose_line(route_plan: &ExecutionRoutePlan) -> String {
    match &route_plan.secondary_pushdown_applicability {
        PushdownApplicability::NotApplicable => {
            "diagnostic.route.secondary_order_pushdown=not_applicable".to_string()
        }
        PushdownApplicability::Applicable(SecondaryOrderPushdownEligibility::Eligible {
            index,
            prefix_len,
        }) => format!(
            "diagnostic.route.secondary_order_pushdown=eligible(index={index},prefix_len={})",
            u64_from_usize(*prefix_len)
        ),
        PushdownApplicability::Applicable(SecondaryOrderPushdownEligibility::Rejected(reason)) => {
            format!(
                "diagnostic.route.secondary_order_pushdown=rejected({})",
                secondary_order_pushdown_rejection_label(reason)
            )
        }
    }
}

fn secondary_order_pushdown_rejection_label(reason: &SecondaryOrderPushdownRejection) -> String {
    match reason {
        SecondaryOrderPushdownRejection::NoOrderBy => "NoOrderBy".to_string(),
        SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix => {
            "AccessPathNotSingleIndexPrefix".to_string()
        }
        SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported { index, prefix_len } => {
            format!(
                "AccessPathIndexRangeUnsupported(index={index},prefix_len={})",
                u64_from_usize(*prefix_len)
            )
        }
        SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
            prefix_len,
            index_field_len,
        } => format!(
            "InvalidIndexPrefixBounds(prefix_len={},index_field_len={})",
            u64_from_usize(*prefix_len),
            u64_from_usize(*index_field_len)
        ),
        SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak { field } => {
            format!("MissingPrimaryKeyTieBreak(field={field})")
        }
        SecondaryOrderPushdownRejection::PrimaryKeyDirectionNotAscending { field } => {
            format!("PrimaryKeyDirectionNotAscending(field={field})")
        }
        SecondaryOrderPushdownRejection::MixedDirectionNotEligible { field } => {
            format!("MixedDirectionNotEligible(field={field})")
        }
        SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
            index,
            prefix_len,
            expected_suffix,
            expected_full,
            actual,
        } => format!(
            "OrderFieldsDoNotMatchIndex(index={index},prefix_len={},expected_suffix={expected_suffix:?},expected_full={expected_full:?},actual={actual:?})",
            u64_from_usize(*prefix_len)
        ),
    }
}

fn index_range_limit_pushdown_descriptor(
    route_plan: &ExecutionRoutePlan,
    execution_mode: ExplainExecutionMode,
) -> Option<ExplainExecutionNodeDescriptor> {
    let spec = route_plan.index_range_limit_spec?;

    let mut node = empty_execution_node_descriptor(
        ExplainExecutionNodeType::IndexRangeLimitPushdown,
        execution_mode,
    );
    node.node_properties
        .insert("fetch".to_string(), Value::from(u64_from_usize(spec.fetch)));

    Some(node)
}

fn top_n_seek_descriptor(
    route_plan: &ExecutionRoutePlan,
    execution_mode: ExplainExecutionMode,
) -> Option<ExplainExecutionNodeDescriptor> {
    let spec = route_plan.top_n_seek_spec()?;

    let mut node =
        empty_execution_node_descriptor(ExplainExecutionNodeType::TopNSeek, execution_mode);
    node.node_properties.insert(
        "fetch".to_string(),
        Value::from(u64_from_usize(spec.fetch())),
    );

    Some(node)
}

fn predicate_stage_descriptors(
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
            .insert("pushdown".to_string(), Value::from(pushdown_predicate));
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
        ExplainAccessRoute::ByKey { .. }
        | ExplainAccessRoute::ByKeys { .. }
        | ExplainAccessRoute::KeyRange { .. }
        | ExplainAccessRoute::FullScan
        | ExplainAccessRoute::Union(_)
        | ExplainAccessRoute::Intersection(_) => None,
    }
}

fn prefix_predicate_text(fields: &[&str], values: &[Value], prefix_len: usize) -> Option<String> {
    let applied_len = prefix_len.min(fields.len()).min(values.len());
    if applied_len == 0 {
        return None;
    }

    let mut parts = Vec::with_capacity(applied_len);
    for idx in 0..applied_len {
        parts.push(format!("{}={:?}", fields[idx], values[idx]));
    }

    Some(parts.join(" AND "))
}

fn index_range_pushdown_predicate_text(
    fields: &[&str],
    prefix_len: usize,
    prefix: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(prefix_text) = prefix_predicate_text(fields, prefix, prefix_len) {
        parts.push(prefix_text);
    }

    let range_field = fields.get(prefix_len).copied().unwrap_or("index_range");
    match lower {
        Bound::Included(value) => parts.push(format!("{range_field}>={value:?}")),
        Bound::Excluded(value) => parts.push(format!("{range_field}>{value:?}")),
        Bound::Unbounded => {}
    }
    match upper {
        Bound::Included(value) => parts.push(format!("{range_field}<={value:?}")),
        Bound::Excluded(value) => parts.push(format!("{range_field}<{value:?}")),
        Bound::Unbounded => {}
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" AND "))
    }
}

fn explain_predicate_for_plan<E>(plan: &AccessPlannedQuery<E::Key>) -> Option<ExplainPredicate>
where
    E: EntityKind,
{
    let explain = plan.explain_with_model(E::MODEL);
    if matches!(explain.predicate(), ExplainPredicate::None) {
        None
    } else {
        Some(explain.predicate().clone())
    }
}

const fn explain_execution_mode(route_shape: ExecutionRouteShape) -> ExplainExecutionMode {
    if route_shape.is_streaming() {
        ExplainExecutionMode::Streaming
    } else {
        ExplainExecutionMode::Materialized
    }
}

fn explain_node_properties_for_route(
    route_plan: &ExecutionRoutePlan,
    aggregation: AggregateKind,
) -> BTreeMap<String, Value> {
    let mut node_properties = BTreeMap::new();

    // Keep seek metadata additive and node-local so explain schema can evolve
    // without introducing new top-level descriptor fields for each route hint.
    if let Some(fetch) = route_plan.aggregate_seek_fetch_hint() {
        node_properties.insert("fetch".to_string(), Value::from(u64_from_usize(fetch)));
    }
    if matches!(aggregation, AggregateKind::Count) {
        node_properties.insert(
            "count_fold_mode".to_string(),
            Value::from(aggregate_fold_mode_label(route_plan.aggregate_fold_mode)),
        );
    }

    node_properties
}

const fn aggregate_fold_mode_label(mode: AggregateFoldMode) -> &'static str {
    match mode {
        AggregateFoldMode::ExistingRows => "existing_rows",
        AggregateFoldMode::KeysOnly => "keys_only",
    }
}

// Return whether one scalar aggregate terminal can remain index-only under the
// current plan and executor preparation contracts.
fn aggregate_covering_projection_for_terminal<K>(
    plan: &AccessPlannedQuery<K>,
    aggregation: AggregateKind,
    execution_preparation: &ExecutionPreparation,
) -> bool {
    let strict_predicate_compatible = execution_preparation.strict_mode().is_some();

    match aggregation {
        AggregateKind::Count | AggregateKind::Exists => {
            index_covering_existing_rows_terminal_eligible(plan, strict_predicate_compatible)
        }
        AggregateKind::Min
        | AggregateKind::Max
        | AggregateKind::First
        | AggregateKind::Last
        | AggregateKind::Sum => false,
    }
}

const fn u64_from_usize(value: usize) -> u64 {
    value as u64
}
