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
            continuation::ScalarContinuationContext,
            route::{AggregateSeekSpec, ExecutionMode, ExecutionRoutePlan},
        },
        query::{
            builder::AggregateExpr,
            explain::{
                ExplainAccessPath as ExplainAccessRoute, ExplainExecutionDescriptor,
                ExplainExecutionMode, ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
                ExplainExecutionOrderingSource, ExplainPredicate,
            },
            plan::{AccessPlannedQuery, DistinctExecutionStrategy},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};
use std::collections::BTreeMap;

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

    // Phase 2: seed one root access node from the canonical access plan projection.
    let execution_mode = explain_execution_mode(route_plan.execution_mode);
    let access_strategy = ExplainAccessRoute::from_access_plan(&plan.access);
    let mut root = access_execution_node_descriptor(access_strategy, execution_mode);

    // Phase 3: project route/planner modifiers in execution order as descriptor children.
    let explain_predicate = explain_predicate_for_plan::<E>(plan);
    for predicate_stage in predicate_stage_descriptors(
        explain_predicate,
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
        let order_node_type = match route_plan.execution_mode {
            ExecutionMode::Streaming => ExplainExecutionNodeType::OrderByAccessSatisfied,
            ExecutionMode::Materialized => ExplainExecutionNodeType::OrderByMaterializedSort,
        };
        root.children.push(empty_execution_node_descriptor(
            order_node_type,
            execution_mode,
        ));
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

    // Phase 2: emit deterministic route-level diagnostics used by verbose surfaces.
    let mut lines = vec![
        format!(
            "diagnostic.route.execution_mode={:?}",
            route_plan.execution_mode
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
            route_plan.continuation().window().limit()
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

    // Phase 2: project route-owned ordering + execution semantics into explain fields.
    let ordering_source = match route_plan.aggregate_seek_spec() {
        Some(AggregateSeekSpec::First { fetch }) => {
            ExplainExecutionOrderingSource::IndexSeekFirst { fetch }
        }
        Some(AggregateSeekSpec::Last { fetch }) => {
            ExplainExecutionOrderingSource::IndexSeekLast { fetch }
        }
        None if matches!(route_plan.execution_mode, ExecutionMode::Materialized) => {
            ExplainExecutionOrderingSource::Materialized
        }
        None => ExplainExecutionOrderingSource::AccessOrder,
    };
    let execution_mode = explain_execution_mode(route_plan.execution_mode);
    let node_properties = explain_node_properties_for_route(&route_plan);

    // Phase 3: emit one stable descriptor payload consumed by explain surfaces.
    ExplainExecutionDescriptor {
        access_strategy: ExplainAccessRoute::from_access_plan(&plan.access),
        // Scalar aggregate id/exists/count terminals do not project row fields.
        covering_projection: false,
        aggregation,
        execution_mode,
        ordering_source,
        limit: route_plan.continuation().window().limit(),
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
        return vec![node];
    }

    let mut node = empty_execution_node_descriptor(
        ExplainExecutionNodeType::ResidualPredicateFilter,
        execution_mode,
    );
    node.residual_predicate = Some(explain_predicate);

    vec![node]
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

const fn explain_execution_mode(mode: ExecutionMode) -> ExplainExecutionMode {
    match mode {
        ExecutionMode::Streaming => ExplainExecutionMode::Streaming,
        ExecutionMode::Materialized => ExplainExecutionMode::Materialized,
    }
}

fn explain_node_properties_for_route(route_plan: &ExecutionRoutePlan) -> BTreeMap<String, Value> {
    let mut node_properties = BTreeMap::new();

    // Keep seek metadata additive and node-local so explain schema can evolve
    // without introducing new top-level descriptor fields for each route hint.
    if let Some(fetch) = route_plan.aggregate_seek_fetch_hint() {
        node_properties.insert("fetch".to_string(), Value::from(u64_from_usize(fetch)));
    }

    node_properties
}

const fn u64_from_usize(value: usize) -> u64 {
    value as u64
}
