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
        direction::Direction,
        executor::{
            ExecutionPreparation,
            aggregate::AggregateFoldMode,
            preparation::slot_map_for_model_plan,
            route::{
                AggregateSeekSpec, ContinuationMode, ExecutionRoutePlan, ExecutionRouteShape,
                FastPathOrder, TopNSeekSpec,
                build_execution_route_plan_for_aggregate_spec_with_model,
                build_initial_execution_route_plan_for_load_with_model,
            },
        },
        predicate::{IndexPredicateCapability, PredicateCapabilityProfile},
        query::{
            builder::AggregateExpr,
            explain::{
                ExplainAccessPath as ExplainAccessRoute, ExplainExecutionDescriptor,
                ExplainExecutionMode, ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
                ExplainExecutionOrderingSource, ExplainPredicate,
            },
            plan::{
                AccessChoiceExplainSnapshot, AccessPlannedQuery, AggregateKind,
                DistinctExecutionStrategy,
                expr::{Expr, ProjectionField},
                index_covering_existing_rows_terminal_eligible,
                project_access_choice_explain_snapshot,
            },
        },
    },
    error::InternalError,
    value::Value,
};
use std::{collections::BTreeMap, ops::Bound};

// Assemble one canonical scalar load execution descriptor tree through one
// model-owned structural authority path.
#[inline(never)]
pub(in crate::db) fn assemble_load_execution_node_descriptor_with_model(
    model: &'static crate::model::entity::EntityModel,
    plan: &AccessPlannedQuery,
) -> Result<ExplainExecutionNodeDescriptor, InternalError> {
    // Phase 1: build canonical reusable preparation and route contracts for load mode.
    let execution_preparation =
        ExecutionPreparation::from_plan(model, plan, slot_map_for_model_plan(model, plan));
    let route_plan = build_initial_execution_route_plan_for_load_with_model(model, plan, None)?;
    let route_shape = route_plan.shape();
    let predicate_index_capability =
        execution_preparation_predicate_index_capability(&execution_preparation);
    let strict_predicate_compatible =
        predicate_index_capability == Some(IndexPredicateCapability::FullyIndexable);
    let execution_mode = explain_execution_mode(route_shape);

    // Phase 2: derive one canonical access projection and reuse it across
    // descriptor assembly instead of re-projecting the chosen route again.
    let access_strategy = ExplainAccessRoute::from_access_plan(&plan.access);
    let access_choice = project_access_choice_explain_snapshot(model, plan, &access_strategy);
    let mut root = access_execution_node_descriptor(access_strategy, execution_mode);
    annotate_access_root_node_properties(&mut root, &route_plan);
    annotate_access_choice_node_properties(&mut root, access_choice);
    let covering_scan = load_covering_scan_eligible(plan, strict_predicate_compatible);
    root.covering_scan = Some(covering_scan);
    root.node_properties.insert(
        "cov_scan_reason",
        Value::from(load_covering_scan_reason(plan, strict_predicate_compatible)),
    );
    if let Some(capability) = predicate_index_capability {
        root.node_properties.insert(
            "pred_idx_cap",
            Value::from(predicate_index_capability_label(capability)),
        );
    }
    annotate_projection_pushdown_node_properties(&mut root, model, plan, covering_scan);
    annotate_fast_path_reason_node_properties(&mut root, &route_plan);

    // Phase 3: project route/planner modifiers in execution order as descriptor children.
    let explain_predicate = explain_predicate_for_plan(plan);
    for predicate_stage in predicate_stage_descriptors(
        explain_predicate,
        root.access_strategy.as_ref(),
        strict_predicate_compatible,
        execution_mode,
    ) {
        root.children.push(predicate_stage);
    }

    for node in [
        secondary_order_pushdown_descriptor(&route_plan, execution_mode),
        index_range_limit_pushdown_descriptor(&route_plan, execution_mode),
        top_n_seek_descriptor(&route_plan, execution_mode),
    ]
    .into_iter()
    .flatten()
    {
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
            "order_by_idx",
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
        node.node_properties
            .insert("offset", Value::from(u64_from_usize(page.offset as usize)));
        root.children.push(node);
    }

    if route_plan.continuation().capabilities().applied() {
        let mut node =
            empty_execution_node_descriptor(ExplainExecutionNodeType::CursorResume, execution_mode);
        node.cursor = Some(true);
        annotate_cursor_resume_node_properties(&mut node, &route_plan);
        root.children.push(node);
    }

    Ok(root)
}

// Assemble canonical verbose diagnostics for one scalar load route through one
// model-owned structural authority path.
pub(in crate::db) fn assemble_load_execution_verbose_diagnostics_with_model(
    model: &'static crate::model::entity::EntityModel,
    plan: &AccessPlannedQuery,
) -> Result<Vec<String>, InternalError> {
    // Phase 1: build canonical route authority inputs for load mode.
    let execution_preparation =
        ExecutionPreparation::from_plan(model, plan, slot_map_for_model_plan(model, plan));
    let route_plan = build_initial_execution_route_plan_for_load_with_model(model, plan, None)?;
    let strict_predicate_compatible =
        execution_preparation_predicate_index_capability(&execution_preparation)
            == Some(IndexPredicateCapability::FullyIndexable);
    let projected_fields = plan
        .projection_spec(model)
        .fields()
        .map(projection_field_label)
        .collect::<Vec<_>>();
    let projection_pushdown = load_covering_scan_eligible(plan, strict_predicate_compatible);
    let access_strategy = ExplainAccessRoute::from_access_plan(&plan.access);
    let access_choice = project_access_choice_explain_snapshot(model, plan, &access_strategy);
    let rejections = access_choice
        .rejected
        .into_iter()
        .map(|entry| entry.render())
        .collect::<Vec<_>>();

    // Phase 2: emit deterministic route-level diagnostics used by verbose surfaces.
    let mut lines = vec![
        format!(
            "diag.r.execution_mode={:?}",
            route_plan.shape().execution_mode()
        ),
        format!(
            "diag.r.continuation_applied={}",
            route_plan.continuation().capabilities().applied()
        ),
        format!("diag.r.limit={:?}", route_plan.continuation().limit()),
        format!("diag.r.fast_path_order={:?}", route_plan.fast_path_order()),
        secondary_order_pushdown_verbose_line(&route_plan),
    ];
    lines.push(route_fetch_diagnostic_line(
        "top_n_seek",
        route_plan.top_n_seek_spec().map(TopNSeekSpec::fetch),
    ));
    lines.push(route_fetch_diagnostic_line(
        "index_range_limit_pushdown",
        route_plan.index_range_limit_spec.map(|spec| spec.fetch),
    ));
    lines.push(format!(
        "diag.r.predicate_stage={}",
        if plan.scalar_plan().predicate.is_none() {
            "none".to_string()
        } else if strict_predicate_compatible {
            "index_prefilter(strict_all_or_none)".to_string()
        } else {
            "residual_post_access".to_string()
        }
    ));
    lines.push(format!("diag.r.projected_fields={projected_fields:?}"));
    lines.push(format!("diag.r.projection_pushdown={projection_pushdown}"));
    lines.push(format!(
        "diag.r.access_choice_chosen={}",
        access_choice.chosen_label
    ));
    lines.push(format!(
        "diag.r.access_choice_chosen_reason={}",
        access_choice.chosen_reason.code()
    ));
    lines.push(format!(
        "diag.r.access_choice_alternatives={:?}",
        access_choice.alternatives
    ));
    lines.push(format!("diag.r.access_choice_rejections={rejections:?}"));
    if let Some(capability) =
        execution_preparation_predicate_index_capability(&execution_preparation)
    {
        lines.push(descriptor_route_property_line(
            "diag.r.predicate_index_capability",
            predicate_index_capability_label(capability),
        ));
    }

    Ok(lines)
}

// Assemble one canonical scalar aggregate execution descriptor through one
// model-owned structural authority path.
#[inline(never)]
pub(in crate::db) fn assemble_aggregate_terminal_execution_descriptor_with_model(
    model: &'static crate::model::entity::EntityModel,
    plan: &AccessPlannedQuery,
    aggregate: AggregateExpr,
) -> ExplainExecutionDescriptor {
    let aggregation = aggregate.kind();
    let projected_field = aggregate.target_field().map(str::to_string);

    // Phase 1: derive one aggregate route plan using precomputed execution preparation.
    let execution_preparation =
        ExecutionPreparation::from_plan(model, plan, slot_map_for_model_plan(model, plan));
    let route_plan = build_execution_route_plan_for_aggregate_spec_with_model(
        model,
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
    let node_properties = explain_node_properties_for_route(
        &route_plan,
        aggregation,
        projected_field.as_deref(),
        covering_projection,
    );

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

    if let ExplainAccessRoute::Union(children) | ExplainAccessRoute::Intersection(children) =
        access_strategy
    {
        for child in children {
            node.children
                .push(access_execution_node_descriptor(child, execution_mode));
        }
    }

    node
}

fn annotate_access_root_node_properties(
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

// Scalar-load covering projection reflects planner-side index-covering
// existing-row eligibility under current strict predicate contracts.
fn load_covering_scan_eligible(
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
) -> bool {
    index_covering_existing_rows_terminal_eligible(plan, strict_predicate_compatible)
}

fn load_covering_scan_reason(
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
) -> &'static str {
    if plan.scalar_plan().order.is_some() {
        return "order_mat";
    }

    let index_shape_supported =
        plan.access.as_index_prefix_path().is_some() || plan.access.as_index_range_path().is_some();
    if !index_shape_supported {
        return "access_not_cov";
    }

    if plan.scalar_plan().predicate.is_some() && !strict_predicate_compatible {
        return "pred_not_strict";
    }

    "idx_cover_rows"
}

fn annotate_projection_pushdown_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    model: &'static crate::model::entity::EntityModel,
    plan: &AccessPlannedQuery,
    covering_scan: bool,
) {
    let projected_fields = plan
        .projection_spec(model)
        .fields()
        .map(projection_field_label)
        .map(Value::from)
        .collect();
    node.node_properties
        .insert("proj_fields", Value::List(projected_fields));
    node.node_properties
        .insert("proj_pushdown", Value::from(covering_scan));
}

fn projection_field_label(field: &ProjectionField) -> String {
    match field {
        ProjectionField::Scalar { expr, .. } => projection_expr_label(expr),
    }
}

// Keep projection metadata deterministic and planner-owned by reducing each
// expression to one stable field-like label for explain projection output.
fn projection_expr_label(expr: &Expr) -> String {
    match expr {
        Expr::Field(field) => field.as_str().to_string(),
        Expr::Alias { expr, .. } | Expr::Unary { expr, .. } => projection_expr_label(expr),
        Expr::Aggregate(aggregate) => aggregate
            .target_field()
            .map_or_else(|| "aggregate".to_string(), str::to_string),
        Expr::Literal(_) => "literal".to_string(),
        Expr::Binary { .. } => "expr".to_string(),
    }
}

fn annotate_access_choice_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    access_choice: AccessChoiceExplainSnapshot,
) {
    node.node_properties
        .insert("acc_choice", Value::from(access_choice.chosen_label));
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
        .map(|entry| Value::from(entry.render()))
        .collect();
    node.node_properties
        .insert("acc_reject", Value::List(rejected));
}

fn descriptor_route_property_line(line_key: &str, property_value: &str) -> String {
    format!("{line_key}={property_value}")
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
    if let Some(ExplainAccessRoute::IndexMultiLookup { values, .. }) = access_strategy {
        Some(values.clone())
    } else {
        None
    }
}

fn scan_fetch_pushdown(route_plan: &ExecutionRoutePlan) -> Option<usize> {
    route_plan
        .top_n_seek_spec()
        .map(TopNSeekSpec::fetch)
        .or_else(|| route_plan.index_range_limit_spec.map(|spec| spec.fetch))
}

fn annotate_cursor_resume_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    route_plan: &ExecutionRoutePlan,
) {
    annotate_continuation_node_properties(
        node,
        route_plan.direction(),
        route_plan.continuation().capabilities().mode(),
    );
}

fn annotate_fast_path_reason_node_properties(
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
            rejections.push(Value::from(format!(
                "{}={}",
                fast_path_label(*route),
                fast_path_rejection_reason(*route, route_plan),
            )));
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
    node.node_properties.insert("index", Value::from(*index));
    node.node_properties
        .insert("prefix_len", Value::from(u64_from_usize(*prefix_len)));

    Some(node)
}

fn secondary_order_pushdown_verbose_line(route_plan: &ExecutionRoutePlan) -> String {
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
            format!(
                "diag.r.secondary_order_pushdown=rejected({})",
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
    Some(fetch_execution_node_descriptor(
        ExplainExecutionNodeType::IndexRangeLimitPushdown,
        execution_mode,
        spec.fetch,
    ))
}

fn top_n_seek_descriptor(
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

fn execution_preparation_predicate_index_capability(
    execution_preparation: &ExecutionPreparation,
) -> Option<IndexPredicateCapability> {
    execution_preparation
        .predicate_capability_profile()
        .map(PredicateCapabilityProfile::index)
}

const fn predicate_index_capability_label(capability: IndexPredicateCapability) -> &'static str {
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

fn explain_predicate_for_plan(plan: &AccessPlannedQuery) -> Option<ExplainPredicate> {
    plan.scalar_plan()
        .predicate
        .as_ref()
        .map(ExplainPredicate::from_predicate)
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
    projected_field: Option<&str>,
    covering_projection: bool,
) -> BTreeMap<&'static str, Value> {
    let mut node_properties = BTreeMap::new();

    // Keep seek metadata additive and node-local so explain schema can evolve
    // without introducing new top-level descriptor fields for each route hint.
    if let Some(fetch) = route_plan.aggregate_seek_fetch_hint() {
        node_properties.insert("fetch", Value::from(u64_from_usize(fetch)));
    }
    if matches!(aggregation, AggregateKind::Count) {
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
    if has_projected_field {
        if covering_projection {
            "field_idx"
        } else {
            "field_mat"
        }
    } else {
        match aggregation {
            AggregateKind::Count
            | AggregateKind::Exists
            | AggregateKind::Sum
            | AggregateKind::Avg => "scalar_agg",
            AggregateKind::Min
            | AggregateKind::Max
            | AggregateKind::First
            | AggregateKind::Last => "entity_term",
        }
    }
}

const fn aggregate_fold_mode_label(mode: AggregateFoldMode) -> &'static str {
    match mode {
        AggregateFoldMode::ExistingRows => "rows",
        AggregateFoldMode::KeysOnly => "keys",
    }
}

// Return whether one scalar aggregate terminal can remain index-only under the
// current plan and executor preparation contracts.
fn aggregate_covering_projection_for_terminal(
    plan: &AccessPlannedQuery,
    aggregation: AggregateKind,
    execution_preparation: &ExecutionPreparation,
) -> bool {
    let strict_predicate_compatible =
        execution_preparation_predicate_index_capability(execution_preparation)
            == Some(IndexPredicateCapability::FullyIndexable);

    match aggregation {
        AggregateKind::Count | AggregateKind::Exists => {
            index_covering_existing_rows_terminal_eligible(plan, strict_predicate_compatible)
        }
        AggregateKind::Min
        | AggregateKind::Max
        | AggregateKind::First
        | AggregateKind::Last
        | AggregateKind::Sum
        | AggregateKind::Avg => false,
    }
}

fn route_fetch_diagnostic_line(label: &str, fetch: Option<usize>) -> String {
    if let Some(fetch) = fetch {
        format!("diag.r.{label}=fetch({})", u64_from_usize(fetch))
    } else {
        format!("diag.r.{label}=disabled")
    }
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
