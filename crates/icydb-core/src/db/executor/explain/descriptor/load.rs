//! Module: db::executor::explain::descriptor::load
//! Responsibility: module-local ownership and contracts for db::executor::explain::descriptor::load.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::{
            ExecutionPreparation,
            preparation::slot_map_for_model_plan,
            route::{
                ExecutionRouteShape, TopNSeekSpec,
                build_initial_execution_route_plan_for_load_with_model,
            },
        },
        predicate::IndexPredicateCapability,
        query::{
            explain::{
                ExplainAccessPath as ExplainAccessRoute, ExplainExecutionMode,
                ExplainExecutionNodeDescriptor, write_access_strategy_label,
            },
            plan::{AccessPlannedQuery, project_access_choice_explain_snapshot},
        },
    },
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;

use crate::db::executor::explain::descriptor::shared::{
    annotate_access_choice_node_properties, annotate_access_root_node_properties,
    annotate_fast_path_reason_node_properties, annotate_projection_pushdown_node_properties,
    cursor_resume_execution_node_descriptor, descriptor_route_property_line,
    distinct_execution_node_descriptor, execution_preparation_predicate_index_capability,
    explain_execution_mode, explain_predicate_for_plan, index_range_limit_pushdown_descriptor,
    load_covering_scan_eligible, load_covering_scan_reason, order_by_execution_node_descriptor,
    predicate_index_capability_label, predicate_stage_descriptors, projection_field_label,
    route_diagnostic_line_bool, route_diagnostic_line_debug, route_fetch_diagnostic_line,
    secondary_order_pushdown_descriptor, secondary_order_pushdown_verbose_line,
    top_n_seek_descriptor,
};

// Assemble one canonical scalar load execution descriptor tree through one
// model-owned authority path.
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
    let mut root =
        crate::db::executor::explain::descriptor::shared::access_execution_node_descriptor(
            access_strategy,
            execution_mode,
        );
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
    root.children.extend(load_modifier_execution_nodes(
        plan,
        &route_plan,
        execution_mode,
        route_shape,
    ));

    Ok(root)
}

fn load_modifier_execution_nodes(
    plan: &AccessPlannedQuery,
    route_plan: &crate::db::executor::route::ExecutionRoutePlan,
    execution_mode: ExplainExecutionMode,
    route_shape: ExecutionRouteShape,
) -> Vec<ExplainExecutionNodeDescriptor> {
    let mut nodes = Vec::new();

    // Phase 1: emit route-owned pushdown and seek modifiers in access execution order.
    for node in [
        secondary_order_pushdown_descriptor(route_plan, execution_mode),
        index_range_limit_pushdown_descriptor(route_plan, execution_mode),
        top_n_seek_descriptor(route_plan, execution_mode),
    ]
    .into_iter()
    .flatten()
    {
        nodes.push(node);
    }

    // Phase 2: emit planner-owned post-access modifiers that depend on route shape,
    // distinct strategy, and continuation state.
    if let Some(node) = order_by_execution_node_descriptor(
        plan.scalar_plan().order.is_some(),
        route_shape,
        execution_mode,
    ) {
        nodes.push(node);
    }
    if let Some(node) =
        distinct_execution_node_descriptor(plan.distinct_execution_strategy(), execution_mode)
    {
        nodes.push(node);
    }
    if let Some(page) = plan.scalar_plan().page.as_ref() {
        nodes.push(crate::db::executor::explain::descriptor::shared::limit_offset_execution_node_descriptor(
            page,
            route_plan,
            execution_mode,
        ));
    }
    if let Some(node) = cursor_resume_execution_node_descriptor(route_plan, execution_mode) {
        nodes.push(node);
    }

    nodes
}

// Assemble canonical verbose diagnostics for one scalar load route through one
// model-owned authority path.
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
        .map(Cow::into_owned)
        .collect::<Vec<_>>();
    let projection_pushdown = load_covering_scan_eligible(plan, strict_predicate_compatible);
    let access_strategy = ExplainAccessRoute::from_access_plan(&plan.access);
    let access_choice = project_access_choice_explain_snapshot(model, plan, &access_strategy);
    let mut chosen_label = String::new();
    write_access_strategy_label(&mut chosen_label, &access_strategy);
    let rejections = access_choice
        .rejected
        .into_iter()
        .map(|entry| entry.render())
        .collect::<Vec<_>>();

    // Phase 2: emit deterministic route-level diagnostics used by verbose surfaces.
    let mut lines = vec![
        route_diagnostic_line_debug("execution_mode", &route_plan.shape().execution_mode()),
        route_diagnostic_line_bool(
            "continuation_applied",
            route_plan.continuation().capabilities().applied(),
        ),
        route_diagnostic_line_debug("limit", &route_plan.continuation().limit()),
        route_diagnostic_line_debug("fast_path_order", &route_plan.fast_path_order()),
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
    let predicate_stage = if plan.scalar_plan().predicate.is_none() {
        "none"
    } else if strict_predicate_compatible {
        "index_prefilter(strict_all_or_none)"
    } else {
        "residual_post_access"
    };
    lines.push(descriptor_route_property_line(
        "diag.r.predicate_stage",
        predicate_stage,
    ));
    lines.push(route_diagnostic_line_debug(
        "projected_fields",
        &projected_fields,
    ));
    lines.push(route_diagnostic_line_bool(
        "projection_pushdown",
        projection_pushdown,
    ));
    lines.push(descriptor_route_property_line(
        "diag.r.access_choice_chosen",
        &chosen_label,
    ));
    lines.push(descriptor_route_property_line(
        "diag.r.access_choice_chosen_reason",
        access_choice.chosen_reason.code(),
    ));
    lines.push(route_diagnostic_line_debug(
        "access_choice_alternatives",
        &access_choice.alternatives,
    ));
    lines.push(route_diagnostic_line_debug(
        "access_choice_rejections",
        &rejections,
    ));
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
