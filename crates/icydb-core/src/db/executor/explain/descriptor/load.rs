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
                ExecutionRouteShape, LoadTerminalFastPathContract, TopNSeekSpec,
                build_initial_execution_route_plan_for_load_with_model,
            },
        },
        predicate::IndexPredicateCapability,
        query::{
            explain::{
                ExplainAccessPath as ExplainAccessRoute, ExplainExecutionMode,
                ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
                write_access_strategy_label,
            },
            plan::{
                AccessPlannedQuery, CoveringProjectionOrder, CoveringReadFieldSource,
                project_access_choice_explain_snapshot,
            },
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
    order_by_execution_node_descriptor, predicate_index_capability_label,
    predicate_stage_descriptors, projection_field_label, route_diagnostic_line_bool,
    route_diagnostic_line_debug, route_fetch_diagnostic_line, secondary_order_pushdown_descriptor,
    secondary_order_pushdown_verbose_line, top_n_seek_descriptor,
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
    let has_residual_predicate = plan.has_residual_predicate();
    let strict_predicate_compatible = !has_residual_predicate
        || predicate_index_capability == Some(IndexPredicateCapability::FullyIndexable);
    let execution_mode = explain_execution_mode(route_shape);
    let load_terminal_fast_path = route_plan.load_terminal_fast_path();

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
    let covering_scan = load_terminal_fast_path.is_some();
    root.covering_scan = Some(covering_scan);
    root.node_properties.insert(
        "cov_scan_reason",
        Value::from(load_covering_scan_reason_for_model(
            plan,
            strict_predicate_compatible,
            load_terminal_fast_path,
        )),
    );
    if let Some(capability) = predicate_index_capability {
        root.node_properties.insert(
            "pred_idx_cap",
            Value::from(predicate_index_capability_label(capability)),
        );
    }
    annotate_projection_pushdown_node_properties(&mut root, model, plan, covering_scan);
    annotate_covering_read_route_node_properties(&mut root, load_terminal_fast_path);
    annotate_fast_path_reason_node_properties(&mut root, &route_plan);

    // Phase 3: project route/planner modifiers in execution order as descriptor children.
    let explain_predicate = if has_residual_predicate {
        explain_predicate_for_plan(plan)
    } else {
        None
    };
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
        load_terminal_fast_path,
    ));

    Ok(root)
}

fn load_modifier_execution_nodes(
    plan: &AccessPlannedQuery,
    route_plan: &crate::db::executor::route::ExecutionRoutePlan,
    execution_mode: ExplainExecutionMode,
    route_shape: ExecutionRouteShape,
    load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
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
    if let Some(node) =
        covering_projection_execution_node_descriptor(load_terminal_fast_path, execution_mode)
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
    let has_residual_predicate = plan.has_residual_predicate();
    let strict_predicate_compatible = !has_residual_predicate
        || execution_preparation_predicate_index_capability(&execution_preparation)
            == Some(IndexPredicateCapability::FullyIndexable);
    let projected_fields = plan
        .projection_spec(model)
        .fields()
        .map(projection_field_label)
        .map(Cow::into_owned)
        .collect::<Vec<_>>();
    let load_terminal_fast_path = route_plan.load_terminal_fast_path();
    let projection_pushdown = load_terminal_fast_path.is_some();
    let access_strategy = ExplainAccessRoute::from_access_plan(&plan.access);
    let access_choice = project_access_choice_explain_snapshot(model, plan, &access_strategy);
    let mut chosen_label = String::new();
    write_access_strategy_label(&mut chosen_label, &access_strategy);
    let rejections = access_choice.rejected.into_iter().collect::<Vec<_>>();

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
    let predicate_stage = if !has_residual_predicate {
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
        "diag.r.covering_read",
        load_covering_scan_reason_for_model(
            plan,
            strict_predicate_compatible,
            load_terminal_fast_path,
        ),
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

// Keep scalar covering-read explain labels local to the load descriptor so the
// route-owned contract and explain payload stay in lockstep.
fn load_covering_scan_reason_for_model(
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
    load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
) -> &'static str {
    if load_terminal_fast_path.is_some() {
        return "cover_read_route";
    }
    if plan.scalar_plan().order.is_some() {
        return "order_mat";
    }
    let index_shape_supported =
        plan.access.as_index_prefix_path().is_some() || plan.access.as_index_range_path().is_some();
    if !index_shape_supported {
        return "access_not_cov";
    }
    if plan.has_residual_predicate() && !strict_predicate_compatible {
        return "pred_not_strict";
    }
    if plan.scalar_plan().distinct {
        return "distinct_mat";
    }

    "proj_not_cov"
}

// Annotate the access root with one stable scalar covering-read route label.
fn annotate_covering_read_route_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
) {
    let route_label = if load_terminal_fast_path.is_some() {
        "covering_read"
    } else {
        "materialized"
    };
    node.node_properties
        .insert("cov_read_route", Value::from(route_label));
}

// Emit one explicit projection terminal node when the scalar load route stays
// on the planner-owned covering-read contract.
fn covering_projection_execution_node_descriptor(
    load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
    execution_mode: ExplainExecutionMode,
) -> Option<ExplainExecutionNodeDescriptor> {
    let LoadTerminalFastPathContract::CoveringRead(covering) = load_terminal_fast_path?;
    let mut node =
        crate::db::executor::explain::descriptor::shared::empty_execution_node_descriptor(
            ExplainExecutionNodeType::CoveringRead,
            execution_mode,
        );
    node.projection = Some("covering_read".to_string());
    node.covering_scan = Some(true);
    node.node_properties.insert(
        "covering_order",
        Value::from(covering_read_order_contract_label(covering.order_contract)),
    );
    node.node_properties.insert(
        "covering_fields",
        Value::List(
            covering
                .fields
                .iter()
                .map(|field| Value::from(field.field_slot.field().to_string()))
                .collect(),
        ),
    );
    node.node_properties.insert(
        "covering_sources",
        Value::List(
            covering
                .fields
                .iter()
                .map(|field| Value::from(covering_read_field_source_label(&field.source)))
                .collect(),
        ),
    );

    Some(node)
}

const fn covering_read_order_contract_label(
    order_contract: CoveringProjectionOrder,
) -> &'static str {
    match order_contract {
        CoveringProjectionOrder::IndexOrder(crate::db::direction::Direction::Asc) => "index_asc",
        CoveringProjectionOrder::IndexOrder(crate::db::direction::Direction::Desc) => "index_desc",
        CoveringProjectionOrder::PrimaryKeyOrder(crate::db::direction::Direction::Asc) => {
            "primary_key_asc"
        }
        CoveringProjectionOrder::PrimaryKeyOrder(crate::db::direction::Direction::Desc) => {
            "primary_key_desc"
        }
    }
}

const fn covering_read_field_source_label(source: &CoveringReadFieldSource) -> &'static str {
    match source {
        CoveringReadFieldSource::IndexComponent { .. } => "index_component",
        CoveringReadFieldSource::PrimaryKey => "primary_key",
        CoveringReadFieldSource::Constant(_) => "constant",
    }
}
