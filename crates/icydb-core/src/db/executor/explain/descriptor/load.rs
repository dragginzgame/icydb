//! Module: db::executor::explain::descriptor::load
//! Responsibility: assemble stable EXPLAIN descriptor trees and verbose route
//! diagnostics for scalar load execution plans.
//! Does not own: route derivation policy or final explain rendering formats.
//! Boundary: projects executor route/planner contracts into descriptor nodes and diagnostics lines.

use crate::{
    db::{
        executor::{
            ExecutionPreparation,
            planning::{preparation::slot_map_for_model_plan, route::GroupedExecutionMode},
            route::{
                ExecutionRoutePlan, LoadTerminalFastPathContract, TopNSeekSpec,
                access_order_satisfied_by_route_contract,
                build_execution_route_plan_for_grouped_plan,
                build_initial_execution_route_plan_for_load_with_fast_path,
            },
        },
        query::{
            explain::{
                ExplainAccessPath as ExplainAccessRoute, ExplainExecutionMode,
                ExplainExecutionNodeDescriptor, ExplainExecutionNodeType, ExplainPredicate,
                write_access_strategy_label,
            },
            plan::{
                AccessPlannedQuery, CoveringExistingRowMode, CoveringProjectionOrder,
                CoveringReadFieldSource, covering_read_execution_plan_from_fields,
                covering_read_reason_code_for_load_plan, covering_strict_predicate_compatible,
                grouped_executor_handoff,
            },
        },
    },
    error::InternalError,
    model::field::FieldModel,
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
// field-table and primary-key explain boundary.
#[inline(never)]
pub(in crate::db) fn assemble_load_execution_node_descriptor(
    fields: &'static [FieldModel],
    primary_key_name: &'static str,
    plan: &AccessPlannedQuery,
) -> Result<ExplainExecutionNodeDescriptor, InternalError> {
    let route_plan = build_execution_route_plan_for_explain(fields, primary_key_name, plan)?;

    Ok(assemble_load_execution_node_descriptor_with_route_plan(
        plan,
        &route_plan,
    ))
}

// Assemble one canonical scalar load execution descriptor tree through one
// caller-supplied route plan.
fn assemble_load_execution_node_descriptor_with_route_plan(
    plan: &AccessPlannedQuery,
    route_plan: &ExecutionRoutePlan,
) -> ExplainExecutionNodeDescriptor {
    // Phase 1: build canonical reusable preparation and route contracts for load mode.
    let execution_preparation =
        ExecutionPreparation::from_plan(plan, slot_map_for_model_plan(plan));
    let predicate_index_capability =
        execution_preparation_predicate_index_capability(&execution_preparation);
    let logical_predicate = plan.scalar_plan().predicate.as_ref();
    let strict_predicate_compatible =
        covering_strict_predicate_compatible(plan, predicate_index_capability);
    let execution_mode = explain_execution_mode(route_plan);
    let load_terminal_fast_path = route_plan.load_terminal_fast_path();

    // Phase 2: derive one canonical access projection and reuse it across
    // descriptor assembly instead of re-projecting the chosen route again.
    let access_strategy = ExplainAccessRoute::from_access_plan(&plan.access);
    let mut root =
        crate::db::executor::explain::descriptor::shared::access_execution_node_descriptor(
            access_strategy,
            execution_mode,
        );
    annotate_access_root_node_properties(&mut root, route_plan);
    annotate_load_order_route_node_properties(&mut root, route_plan);
    annotate_access_choice_node_properties(&mut root, plan.access_choice().clone());
    let covering_scan = load_terminal_fast_path.is_some();
    root.covering_scan = Some(covering_scan);
    root.node_properties.insert(
        "cov_scan_reason",
        Value::from(covering_read_reason_code_for_load_plan(
            plan,
            strict_predicate_compatible,
            load_terminal_fast_path.is_some(),
        )),
    );
    annotate_grouped_route_node_properties(&mut root, route_plan);
    if let Some(capability) = predicate_index_capability {
        root.node_properties.insert(
            "pred_idx_cap",
            Value::from(predicate_index_capability_label(capability)),
        );
    }
    annotate_projection_pushdown_node_properties(&mut root, plan, covering_scan);
    annotate_covering_read_route_node_properties(&mut root, load_terminal_fast_path);
    annotate_fast_path_reason_node_properties(&mut root, route_plan);

    // Phase 3: project route/planner modifiers in execution order as descriptor children.
    let explain_predicate = if strict_predicate_compatible {
        logical_predicate.map(ExplainPredicate::from_predicate)
    } else {
        explain_predicate_for_plan(plan)
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
        route_plan,
        execution_mode,
        load_terminal_fast_path,
    ));

    root
}

fn load_modifier_execution_nodes(
    plan: &AccessPlannedQuery,
    route_plan: &ExecutionRoutePlan,
    execution_mode: ExplainExecutionMode,
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
        explain_access_order_satisfied(plan, load_terminal_fast_path),
        execution_mode,
    ) {
        nodes.push(node);
    }
    if let Some(node) = grouped_aggregate_execution_node_descriptor(route_plan, execution_mode) {
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

// EXPLAIN needs a slightly narrower access-order signal than the generic route
// contract. Covering-read terminals keep index order intact even when the full
// row lane materializes, while non-unique bounded range scans over multiple
// ordered suffix fields still need a fail-closed materialized sort contract.
fn explain_access_order_satisfied(
    plan: &AccessPlannedQuery,
    load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
) -> bool {
    if !access_order_satisfied_by_route_contract(plan) {
        return false;
    }

    let access_class = plan.access_strategy().class();
    let Some(order_contract) =
        plan.scalar_plan().order.as_ref().and_then(|order| {
            order.deterministic_secondary_order_contract(plan.primary_key_name())
        })
    else {
        return true;
    };

    if let Some((index, prefix_len)) = access_class.single_path_index_prefix_details() {
        if !index.is_unique()
            && prefix_len > 0
            && matches!(
                order_contract.direction(),
                crate::db::query::plan::OrderDirection::Desc
            )
        {
            return false;
        }
    }

    if load_terminal_fast_path.is_some() {
        return true;
    }

    let Some((index, prefix_len)) = access_class.single_path_index_range_details() else {
        return true;
    };
    if index.is_unique() {
        return true;
    }
    if prefix_len == 0 {
        return true;
    }

    order_contract.non_primary_key_terms().len() <= 1
}

// Assemble canonical verbose diagnostics for one scalar load route through one
// field-table and primary-key explain boundary.
pub(in crate::db) fn assemble_load_execution_verbose_diagnostics(
    fields: &'static [FieldModel],
    primary_key_name: &'static str,
    plan: &AccessPlannedQuery,
) -> Result<Vec<String>, InternalError> {
    let route_plan = build_execution_route_plan_for_explain(fields, primary_key_name, plan)?;

    Ok(assemble_load_execution_verbose_diagnostics_with_route_plan(
        plan,
        &route_plan,
    ))
}

// Assemble canonical verbose diagnostics for one scalar load route through one
// caller-supplied route plan.
fn assemble_load_execution_verbose_diagnostics_with_route_plan(
    plan: &AccessPlannedQuery,
    route_plan: &ExecutionRoutePlan,
) -> Vec<String> {
    // Phase 1: build canonical route/planner inputs for load mode.
    let execution_preparation =
        ExecutionPreparation::from_plan(plan, slot_map_for_model_plan(plan));
    let logical_predicate = plan.scalar_plan().predicate.as_ref();
    let strict_predicate_compatible = covering_strict_predicate_compatible(
        plan,
        execution_preparation_predicate_index_capability(&execution_preparation),
    );
    let projected_fields = plan
        .frozen_projection_spec()
        .fields()
        .map(projection_field_label)
        .map(Cow::into_owned)
        .collect::<Vec<_>>();
    let load_terminal_fast_path = route_plan.load_terminal_fast_path();
    let projection_pushdown = load_terminal_fast_path.is_some();
    let access_strategy = ExplainAccessRoute::from_access_plan(&plan.access);
    let access_choice = plan.access_choice().clone();
    let mut chosen_label = String::new();
    write_access_strategy_label(&mut chosen_label, &access_strategy);
    let rejections = access_choice.rejected.into_iter().collect::<Vec<_>>();

    // Phase 2: emit deterministic route-level diagnostics used by verbose surfaces.
    let mut lines = vec![
        route_diagnostic_line_debug("execution_mode", &route_plan.execution_mode()),
        route_diagnostic_line_bool("continuation_applied", route_plan.continuation().applied()),
        route_diagnostic_line_debug("limit", &route_plan.continuation().limit()),
        route_diagnostic_line_debug("fast_path_order", &route_plan.fast_path_order()),
        secondary_order_pushdown_verbose_line(route_plan),
    ];
    lines.push(route_fetch_diagnostic_line(
        "top_n_seek",
        route_plan.top_n_seek_spec().map(TopNSeekSpec::fetch),
    ));
    lines.push(route_fetch_diagnostic_line(
        "index_range_limit_pushdown",
        route_plan.index_range_limit_spec.map(|spec| spec.fetch),
    ));
    let predicate_stage = if logical_predicate.is_none() {
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
    let (load_order_route_contract, load_order_route_reason) =
        load_order_route_property_values(route_plan);
    lines.push(descriptor_route_property_line(
        "diag.r.load_order_route_contract",
        load_order_route_contract,
    ));
    lines.push(descriptor_route_property_line(
        "diag.r.load_order_route_reason",
        load_order_route_reason,
    ));
    lines.push(route_diagnostic_line_bool(
        "projection_pushdown",
        projection_pushdown,
    ));
    lines.push(descriptor_route_property_line(
        "diag.r.covering_read",
        covering_read_reason_code_for_load_plan(
            plan,
            strict_predicate_compatible,
            load_terminal_fast_path.is_some(),
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
    append_access_choice_verbose_diagnostics(
        &mut lines,
        &access_choice.alternatives,
        rejections.as_slice(),
    );
    append_grouped_route_verbose_diagnostics(&mut lines, route_plan);
    if let Some(capability) =
        execution_preparation_predicate_index_capability(&execution_preparation)
    {
        lines.push(descriptor_route_property_line(
            "diag.r.predicate_index_capability",
            predicate_index_capability_label(capability),
        ));
    }

    lines
}

fn append_access_choice_verbose_diagnostics(
    lines: &mut Vec<String>,
    alternatives: &(impl core::fmt::Debug + ?Sized),
    rejections: &(impl core::fmt::Debug + ?Sized),
) {
    lines.push(route_diagnostic_line_debug(
        "access_choice_alternatives",
        &alternatives,
    ));
    lines.push(route_diagnostic_line_debug(
        "access_choice_rejections",
        &rejections,
    ));
}

fn append_grouped_route_verbose_diagnostics(
    lines: &mut Vec<String>,
    route_plan: &ExecutionRoutePlan,
) {
    let Some((
        grouped_route_outcome,
        grouped_route_rejection_reason,
        grouped_plan_fallback_reason,
        _grouped_route_eligible,
        grouped_execution_mode,
    )) = grouped_route_property_values(route_plan)
    else {
        return;
    };
    lines.push(descriptor_route_property_line(
        "diag.r.grouped_route_outcome",
        grouped_route_outcome,
    ));
    lines.push(descriptor_route_property_line(
        "diag.r.grouped_route_rejection_reason",
        grouped_route_rejection_reason,
    ));
    lines.push(descriptor_route_property_line(
        "diag.r.grouped_plan_fallback_reason",
        grouped_plan_fallback_reason,
    ));
    lines.push(descriptor_route_property_line(
        "diag.r.grouped_execution_mode",
        grouped_execution_mode,
    ));
}

// Grouped execution descriptors must consume the planner-owned grouped handoff
// so explain does not silently rebuild a scalar load route for grouped plans.
fn build_execution_route_plan_for_explain(
    fields: &'static [FieldModel],
    primary_key_name: &'static str,
    plan: &AccessPlannedQuery,
) -> Result<ExecutionRoutePlan, InternalError> {
    if plan.grouped_plan().is_some() {
        let grouped_handoff = grouped_executor_handoff(plan)?;

        return Ok(build_execution_route_plan_for_grouped_plan(
            grouped_handoff.base(),
            grouped_handoff.grouped_plan_strategy(),
        ));
    }

    build_initial_execution_route_plan_for_load_with_fast_path(
        plan,
        None,
        derive_explain_load_terminal_fast_path_contract(fields, primary_key_name, plan),
    )
}

// Explain-only load routing derives covering-read eligibility from the same
// generated field table and frozen PK identity that planner/authority paths use.
fn derive_explain_load_terminal_fast_path_contract(
    fields: &'static [FieldModel],
    primary_key_name: &'static str,
    plan: &AccessPlannedQuery,
) -> Option<LoadTerminalFastPathContract> {
    if !plan.scalar_plan().mode.is_load() {
        return None;
    }

    let execution_preparation =
        ExecutionPreparation::from_plan(plan, slot_map_for_model_plan(plan));
    let strict_predicate_compatible = covering_strict_predicate_compatible(
        plan,
        execution_preparation
            .predicate_capability_profile()
            .map(crate::db::predicate::PredicateCapabilityProfile::index),
    );

    covering_read_execution_plan_from_fields(
        fields,
        plan,
        primary_key_name,
        strict_predicate_compatible,
    )
    .map(LoadTerminalFastPathContract::CoveringRead)
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

// Keep ordered-load route diagnostics local to the load descriptor so JSON and
// verbose explain stay projections of the same route-owned contract.
fn annotate_load_order_route_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    route_plan: &ExecutionRoutePlan,
) {
    let (load_order_route_contract, load_order_route_reason) =
        load_order_route_property_values(route_plan);
    node.node_properties
        .insert("ord_route_contract", Value::from(load_order_route_contract));
    node.node_properties
        .insert("ord_route_reason", Value::from(load_order_route_reason));
}

// Project grouped route observability directly onto the access root so the
// descriptor exposes planner fallback versus route rejection without inference.
fn annotate_grouped_route_node_properties(
    node: &mut ExplainExecutionNodeDescriptor,
    route_plan: &ExecutionRoutePlan,
) {
    let Some((
        grouped_route_outcome,
        grouped_route_rejection_reason,
        grouped_plan_fallback_reason,
        grouped_route_eligible,
        grouped_execution_mode,
    )) = grouped_route_property_values(route_plan)
    else {
        return;
    };
    node.node_properties
        .insert("grouped_route_outcome", Value::from(grouped_route_outcome));
    node.node_properties.insert(
        "grouped_route_rejection_reason",
        Value::from(grouped_route_rejection_reason),
    );
    node.node_properties.insert(
        "grouped_plan_fallback_reason",
        Value::from(grouped_plan_fallback_reason),
    );
    node.node_properties.insert(
        "grouped_route_eligible",
        Value::from(grouped_route_eligible),
    );
    node.node_properties.insert(
        "grouped_execution_mode",
        Value::from(grouped_execution_mode),
    );
}

// Emit one explicit grouped aggregate node so grouped execution descriptors
// expose grouped materialization shape as part of the execution tree.
fn grouped_aggregate_execution_node_descriptor(
    route_plan: &ExecutionRoutePlan,
    execution_mode: ExplainExecutionMode,
) -> Option<ExplainExecutionNodeDescriptor> {
    let grouped_observability = route_plan.grouped_observability()?;
    let node_type = match grouped_observability.grouped_execution_mode() {
        GroupedExecutionMode::HashMaterialized => {
            ExplainExecutionNodeType::GroupedAggregateHashMaterialized
        }
        GroupedExecutionMode::OrderedMaterialized => {
            ExplainExecutionNodeType::GroupedAggregateOrderedMaterialized
        }
    };
    let mut node =
        crate::db::executor::explain::descriptor::shared::empty_execution_node_descriptor(
            node_type,
            execution_mode,
        );
    annotate_grouped_route_node_properties(&mut node, route_plan);

    Some(node)
}

const fn load_order_route_property_values(
    route_plan: &ExecutionRoutePlan,
) -> (&'static str, &'static str) {
    (
        route_plan.load_order_route_contract().code(),
        route_plan.load_order_route_reason().code(),
    )
}

fn grouped_route_property_values(
    route_plan: &ExecutionRoutePlan,
) -> Option<(&'static str, &'static str, &'static str, bool, &'static str)> {
    let grouped_observability = route_plan.grouped_observability()?;

    Some((
        grouped_observability.outcome().code(),
        grouped_observability
            .rejection_reason()
            .map_or("none", |reason| reason.code()),
        grouped_observability
            .planner_fallback_reason()
            .map_or("none", |reason| reason.code()),
        grouped_observability.eligible(),
        grouped_observability.grouped_execution_mode().code(),
    ))
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
    node.node_properties.insert(
        "existing_row_mode",
        Value::from(covering_existing_row_mode_label(covering.existing_row_mode)),
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

const fn covering_existing_row_mode_label(
    existing_row_mode: CoveringExistingRowMode,
) -> &'static str {
    match existing_row_mode {
        CoveringExistingRowMode::ProvenByPlanner => "planner_proven",
        CoveringExistingRowMode::RequiresRowPresenceCheck => "row_check_required",
    }
}
