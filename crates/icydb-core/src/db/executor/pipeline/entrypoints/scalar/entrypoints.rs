//! Module: executor::pipeline::entrypoints::scalar::entrypoints
//! Responsibility: structural SQL scalar execution entrypoints.
//! Does not own: typed/dynamic response decoding or logical planning.
//! Boundary: consumes prepared structural runtime handoffs.

#[cfg(feature = "sql")]
use crate::db::executor::{
    ExecutionRoutePlan, PreparedLoadPlan, RetainedSlotLayout,
    pipeline::{
        contracts::ProjectionMaterializationMode,
        entrypoints::scalar::{
            runtime::{
                InitialScalarPlanRuntimeOptions,
                prepare_initial_scalar_route_runtime_from_plan_with_retained_slot_layout,
            },
            streaming::execute_prepared_scalar_kernel_row_sink_execution,
        },
    },
    terminal::KernelRow,
};
use crate::{
    db::{
        Db,
        executor::{
            PreparedScalarRuntimeHandoff,
            pipeline::{
                contracts::{CursorEmissionMode, StructuralCursorPage},
                entrypoints::scalar::{
                    materialized::execute_prepared_scalar_route_runtime_with_scan_count,
                    runtime::{
                        prepare_initial_scalar_retained_slot_page_runtime_from_handoff,
                        prepare_resumed_scalar_retained_slot_page_runtime_from_handoff,
                    },
                },
            },
        },
    },
    error::InternalError,
    traits::CanisterKind,
};

/// Execute one retained-slot initial scalar page from a prepared runtime handoff.
pub(in crate::db::executor) fn execute_initial_scalar_retained_slot_page_from_runtime_handoff_for_canister<
    C,
>(
    db: &Db<C>,
    debug: bool,
    prepared: PreparedScalarRuntimeHandoff,
    emit_cursor: bool,
    suppress_route_scan_hints: bool,
    enforced_scan_probe_limit: Option<usize>,
) -> Result<(StructuralCursorPage, usize), InternalError>
where
    C: CanisterKind,
{
    let mut prepared = prepare_initial_scalar_retained_slot_page_runtime_from_handoff(
        db,
        debug,
        prepared,
        if emit_cursor {
            CursorEmissionMode::Emit
        } else {
            CursorEmissionMode::Suppress
        },
        suppress_route_scan_hints,
    )?;
    if let Some(probe_limit) = enforced_scan_probe_limit {
        prepared = prepared.with_enforced_scan_probe_limit(probe_limit);
    }

    execute_prepared_scalar_route_runtime_with_scan_count(prepared)
}

/// Execute one resumed scalar page from an authenticated logical boundary.
pub(in crate::db::executor) fn execute_resumed_scalar_retained_slot_page_from_runtime_handoff_for_canister<
    C,
>(
    db: &Db<C>,
    debug: bool,
    prepared: crate::db::executor::PreparedScalarRuntimeHandoff,
    continuation: crate::db::executor::ScalarContinuationContext,
    emit_cursor: bool,
    enforced_scan_probe_limit: Option<usize>,
) -> Result<(StructuralCursorPage, usize), InternalError>
where
    C: CanisterKind,
{
    let mut prepared = prepare_resumed_scalar_retained_slot_page_runtime_from_handoff(
        db,
        debug,
        prepared,
        continuation,
        if emit_cursor {
            CursorEmissionMode::Emit
        } else {
            CursorEmissionMode::Suppress
        },
    )?;
    if let Some(probe_limit) = enforced_scan_probe_limit {
        prepared = prepared.with_enforced_scan_probe_limit(probe_limit);
    }

    execute_prepared_scalar_route_runtime_with_scan_count(prepared)
}

/// Execute one prepared scalar plan into an aggregate-owned retained-slot sink.
#[cfg(feature = "sql")]
pub(in crate::db::executor) fn execute_prepared_scalar_aggregate_kernel_row_sink_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
    retained_slot_layout: RetainedSlotLayout,
    aggregate_route_plan: Option<ExecutionRoutePlan>,
    row_sink: impl FnMut(&KernelRow) -> Result<(), InternalError>,
) -> Result<(), InternalError>
where
    C: CanisterKind,
{
    let prepared = prepare_initial_scalar_route_runtime_from_plan_with_retained_slot_layout(
        db,
        debug,
        plan,
        retained_slot_layout,
        InitialScalarPlanRuntimeOptions::unpaged_rows(
            ProjectionMaterializationMode::RetainSlotRows,
        )
        .with_prebuilt_route_plan(aggregate_route_plan),
    )?;

    execute_prepared_scalar_kernel_row_sink_execution(prepared, row_sink)?;

    Ok(())
}
