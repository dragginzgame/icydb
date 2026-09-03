//! Module: db::executor::pipeline::entrypoints::grouped
//! Defines grouped pipeline entrypoints from prepared route shapes into grouped
//! runtime execution.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::{SharedPreparedExecutionPlan, StructuralGroupedProjectionResult};
use crate::db::registry::StoreHandle;
use crate::{
    db::{
        cursor::ValidatedGroupedCursor,
        executor::{
            EntityAuthority, ExecutionPreparation, ExecutionTrace, PreparedGroupedRuntimeResidents,
            PreparedLoadPlan, RetainedSlotLayout,
            aggregate::runtime::{
                build_grouped_stream_with_runtime, execute_group_fold_stage,
                finalize_grouped_output,
            },
            budget::{
                charge_current_execution_budget, charge_runtime_grouped_rows,
                prepared_read_execution_context, runtime_value_work, with_read_execution_budget,
            },
            pipeline::contracts::{ExecutionRuntimeAdapter, GroupedCursorPage, GroupedRouteStage},
            pipeline::grouped_runtime::resolve_grouped_route_for_plan,
            pipeline::runtime::{
                GroupedStreamStage, StructuralGroupedRowRuntime,
                compile_grouped_row_slot_layout_from_inputs,
            },
            pipeline::timing::{elapsed_execution_micros, start_execution_timer},
            record_aggregation,
            stream::access::TraversalRuntime,
            with_execution_stats_capture,
        },
        schema::cardinality_generation::CardinalityAcceptedRootIdentity,
    },
    error::InternalError,
    metrics::EntityMetricsSpan,
    traits::CanisterKind,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource, DiagnosticExecutionLane};

/// Execute one generic-free shared grouped plan through the canonical runtime.
pub(in crate::db) fn execute_shared_grouped_plan_for_canister<C>(
    db: &crate::db::Db<C>,
    debug: bool,
    plan: SharedPreparedExecutionPlan,
    cursor: ValidatedGroupedCursor,
    execution_lane: DiagnosticExecutionLane,
) -> Result<(StructuralGroupedProjectionResult, Option<ExecutionTrace>), InternalError>
where
    C: CanisterKind,
{
    let context = prepared_read_execution_context(&plan, execution_lane);
    with_read_execution_budget(db.request_execution_scope(), context, || {
        execute_shared_grouped_plan_for_canister_inner(db, debug, plan, cursor)
    })
}

fn execute_shared_grouped_plan_for_canister_inner<C>(
    db: &crate::db::Db<C>,
    debug: bool,
    plan: SharedPreparedExecutionPlan,
    cursor: ValidatedGroupedCursor,
) -> Result<(StructuralGroupedProjectionResult, Option<ExecutionTrace>), InternalError>
where
    C: CanisterKind,
{
    let entity_path = plan.authority_ref().entity_path_handle();
    let _metrics_span = EntityMetricsSpan::new(entity_path.as_ref());
    charge_grouped_cursor_input(&cursor)?;
    let value_catalog = plan
        .authority_ref()
        .accepted_schema_info()
        .map(crate::db::schema::SchemaInfo::value_catalog_handle)
        .cloned()
        .ok_or_else(InternalError::query_executor_invariant)?;
    let prepared = prepare_grouped_route_runtime_for_load_plan(
        db,
        debug,
        plan.into_prepared_load_plan(),
        cursor,
    )?;
    let (page, trace) = execute_prepared_grouped_route_runtime(prepared)?;
    charge_grouped_page_result(&page)?;

    Ok((
        StructuralGroupedProjectionResult::from_page(page, value_catalog),
        trace,
    ))
}
fn charge_grouped_page_result(page: &GroupedCursorPage) -> Result<(), InternalError> {
    charge_runtime_grouped_rows(&page.rows)?;
    if let Some(cursor) = page.next_cursor.as_ref() {
        let encoded = cursor
            .encode()
            .map_err(|_| InternalError::query_executor_invariant())?;
        charge_current_execution_budget(DiagnosticExecutionBudgetResource::CursorSteps, 1)?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            u64::try_from(encoded.len()).unwrap_or(u64::MAX),
        )?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::ResultBytes,
            u64::try_from(encoded.len().saturating_mul(2)).unwrap_or(u64::MAX),
        )?;
    }

    Ok(())
}

fn charge_grouped_cursor_input(cursor: &ValidatedGroupedCursor) -> Result<(), InternalError> {
    let Some(group_key) = cursor.last_group_key() else {
        return Ok(());
    };
    let (bytes, nested_steps) = group_key.iter().fold((0_u64, 0_u64), |total, value| {
        let value_work = runtime_value_work(value);
        (
            total.0.saturating_add(value_work.0),
            total.1.saturating_add(value_work.1),
        )
    });
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::CursorSteps,
        u64::try_from(group_key.len()).unwrap_or(u64::MAX),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::NestedValueSteps,
        nested_steps,
    )?;
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::TemporaryBytes, bytes)
}

///
/// GroupedPathRuntimeContext
///
/// GroupedPathRuntimeContext is the owner-local runtime context needed by the
/// grouped execution spine after the frontend resolves store authority.
/// Shared grouped entrypoint orchestration stays monomorphic by driving this
/// structural context directly.
///

struct GroupedPathRuntimeContext {
    traversal_runtime: TraversalRuntime,
    row_store: StoreHandle,
    authority: EntityAuthority,
}

///
/// PreparedGroupedRouteRuntime
///
/// PreparedGroupedRouteRuntime is the generic-free grouped execution bundle
/// emitted once the frontend has resolved route metadata and structural
/// runtime authority.
/// Grouped runtime execution consumes this bundle directly.
///

pub(in crate::db::executor) struct PreparedGroupedRouteRuntime {
    route: GroupedRouteStage,
    runtime: GroupedPathRuntimeContext,
    execution_preparation: ExecutionPreparation,
    grouped_slot_layout: RetainedSlotLayout,
}

impl GroupedPathRuntimeContext {
    // Build the grouped runtime spine once from one recovered store handle and
    // its resolved structural entity authority.
    fn from_store(store: StoreHandle, authority: EntityAuthority) -> Result<Self, InternalError> {
        let entity_tag = authority.entity_tag();
        let accepted_schema = authority.accepted_schema_authority()?;
        let accepted_root = CardinalityAcceptedRootIdentity::new(
            accepted_schema.revision(),
            accepted_schema.fingerprint(),
        )?;

        Ok(Self {
            traversal_runtime: TraversalRuntime::new(
                store,
                entity_tag,
                authority
                    .accepted_runtime_root_identity()
                    .database_incarnation(),
                accepted_root,
            ),
            row_store: store,
            authority,
        })
    }

    /// Build one grouped execution stream for an already resolved route.
    fn build_grouped_stream(
        &self,
        route: &GroupedRouteStage,
        execution_preparation: ExecutionPreparation,
        grouped_slot_layout: RetainedSlotLayout,
    ) -> Result<GroupedStreamStage, InternalError> {
        let runtime = ExecutionRuntimeAdapter::from_stream_runtime(self.traversal_runtime);
        let single_grouped_path = if execution_preparation
            .effective_runtime_filter_program()
            .is_none()
            && matches!(route.grouped_aggregate_execution_specs(), [aggregate] if aggregate.admits_count_rows_dedicated_fold())
        {
            route
                .group_fields()
                .as_path_aware()
                .and_then(|fields| match fields {
                    [field] => field.as_scalar_path(),
                    _ => None,
                })
        } else {
            None
        };
        build_grouped_stream_with_runtime(
            route,
            &runtime,
            execution_preparation,
            StructuralGroupedRowRuntime::new(
                self.row_store,
                self.authority.row_layout()?,
                grouped_slot_layout,
                single_grouped_path,
            ),
        )
    }
}

impl PreparedGroupedRouteRuntime {
    // Build one prepared grouped runtime bundle from one resolved route and
    // one structural grouped runtime core without duplicating plan prep logic.
    fn new(
        route: GroupedRouteStage,
        runtime: GroupedPathRuntimeContext,
        prepared_residents: Option<PreparedGroupedRuntimeResidents>,
    ) -> Result<Self, InternalError> {
        let residents = if let Some(residents) = prepared_residents {
            residents
        } else {
            let execution_preparation = ExecutionPreparation::from_runtime_plan(
                route.plan(),
                route.plan().slot_map().map(<[usize]>::to_vec),
            );
            let grouped_slot_layout = compile_grouped_row_slot_layout_from_inputs(
                runtime.authority.row_layout()?,
                route.group_fields(),
                route.grouped_aggregate_execution_specs(),
                route.grouped_distinct_execution_strategy(),
                execution_preparation.effective_runtime_filter_program(),
            );

            PreparedGroupedRuntimeResidents::new(execution_preparation, grouped_slot_layout)
        };
        let (execution_preparation, grouped_slot_layout) = residents.into_parts();

        Ok(Self {
            route,
            runtime,
            execution_preparation,
            grouped_slot_layout,
        })
    }
}

// Prepare one grouped runtime bundle from one prepared load plan plus the
// caller-resolved grouped cursor so entrypoints and orchestrator strategy
// share one route/runtime assembly seam.
pub(in crate::db::executor) fn prepare_grouped_route_runtime_for_load_plan<C>(
    db: &crate::db::Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
    cursor: ValidatedGroupedCursor,
) -> Result<PreparedGroupedRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let authority = plan.authority();
    let prepared_residents = plan.cloned_grouped_runtime_residents()?;
    let route = resolve_grouped_route_for_plan(plan, cursor, debug)?;
    let store = db.recovered_store(authority.store_path())?;

    PreparedGroupedRouteRuntime::new(
        route,
        GroupedPathRuntimeContext::from_store(store, authority)?,
        prepared_residents,
    )
}

// Execute one fully resolved grouped route through the canonical grouped
// runtime spine. The grouped route/stream/page contracts are already structural,
// so this orchestration can stay monomorphic.
fn execute_grouped_route_path(
    runtime: &GroupedPathRuntimeContext,
    mut route: GroupedRouteStage,
    execution_preparation: ExecutionPreparation,
    grouped_slot_layout: RetainedSlotLayout,
) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
    let collect_stats = route.execution_trace.is_some();
    let execution_started_at = start_execution_timer();
    let (fold_result, mut execution_stats) = with_execution_stats_capture(collect_stats, || {
        let stream =
            runtime.build_grouped_stream(&route, execution_preparation, grouped_slot_layout)?;
        let (folded, aggregation_micros) =
            crate::db::executor::measure_execution_stats_phase(|| {
                execute_group_fold_stage(&route, stream)
            });
        record_aggregation(aggregation_micros);

        folded
    });
    let folded = fold_result?;
    let execution_time_micros = elapsed_execution_micros(execution_started_at);
    if let Some(stats) = execution_stats.as_mut() {
        stats.apply_grouped_outcome(folded.rows_returned());
    }
    if let Some(trace) = route.execution_trace_mut().as_mut() {
        trace.set_execution_stats(
            execution_stats.map(crate::db::executor::ExecutionProfileStats::into_execution_stats),
        );
    }
    Ok(finalize_grouped_output(
        route,
        folded,
        execution_time_micros,
    ))
}

// Execute one fully prepared grouped runtime bundle through the canonical
// grouped runtime spine without re-entering typed executor state.
pub(in crate::db::executor) fn execute_prepared_grouped_route_runtime(
    prepared: PreparedGroupedRouteRuntime,
) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
    let PreparedGroupedRouteRuntime {
        route,
        runtime,
        execution_preparation,
        grouped_slot_layout,
    } = prepared;

    execute_grouped_route_path(&runtime, route, execution_preparation, grouped_slot_layout)
}
