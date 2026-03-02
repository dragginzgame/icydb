//! Module: executor::load::entrypoints
//! Responsibility: load executor public entrypoint orchestration for scalar and grouped paths.
//! Does not own: stream resolution internals or projection/having evaluation mechanics.
//! Boundary: validates entrypoint contracts, builds route context, and delegates execution.

use crate::{
    db::{
        cursor::{GroupedPlannedCursor, PlannedCursor},
        executor::{
            AccessStreamBindings, ContinuationEngine, ExecutablePlan, ExecutionKernel,
            ExecutionPreparation, ExecutionTrace,
            load::{CursorPage, GroupedCursorPage, LoadExecutor},
            plan_metrics::record_plan_metrics,
            range_token_anchor_key, validate_executor_plan,
        },
        index::IndexCompilePolicy,
        response::Response,
    },
    error::InternalError,
    obs::sink::{ExecKind, Span},
    traits::{EntityKind, EntityValue},
};

///
/// LoadGroupingMode
///
/// Grouping contract for one load orchestration request.
///
#[derive(Clone, Copy)]
enum LoadGroupingMode {
    Scalar,
    Grouped,
}

///
/// LoadPagingMode
///
/// Pagination contract for one load orchestration request.
///
#[derive(Clone, Copy)]
enum LoadPagingMode {
    Unpaged,
    Paged,
}

///
/// LoadTracingMode
///
/// Trace emission contract for one load orchestration request.
///
#[derive(Clone, Copy)]
enum LoadTracingMode {
    Disabled,
    Enabled,
}

///
/// LoadSurfaceMode
///
/// Response-surface contract for one load orchestration request.
///
#[derive(Clone, Copy)]
enum LoadSurfaceMode {
    Rows,
    Page,
}

///
/// LoadExecutionMode
///
/// Unified load entrypoint mode bundle used by `execute_load`.
/// Encodes grouping, pagination, tracing, and response-surface contracts.
///
#[derive(Clone, Copy)]
struct LoadExecutionMode {
    grouping: LoadGroupingMode,
    paging: LoadPagingMode,
    tracing: LoadTracingMode,
    mode: LoadSurfaceMode,
}

impl LoadExecutionMode {
    // Build one scalar unpaged rows mode contract.
    const fn scalar_unpaged_rows() -> Self {
        Self {
            grouping: LoadGroupingMode::Scalar,
            paging: LoadPagingMode::Unpaged,
            tracing: LoadTracingMode::Disabled,
            mode: LoadSurfaceMode::Rows,
        }
    }

    // Build one scalar paged mode contract with configurable tracing.
    const fn scalar_paged(tracing: LoadTracingMode) -> Self {
        Self {
            grouping: LoadGroupingMode::Scalar,
            paging: LoadPagingMode::Paged,
            tracing,
            mode: LoadSurfaceMode::Page,
        }
    }

    // Build one grouped paged mode contract with configurable tracing.
    const fn grouped_paged(tracing: LoadTracingMode) -> Self {
        Self {
            grouping: LoadGroupingMode::Grouped,
            paging: LoadPagingMode::Paged,
            tracing,
            mode: LoadSurfaceMode::Page,
        }
    }

    // Validate one mode tuple so wrappers cannot silently drift.
    fn validate(self) -> Result<(), InternalError> {
        let valid = matches!(
            (self.grouping, self.paging, self.mode),
            (
                LoadGroupingMode::Scalar,
                LoadPagingMode::Unpaged,
                LoadSurfaceMode::Rows
            ) | (
                LoadGroupingMode::Scalar | LoadGroupingMode::Grouped,
                LoadPagingMode::Paged,
                LoadSurfaceMode::Page,
            )
        );
        if valid {
            Ok(())
        } else {
            Err(super::invariant(
                "invalid load execution mode tuple for entrypoint orchestration",
            ))
        }
    }
}

// Cursor variant input contract for unified load entrypoint dispatch.
enum LoadCursorInput {
    Scalar(PlannedCursor),
    Grouped(GroupedPlannedCursor),
}

// Normalized cursor contract carried by the staged load pipeline.
enum LoadPreparedCursor {
    Scalar(PlannedCursor),
    Grouped(GroupedPlannedCursor),
}

///
/// LoadExecutionContext
///
/// Canonical execution artifacts normalized before staged orchestration.
/// Owns immutable mode contracts consumed by pipeline stages.
///
struct LoadExecutionContext {
    mode: LoadExecutionMode,
}

impl LoadExecutionContext {
    // Construct one immutable execution context from normalized mode contracts.
    const fn new(mode: LoadExecutionMode) -> Self {
        Self { mode }
    }
}

///
/// LoadAccessInputs
///
/// Access-stage payload extracted from execution context.
/// Carries normalized plan/cursor artifacts into grouping/projection stage.
///
struct LoadAccessInputs<E: EntityKind> {
    plan: ExecutablePlan<E>,
    cursor: LoadPreparedCursor,
}

///
/// LoadAccessState
///
/// Access-stage execution artifacts for one load orchestration pass.
/// Carries normalized context and one required access-stage payload.
///
struct LoadAccessState<E: EntityKind> {
    context: LoadExecutionContext,
    access_inputs: LoadAccessInputs<E>,
}

///
/// LoadPayloadState
///
/// Payload-stage execution artifacts for one load orchestration pass.
/// Carries normalized context, one required payload, and optional trace output.
///
struct LoadPayloadState<E: EntityKind> {
    context: LoadExecutionContext,
    payload: LoadExecutionPayload<E>,
    trace: Option<ExecutionTrace>,
}

///
/// LoadExecutionPayload
///
/// Canonical payload envelope produced by one load orchestration pass.
///
enum LoadExecutionPayload<E: EntityKind> {
    Scalar(CursorPage<E>),
    Grouped(GroupedCursorPage),
}

///
/// LoadExecutionOutput
///
/// Canonical output contract produced by one load orchestration pass.
/// Keeps payload and optional trace on one shared boundary.
///
struct LoadExecutionOutput<E: EntityKind> {
    payload: LoadExecutionPayload<E>,
    trace: Option<ExecutionTrace>,
}

impl<E: EntityKind> LoadExecutionOutput<E> {
    // Convert one output payload into scalar rows.
    fn into_scalar_rows(self) -> Result<Response<E>, InternalError> {
        let LoadExecutionPayload::Scalar(page) = self.payload else {
            return Err(super::invariant(
                "scalar rows mode must emit scalar execution payload",
            ));
        };

        Ok(page.items)
    }

    // Convert one output payload into scalar page without trace output.
    fn into_scalar_page(self) -> Result<CursorPage<E>, InternalError> {
        let LoadExecutionPayload::Scalar(page) = self.payload else {
            return Err(super::invariant(
                "scalar page mode must emit scalar execution payload",
            ));
        };

        Ok(page)
    }

    // Convert one output payload into scalar page with optional trace output.
    fn into_scalar_page_with_trace(
        self,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        let LoadExecutionPayload::Scalar(page) = self.payload else {
            return Err(super::invariant(
                "scalar traced mode must emit scalar execution payload",
            ));
        };

        Ok((page, self.trace))
    }

    // Convert one output payload into grouped page with optional trace output.
    fn into_grouped_page_with_trace(
        self,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        let LoadExecutionPayload::Grouped(page) = self.payload else {
            return Err(super::invariant(
                "grouped traced mode must emit grouped execution payload",
            ));
        };

        Ok((page, self.trace))
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one scalar load plan without explicit cursor input.
    pub(crate) fn execute(&self, plan: ExecutablePlan<E>) -> Result<Response<E>, InternalError> {
        let output = self.execute_load(
            plan,
            LoadCursorInput::Scalar(PlannedCursor::none()),
            LoadExecutionMode::scalar_unpaged_rows(),
        )?;

        output.into_scalar_rows()
    }

    // Execute one scalar load plan with optional cursor input.
    // Retained as a direct scalar pagination adapter for executor-level tests.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::db) fn execute_paged_with_cursor(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<PlannedCursor>,
    ) -> Result<CursorPage<E>, InternalError> {
        let output = self.execute_load(
            plan,
            LoadCursorInput::Scalar(cursor.into()),
            LoadExecutionMode::scalar_paged(LoadTracingMode::Disabled),
        )?;

        output.into_scalar_page()
    }

    // Execute one scalar load plan and optionally emit execution trace output.
    pub(in crate::db) fn execute_paged_with_cursor_traced(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<PlannedCursor>,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        let output = self.execute_load(
            plan,
            LoadCursorInput::Scalar(cursor.into()),
            LoadExecutionMode::scalar_paged(LoadTracingMode::Enabled),
        )?;

        output.into_scalar_page_with_trace()
    }

    // Execute one grouped load plan with grouped cursor support and trace output.
    pub(in crate::db) fn execute_grouped_paged_with_cursor_traced(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<GroupedPlannedCursor>,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        let output = self.execute_load(
            plan,
            LoadCursorInput::Grouped(cursor.into()),
            LoadExecutionMode::grouped_paged(LoadTracingMode::Enabled),
        )?;

        output.into_grouped_page_with_trace()
    }

    // Unified load entrypoint pipeline:
    // 1) build execution context
    // 2) execute access path
    // 3) apply grouping/projection contract
    // 4) apply paging contract
    // 5) apply tracing contract
    // 6) materialize response surface
    fn execute_load(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<LoadExecutionOutput<E>, InternalError> {
        let state = Self::build_execution_context(plan, cursor, execution_mode)?;
        let state = Self::execute_access_path(state);
        let state = self.apply_grouping_projection(state)?;
        let state = Self::apply_paging(state)?;
        let state = Self::apply_tracing(state);

        Self::materialize_surface(state)
    }

    // Build one canonical execution context from mode + plan + cursor inputs.
    fn build_execution_context(
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<LoadAccessState<E>, InternalError> {
        execution_mode.validate()?;
        if !plan.mode().is_load() {
            return Err(super::invariant("load executor requires load plans"));
        }

        let grouped_plan = plan.is_grouped();
        match (execution_mode.grouping, grouped_plan) {
            (LoadGroupingMode::Scalar, false) | (LoadGroupingMode::Grouped, true) => {}
            (LoadGroupingMode::Scalar, true) => {
                return Err(super::invariant(
                    "grouped plans require grouped load execution mode",
                ));
            }
            (LoadGroupingMode::Grouped, false) => {
                return Err(super::invariant(
                    "grouped load execution mode requires grouped logical plans",
                ));
            }
        }

        let prepared_cursor = match (execution_mode.grouping, cursor) {
            (LoadGroupingMode::Scalar, LoadCursorInput::Scalar(cursor)) => {
                LoadPreparedCursor::Scalar(plan.revalidate_cursor(cursor)?)
            }
            (LoadGroupingMode::Grouped, LoadCursorInput::Grouped(cursor)) => {
                LoadPreparedCursor::Grouped(plan.revalidate_grouped_cursor(cursor)?)
            }
            (LoadGroupingMode::Scalar, LoadCursorInput::Grouped(_)) => {
                return Err(super::invariant(
                    "scalar load execution mode requires scalar cursor input",
                ));
            }
            (LoadGroupingMode::Grouped, LoadCursorInput::Scalar(_)) => {
                return Err(super::invariant(
                    "grouped load execution mode requires grouped cursor input",
                ));
            }
        };
        Ok(LoadAccessState {
            context: LoadExecutionContext::new(execution_mode),
            access_inputs: LoadAccessInputs {
                plan,
                cursor: prepared_cursor,
            },
        })
    }

    // Execute one canonical access path and stage payload + trace artifacts.
    const fn execute_access_path(state: LoadAccessState<E>) -> LoadAccessState<E> {
        // Mechanical stage boundary: access inputs stay normalized and stage-owned.
        state
    }

    // Apply grouping/projection contracts over staged payload artifacts.
    fn apply_grouping_projection(
        &self,
        state: LoadAccessState<E>,
    ) -> Result<LoadPayloadState<E>, InternalError> {
        let LoadAccessState {
            context,
            access_inputs,
        } = state;
        let LoadAccessInputs { plan, cursor } = access_inputs;
        let (payload, trace) = match (context.mode.grouping, cursor) {
            (LoadGroupingMode::Scalar, LoadPreparedCursor::Scalar(cursor)) => {
                let (page, trace) = self.execute_scalar_path(plan, cursor)?;
                (LoadExecutionPayload::Scalar(page), trace)
            }
            (LoadGroupingMode::Grouped, LoadPreparedCursor::Grouped(cursor)) => {
                let (page, trace) = self.execute_grouped_path(plan, cursor)?;
                (LoadExecutionPayload::Grouped(page), trace)
            }
            (LoadGroupingMode::Scalar, LoadPreparedCursor::Grouped(_)) => {
                return Err(super::invariant(
                    "scalar load execution mode must not carry grouped cursor inputs",
                ));
            }
            (LoadGroupingMode::Grouped, LoadPreparedCursor::Scalar(_)) => {
                return Err(super::invariant(
                    "grouped load execution mode must not carry scalar cursor inputs",
                ));
            }
        };

        Ok(LoadPayloadState {
            context,
            payload,
            trace,
        })
    }

    // Apply paging contracts over staged payload artifacts.
    fn apply_paging(mut state: LoadPayloadState<E>) -> Result<LoadPayloadState<E>, InternalError> {
        let payload = match (state.context.mode.paging, state.payload) {
            (LoadPagingMode::Paged, payload) => payload,
            (LoadPagingMode::Unpaged, LoadExecutionPayload::Scalar(mut page)) => {
                // Unpaged scalar execution intentionally suppresses continuation payload.
                page.next_cursor = None;
                LoadExecutionPayload::Scalar(page)
            }
            (LoadPagingMode::Unpaged, LoadExecutionPayload::Grouped(_)) => {
                return Err(super::invariant(
                    "unpaged load execution mode must not carry grouped payload",
                ));
            }
        };
        state.payload = payload;

        Ok(state)
    }

    // Apply tracing contracts as a post-processing layer over staged artifacts.
    const fn apply_tracing(mut state: LoadPayloadState<E>) -> LoadPayloadState<E> {
        if matches!(state.context.mode.tracing, LoadTracingMode::Disabled) {
            state.trace = None;
        }

        state
    }

    // Materialize one finalized response surface from staged artifacts.
    fn materialize_surface(
        state: LoadPayloadState<E>,
    ) -> Result<LoadExecutionOutput<E>, InternalError> {
        let output = match (state.context.mode.mode, state.payload) {
            (LoadSurfaceMode::Page, payload) => LoadExecutionOutput {
                payload,
                trace: state.trace,
            },
            (LoadSurfaceMode::Rows, LoadExecutionPayload::Scalar(page)) => LoadExecutionOutput {
                payload: LoadExecutionPayload::Scalar(page),
                trace: None,
            },
            (LoadSurfaceMode::Rows, LoadExecutionPayload::Grouped(_)) => {
                return Err(super::invariant(
                    "rows load surface mode must not carry grouped payload",
                ));
            }
        };

        Ok(output)
    }

    // Scalar execution spine:
    // 1) normalize continuation runtime bindings
    // 2) derive routing and trace contracts
    // 3) execute kernel materialization
    // 4) finalize scalar page + observability
    fn execute_scalar_path(
        &self,
        plan: ExecutablePlan<E>,
        cursor: PlannedCursor,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        let scalar_runtime = ContinuationEngine::scalar_runtime(cursor);
        let cursor_boundary = scalar_runtime.cursor_boundary();
        let index_range_token = scalar_runtime.index_range_token();
        let continuation_signature = plan.continuation_signature();
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let route_plan = Self::build_execution_route_plan_for_load(
            plan.as_inner(),
            cursor_boundary,
            index_range_token,
            None,
        )?;
        let continuation_applied = !matches!(
            route_plan.continuation_mode(),
            crate::db::executor::route::ContinuationMode::Initial
        );
        let direction = route_plan.direction();
        debug_assert_eq!(
            route_plan.window().effective_offset,
            ExecutionKernel::effective_page_offset(plan.as_inner(), cursor_boundary),
            "route window effective offset must match logical plan offset semantics",
        );
        let mut execution_trace = self
            .debug
            .then(|| ExecutionTrace::new(plan.access(), direction, continuation_applied));
        let plan = plan.into_inner();
        let execution_preparation = ExecutionPreparation::for_plan::<E>(&plan);

        let result = (|| {
            let mut span = Span::<E>::new(ExecKind::Load);

            validate_executor_plan::<E>(&plan)?;
            let ctx = self.db.recovered_context::<E>()?;
            let execution_inputs = super::ExecutionInputs {
                ctx: &ctx,
                plan: &plan,
                stream_bindings: AccessStreamBindings {
                    index_prefix_specs: index_prefix_specs.as_slice(),
                    index_range_specs: index_range_specs.as_slice(),
                    index_range_anchor: index_range_token.map(range_token_anchor_key),
                    direction,
                },
                execution_preparation: &execution_preparation,
            };

            record_plan_metrics(&plan.access);
            let materialized = ExecutionKernel::materialize_with_optional_residual_retry(
                &execution_inputs,
                &route_plan,
                cursor_boundary,
                continuation_signature,
                IndexCompilePolicy::ConservativeSubset,
            )?;
            let page = materialized.page;
            let rows_scanned = materialized.rows_scanned;
            let post_access_rows = materialized.post_access_rows;
            let optimization = materialized.optimization;
            let index_predicate_applied = materialized.index_predicate_applied;
            let index_predicate_keys_rejected = materialized.index_predicate_keys_rejected;
            let distinct_keys_deduped = materialized.distinct_keys_deduped;

            Ok(Self::finalize_execution(
                page,
                optimization,
                rows_scanned,
                post_access_rows,
                index_predicate_applied,
                index_predicate_keys_rejected,
                distinct_keys_deduped,
                &mut span,
                &mut execution_trace,
            ))
        })();

        result.map(|page| (page, execution_trace))
    }

    // Grouped execution spine:
    // 1) resolve grouped route/metadata
    // 2) build grouped key stream
    // 3) execute grouped fold
    // 4) finalize grouped output + observability
    fn execute_grouped_path(
        &self,
        plan: ExecutablePlan<E>,
        cursor: GroupedPlannedCursor,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        let route = Self::resolve_grouped_route(plan, cursor, self.debug)?;
        let stream = self.build_grouped_stream(&route)?;
        let folded = Self::execute_group_fold(&route, stream)?;

        Ok(Self::finalize_grouped_output(route, folded))
    }
}

#[cfg(test)]
pub(in crate::db::executor) const fn load_execute_stage_order_guard() -> [&'static str; 6] {
    [
        "build_execution_context",
        "execute_access_path",
        "apply_grouping_projection",
        "apply_paging",
        "apply_tracing",
        "materialize_surface",
    ]
}

#[cfg(test)]
pub(in crate::db::executor) fn load_pipeline_state_optional_slot_count_guard<E: EntityKind>()
-> usize {
    fn consume_access_state_shape<E: EntityKind>(state: LoadAccessState<E>) {
        let LoadAccessState {
            context,
            access_inputs,
        } = state;
        let _ = (context, access_inputs);
    }

    fn consume_payload_state_shape<E: EntityKind>(state: LoadPayloadState<E>) {
        let LoadPayloadState {
            context,
            payload,
            trace,
        } = state;
        let _ = (context, payload, trace);
    }

    let _ = consume_access_state_shape::<E> as fn(LoadAccessState<E>);
    let _ = consume_payload_state_shape::<E> as fn(LoadPayloadState<E>);

    0
}
