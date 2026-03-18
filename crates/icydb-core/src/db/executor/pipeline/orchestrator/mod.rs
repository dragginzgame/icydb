//! Module: executor::pipeline::orchestrator
//! Responsibility: load staged orchestration wiring and contract-boundary exports.
//! Does not own: row materialization mechanics or continuation cursor resolution internals.
//! Boundary: executes staged orchestration and exposes stable load contracts.

mod contracts;
mod dispatch;
#[cfg(test)]
mod guards;
mod payload;
mod state;
mod strategy;

use crate::{
    db::executor::{
        ExecutablePlan, ExecutionTrace, LoadCursorInput, PreparedLoadCursor,
        pipeline::contracts::GroupedCursorPage,
        pipeline::orchestrator::state::LoadExecutionContext,
        pipeline::{
            contracts::LoadExecutor,
            orchestrator::{contracts::LoadExecutionDescriptor, state::LoadPipelineState},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
use std::any::Any;

pub(in crate::db::executor) use contracts::{
    ErasedLoadExecutionSurface, ErasedLoadPayload, LoadExecutionMode, LoadExecutionSurface,
    LoadTracingMode,
};
#[cfg(test)]
pub(in crate::db::executor) use guards::{
    load_execute_stage_order_guard, load_pipeline_state_optional_slot_count_guard,
};

///
/// ErasedExecutablePlan
///
/// ErasedExecutablePlan is the single orchestrator-owned wrapper for typed
/// executable-plan erasure across the monomorphic load entrypoint boundary.
/// It keeps raw `Any` plan transport localized so orchestrator stages do not
/// depend on open-coded downcasts.
///

struct ErasedExecutablePlan {
    plan: Box<dyn Any>,
}

impl ErasedExecutablePlan {
    // Erase one typed executable plan at the entrypoint boundary.
    fn new<E>(plan: ExecutablePlan<E>) -> Self
    where
        E: EntityKind + 'static,
    {
        Self {
            plan: Box::new(plan),
        }
    }

    // Recover one typed executable plan at the orchestrator leaf boundary.
    fn into_typed<E>(
        self,
        mismatch_message: &'static str,
    ) -> Result<ExecutablePlan<E>, InternalError>
    where
        E: EntityKind + 'static,
    {
        self.plan
            .downcast::<ExecutablePlan<E>>()
            .map(|plan| *plan)
            .map_err(|_| crate::db::error::query_executor_invariant(mismatch_message))
    }
}

///
/// RuntimeAccessState
///
/// RuntimeAccessState is the generic-free access-stage envelope used by the
/// monomorphic load entrypoint path.
/// It carries erased typed plan ownership together with the resolved cursor
/// contract needed by scalar/grouped payload materialization.
///

struct RuntimeAccessState {
    context: LoadExecutionContext,
    plan: ErasedExecutablePlan,
    cursor: PreparedLoadCursor,
}

///
/// RuntimeLoadPayload
///
/// RuntimeLoadPayload is the generic-free payload envelope produced by the
/// monomorphic load entrypoint path.
///

enum RuntimeLoadPayload {
    Scalar(ErasedLoadPayload),
    Grouped(GroupedCursorPage),
}

///
/// RuntimePayloadState
///
/// RuntimePayloadState carries generic-free payload artifacts across the final
/// paging, tracing, and surface-projection stages of the load entrypoint path.
///

struct RuntimePayloadState {
    context: LoadExecutionContext,
    payload: RuntimeLoadPayload,
    trace: Option<ExecutionTrace>,
}

///
/// LoadExecutionRuntime
///
/// LoadExecutionRuntime keeps typed load entrypoint setup and payload
/// materialization behind one object-safe boundary.
/// The root load-orchestrator path stays monomorphic by delegating only the
/// typed access-state and payload-stage leaves through this trait.
///

trait LoadExecutionRuntime {
    /// Resolve one erased executable plan plus cursor input into access-stage state.
    fn build_runtime_access_state(
        &self,
        plan: ErasedExecutablePlan,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<RuntimeAccessState, InternalError>;

    /// Materialize one generic-free payload state from one access-stage envelope.
    fn apply_runtime_grouping_projection(
        &self,
        state: RuntimeAccessState,
    ) -> Result<RuntimePayloadState, InternalError>;
}

// Execute one canonical load pipeline over generic-free entrypoint state.
fn execute_load_with_runtime(
    runtime: &dyn LoadExecutionRuntime,
    plan: ErasedExecutablePlan,
    cursor: LoadCursorInput,
    execution_mode: LoadExecutionMode,
) -> Result<ErasedLoadExecutionSurface, InternalError> {
    // Phase 1: normalize context + cursor contracts once.
    let access_state = runtime.build_runtime_access_state(plan, cursor, execution_mode)?;
    // Phase 2: the access-path stage is currently a mechanical boundary.
    let payload_state = runtime.apply_runtime_grouping_projection(access_state)?;
    // Phase 3: apply paging/tracing/surface projection over generic-free payloads.
    let payload_state = apply_runtime_paging(payload_state)?;
    let payload_state = apply_runtime_tracing(payload_state);

    materialize_runtime_surface(payload_state)
}

// Apply paging contracts over generic-free payload artifacts.
fn apply_runtime_paging(
    mut state: RuntimePayloadState,
) -> Result<RuntimePayloadState, InternalError> {
    let execution_mode = state.context.mode;
    let payload = if execution_mode.scalar_rows_mode() || execution_mode.scalar_page_mode() {
        match state.payload {
            RuntimeLoadPayload::Scalar(payload) => RuntimeLoadPayload::Scalar(payload),
            RuntimeLoadPayload::Grouped(_) => {
                return Err(crate::db::error::query_executor_invariant(
                    "scalar load mode must carry scalar runtime payload",
                ));
            }
        }
    } else {
        debug_assert!(
            execution_mode.grouped_page_mode(),
            "runtime payload paging expects grouped mode for non-scalar load surfaces",
        );
        match state.payload {
            RuntimeLoadPayload::Grouped(page) => RuntimeLoadPayload::Grouped(page),
            RuntimeLoadPayload::Scalar(_) => {
                return Err(crate::db::error::query_executor_invariant(
                    "grouped load mode must carry grouped runtime payload",
                ));
            }
        }
    };
    state.payload = payload;

    Ok(state)
}

// Apply tracing contracts over generic-free runtime payload artifacts.
const fn apply_runtime_tracing(mut state: RuntimePayloadState) -> RuntimePayloadState {
    if !state.context.mode.tracing_enabled() {
        state.trace = None;
    }

    state
}

// Materialize one finalized generic-free load surface from runtime payload artifacts.
fn materialize_runtime_surface(
    state: RuntimePayloadState,
) -> Result<ErasedLoadExecutionSurface, InternalError> {
    let execution_mode = state.context.mode;
    if execution_mode.scalar_page_mode() {
        let RuntimeLoadPayload::Scalar(page) = state.payload else {
            return Err(crate::db::error::query_executor_invariant(
                "scalar page load mode must carry scalar runtime payload",
            ));
        };

        if execution_mode.tracing_enabled() {
            Ok(ErasedLoadExecutionSurface::ScalarPageWithTrace(
                page,
                state.trace,
            ))
        } else {
            Ok(ErasedLoadExecutionSurface::ScalarPage(page))
        }
    } else if execution_mode.scalar_rows_mode() {
        let RuntimeLoadPayload::Scalar(page) = state.payload else {
            return Err(crate::db::error::query_executor_invariant(
                "scalar rows load mode must carry scalar runtime payload",
            ));
        };

        Ok(ErasedLoadExecutionSurface::ScalarPage(page))
    } else {
        debug_assert!(
            execution_mode.grouped_page_mode(),
            "runtime surface materialization expects grouped mode for non-scalar load surfaces",
        );
        let RuntimeLoadPayload::Grouped(page) = state.payload else {
            return Err(crate::db::error::query_executor_invariant(
                "grouped page load mode must carry grouped runtime payload",
            ));
        };

        Ok(ErasedLoadExecutionSurface::GroupedPageWithTrace(
            page,
            state.trace,
        ))
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute one load plan through the monomorphic erased-surface path.
    pub(in crate::db::executor) fn execute_load_erased(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<ErasedLoadExecutionSurface, InternalError> {
        execute_load_with_runtime(
            self,
            ErasedExecutablePlan::new(plan),
            cursor,
            execution_mode,
        )
    }

    // B1 dynamic load entrypoint:
    // consumes one immutable descriptor that owns stage-loop authority.
    // Existing typed entrypoints delegate here so subsequent slices can
    // migrate runtime internals without changing public call sites.
    // This remains as a test/backstop surface while release entrypoints route
    // through the canonical fixed-order stage path below.
    #[allow(dead_code)]
    pub(in crate::db::executor) fn execute_load_dyn(
        &self,
        descriptor: LoadExecutionDescriptor,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<LoadExecutionSurface<E>, InternalError> {
        let mut state = LoadPipelineState::Inputs {
            plan,
            cursor,
            execution_mode,
        };

        for stage in descriptor.stage_plan() {
            state = self.execute_load_stage(*stage, state)?;
        }

        state.into_surface()
    }

    // Unified load entrypoint pipeline:
    // 1) build execution context
    // 2) execute access path
    // 3) apply grouping/projection contract
    // 4) apply paging contract
    // 5) apply tracing contract
    // 6) materialize response surface
    #[allow(dead_code)]
    pub(in crate::db::executor) fn execute_load(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<LoadExecutionSurface<E>, InternalError> {
        // Canonical stage order:
        // 1) build execution context
        let access_state = Self::build_execution_context(plan, cursor, execution_mode)?;
        // 2) execute access path
        let access_state = Self::execute_access_path(access_state);
        // 3) apply grouping/projection contract
        let payload_state = self.apply_grouping_projection(access_state)?;
        // 4) apply paging contract
        let payload_state = Self::apply_paging(payload_state)?;
        // 5) apply tracing contract
        let payload_state = Self::apply_tracing(payload_state);
        // 6) materialize response surface
        Self::materialize_surface(payload_state)
    }
}

impl<E> LoadExecutionRuntime for LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    fn build_runtime_access_state(
        &self,
        plan: ErasedExecutablePlan,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<RuntimeAccessState, InternalError> {
        let plan = plan.into_typed(
            "load execution runtime received executable plan with unexpected entity type",
        )?;
        let access_state = Self::build_execution_context(plan, cursor, execution_mode)?;
        let crate::db::executor::pipeline::orchestrator::state::LoadAccessState {
            context,
            access_inputs,
        } = access_state;
        let crate::db::executor::pipeline::orchestrator::state::LoadAccessInputs { plan, cursor } =
            access_inputs;

        Ok(RuntimeAccessState {
            context,
            plan: ErasedExecutablePlan::new(plan),
            cursor,
        })
    }

    fn apply_runtime_grouping_projection(
        &self,
        state: RuntimeAccessState,
    ) -> Result<RuntimePayloadState, InternalError> {
        let RuntimeAccessState {
            context,
            plan,
            cursor,
        } = state;
        let plan = plan.into_typed(
            "load runtime payload stage received executable plan with unexpected entity type",
        )?;
        let access_state = crate::db::executor::pipeline::orchestrator::state::LoadAccessState {
            context,
            access_inputs: crate::db::executor::pipeline::orchestrator::state::LoadAccessInputs {
                plan,
                cursor,
            },
        };
        let payload_state = Self::apply_grouping_projection(self, access_state)?;
        let crate::db::executor::pipeline::orchestrator::state::LoadPayloadState {
            context,
            payload,
            trace,
        } = payload_state;
        let payload = match payload {
            crate::db::executor::pipeline::orchestrator::state::LoadExecutionPayload::Scalar(
                page,
            ) => RuntimeLoadPayload::Scalar(page),
            crate::db::executor::pipeline::orchestrator::state::LoadExecutionPayload::Grouped(
                page,
            ) => RuntimeLoadPayload::Grouped(page),
        };

        Ok(RuntimePayloadState {
            context,
            payload,
            trace,
        })
    }
}
