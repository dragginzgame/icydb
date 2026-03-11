//! Module: executor::pipeline::orchestrator
//! Responsibility: load entrypoint mode/output contracts and staged orchestration.
//! Does not own: row materialization mechanics or continuation cursor resolution internals.
//! Boundary: defines stable load mode/response envelopes and executes staged orchestration.

#[cfg(test)]
mod guards;
mod payload;
mod state;

use crate::{
    db::{
        executor::{
            ExecutablePlan, ExecutionTrace, LoadCursorInput, PreparedLoadCursor,
            RequestedLoadExecutionShape,
            pipeline::{
                contracts::{CursorPage, GroupedCursorPage, LoadExecutor},
                stages::{LoadPipelineStage, plan_load_pipeline_stages},
            },
        },
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::pipeline::orchestrator::state::{
    LoadAccessInputs, LoadAccessState, LoadExecutionContext, LoadExecutionPayload, LoadPayloadState,
};
#[cfg(test)]
pub(in crate::db::executor) use guards::{
    load_execute_stage_order_guard, load_pipeline_state_optional_slot_count_guard,
};

///
/// LoadTracingMode
///
/// Trace emission contract for one load orchestration request.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum LoadTracingMode {
    Disabled,
    Enabled,
}

///
/// LoadMode
///
/// Canonical load pipeline mode selected before staged execution starts.
/// This is the single mode-selection boundary for load orchestration.
///

#[derive(Clone, Copy)]
pub(super) enum LoadMode {
    ScalarRows,
    ScalarPage,
    GroupedPage,
}

///
/// LoadExecutionMode
///
/// Unified load entrypoint mode bundle used by `execute_load`.
/// Encodes one canonical load mode plus tracing contract.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct LoadExecutionMode {
    pub(super) mode: LoadMode,
    pub(super) tracing: LoadTracingMode,
}

impl LoadExecutionMode {
    // Build one scalar unpaged rows mode contract.
    pub(in crate::db::executor) const fn scalar_unpaged_rows() -> Self {
        Self {
            mode: LoadMode::ScalarRows,
            tracing: LoadTracingMode::Disabled,
        }
    }

    // Build one scalar paged mode contract with configurable tracing.
    pub(in crate::db::executor) const fn scalar_paged(tracing: LoadTracingMode) -> Self {
        Self {
            mode: LoadMode::ScalarPage,
            tracing,
        }
    }

    // Build one grouped paged mode contract with configurable tracing.
    pub(in crate::db::executor) const fn grouped_paged(tracing: LoadTracingMode) -> Self {
        Self {
            mode: LoadMode::GroupedPage,
            tracing,
        }
    }

    // Validate one mode tuple so wrappers cannot silently drift.
    pub(super) fn validate(self) -> Result<(), InternalError> {
        if matches!(
            (self.mode, self.tracing),
            (LoadMode::ScalarRows, LoadTracingMode::Enabled)
        ) {
            Err(crate::db::error::query_executor_invariant(
                "scalar rows load mode must not request tracing output",
            ))
        } else {
            Ok(())
        }
    }

    // Resolve entrypoint-selected mode into the requested scalar/grouped execution shape.
    pub(in crate::db::executor) const fn requested_shape(self) -> RequestedLoadExecutionShape {
        match self.mode {
            LoadMode::ScalarRows | LoadMode::ScalarPage => RequestedLoadExecutionShape::Scalar,
            LoadMode::GroupedPage => RequestedLoadExecutionShape::Grouped,
        }
    }

    // True when load mode materializes unpaged scalar rows.
    const fn scalar_rows_mode(self) -> bool {
        matches!(self.mode, LoadMode::ScalarRows)
    }

    // True when load mode materializes one paged scalar surface.
    const fn scalar_page_mode(self) -> bool {
        matches!(self.mode, LoadMode::ScalarPage)
    }

    // True when load mode materializes one grouped paged surface.
    const fn grouped_page_mode(self) -> bool {
        matches!(self.mode, LoadMode::GroupedPage)
    }

    // True when load mode should preserve execution trace output.
    const fn tracing_enabled(self) -> bool {
        matches!(self.tracing, LoadTracingMode::Enabled)
    }
}

///
/// LoadExecutionSurface
///
/// Finalized load output surface for entrypoint wrappers.
/// Encodes one terminal response shape so wrapper adapters do not carry
/// payload/trace pairing branches.
///

pub(in crate::db::executor) enum LoadExecutionSurface<E: EntityKind> {
    ScalarRows(EntityResponse<E>),
    ScalarPage(CursorPage<E>),
    ScalarPageWithTrace(CursorPage<E>, Option<ExecutionTrace>),
    GroupedPageWithTrace(GroupedCursorPage, Option<ExecutionTrace>),
}

///
/// LoadPipelineState
///
/// Stage-local state envelope used by deterministic load stage execution.
/// Encodes typed stage artifacts so orchestration remains linear and explicit.
///

enum LoadPipelineState<E: EntityKind> {
    Inputs {
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    },
    Access(LoadAccessState<E>),
    Payload(LoadPayloadState<E>),
    Surface(LoadExecutionSurface<E>),
}

impl<E> LoadPipelineState<E>
where
    E: EntityKind,
{
    // Build one access-stage envelope from one access-state payload.
    const fn from_access(state: LoadAccessState<E>) -> Self {
        Self::Access(state)
    }

    // Build one payload-stage envelope from one payload-state payload.
    const fn from_payload(state: LoadPayloadState<E>) -> Self {
        Self::Payload(state)
    }

    // Build one surface-stage envelope from one finalized load surface.
    const fn from_surface(surface: LoadExecutionSurface<E>) -> Self {
        Self::Surface(surface)
    }

    // Extract pre-context inputs and reject stage-order drift with one invariant error.
    fn expect_inputs(
        self,
        mismatch_message: &'static str,
    ) -> Result<(ExecutablePlan<E>, LoadCursorInput, LoadExecutionMode), InternalError> {
        match self {
            Self::Inputs {
                plan,
                cursor,
                execution_mode,
            } => Ok((plan, cursor, execution_mode)),
            _ => Err(crate::db::error::query_executor_invariant(mismatch_message)),
        }
    }

    // Extract access-stage state and reject stage-order drift with one invariant error.
    fn expect_access(
        self,
        mismatch_message: &'static str,
    ) -> Result<LoadAccessState<E>, InternalError> {
        match self {
            Self::Access(state) => Ok(state),
            _ => Err(crate::db::error::query_executor_invariant(mismatch_message)),
        }
    }

    // Extract payload-stage state and reject stage-order drift with one invariant error.
    fn expect_payload(
        self,
        mismatch_message: &'static str,
    ) -> Result<LoadPayloadState<E>, InternalError> {
        match self {
            Self::Payload(state) => Ok(state),
            _ => Err(crate::db::error::query_executor_invariant(mismatch_message)),
        }
    }

    // Consume final orchestration state into one terminal surface.
    fn into_surface(self) -> Result<LoadExecutionSurface<E>, InternalError> {
        match self {
            Self::Surface(surface) => Ok(surface),
            _ => Err(crate::db::error::query_executor_invariant(
                "load stage loop must terminate with a materialized surface",
            )),
        }
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Unified load entrypoint pipeline:
    // 1) build execution context
    // 2) execute access path
    // 3) apply grouping/projection contract
    // 4) apply paging contract
    // 5) apply tracing contract
    // 6) materialize response surface
    pub(in crate::db::executor) fn execute_load(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<LoadExecutionSurface<E>, InternalError> {
        let mut state = LoadPipelineState::Inputs {
            plan,
            cursor,
            execution_mode,
        };

        for stage in plan_load_pipeline_stages() {
            state = self.execute_load_stage(stage, state)?;
        }

        state.into_surface()
    }

    // Execute one deterministic stage descriptor over stage-local state artifacts.
    fn execute_load_stage(
        &self,
        stage_descriptor: LoadPipelineStage,
        stage_state: LoadPipelineState<E>,
    ) -> Result<LoadPipelineState<E>, InternalError> {
        match stage_descriptor {
            LoadPipelineStage::BuildExecutionContext => {
                let (plan, cursor, execution_mode) = stage_state.expect_inputs(
                    "build_execution_context stage requires pre-context input artifacts",
                )?;
                let next = Self::build_execution_context(plan, cursor, execution_mode)?;

                Ok(LoadPipelineState::from_access(next))
            }
            LoadPipelineStage::ExecuteAccessPath => {
                let access_state = stage_state
                    .expect_access("execute_access_path stage requires access-state artifacts")?;
                let next = Self::execute_access_path(access_state);

                Ok(LoadPipelineState::from_access(next))
            }
            LoadPipelineStage::ApplyGroupingProjection => {
                let access_state = stage_state.expect_access(
                    "apply_grouping_projection stage requires access-state artifacts",
                )?;
                let next = self.apply_grouping_projection(access_state)?;

                Ok(LoadPipelineState::from_payload(next))
            }
            LoadPipelineStage::ApplyPaging => {
                let payload_state = stage_state
                    .expect_payload("apply_paging stage requires payload-state artifacts")?;
                let next = Self::apply_paging(payload_state)?;

                Ok(LoadPipelineState::from_payload(next))
            }
            LoadPipelineStage::ApplyTracing => {
                let payload_state = stage_state
                    .expect_payload("apply_tracing stage requires payload-state artifacts")?;
                let next = Self::apply_tracing(payload_state);

                Ok(LoadPipelineState::from_payload(next))
            }
            LoadPipelineStage::MaterializeSurface => {
                let payload_state = stage_state
                    .expect_payload("materialize_surface stage requires payload-state artifacts")?;
                let next = Self::materialize_surface(payload_state)?;

                Ok(LoadPipelineState::from_surface(next))
            }
        }
    }

    // Build one canonical execution context from mode + plan + cursor inputs.
    fn build_execution_context(
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<LoadAccessState<E>, InternalError> {
        execution_mode.validate()?;
        if !plan.mode().is_load() {
            return Err(crate::db::error::query_executor_invariant(
                "load executor requires load plans",
            ));
        }

        let resolved_cursor = Self::resolve_entrypoint_cursor(&plan, cursor, execution_mode)?;
        Ok(LoadAccessState {
            context: LoadExecutionContext::new(execution_mode),
            access_inputs: LoadAccessInputs {
                plan,
                cursor: resolved_cursor.into_cursor(),
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
        let scalar_rows_mode = context.mode.scalar_rows_mode();
        let LoadAccessInputs { plan, cursor } = access_inputs;
        let (payload, trace) = match cursor {
            PreparedLoadCursor::Scalar(resolved_continuation) => {
                let (page, trace) =
                    self.execute_scalar_path(plan, *resolved_continuation, scalar_rows_mode)?;
                (LoadExecutionPayload::Scalar(page), trace)
            }
            PreparedLoadCursor::Grouped(cursor) => {
                let (page, trace) = self.execute_grouped_path(plan, cursor)?;
                (LoadExecutionPayload::Grouped(page), trace)
            }
        };

        Ok(LoadPayloadState {
            context,
            payload,
            trace,
        })
    }
}
