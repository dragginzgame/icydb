use crate::{
    db::{
        executor::{
            ExecutablePlan, ExecutionTrace, LoadCursorInput, PreparedLoadCursor,
            RequestedLoadExecutionShape,
            load::{CursorPage, GroupedCursorPage, LoadExecutor},
        },
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// LoadTracingMode
///
/// Trace emission contract for one load orchestration request.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor::load) enum LoadTracingMode {
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
enum LoadMode {
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
pub(in crate::db::executor::load) struct LoadExecutionMode {
    mode: LoadMode,
    tracing: LoadTracingMode,
}

impl LoadExecutionMode {
    // Build one scalar unpaged rows mode contract.
    pub(in crate::db::executor::load) const fn scalar_unpaged_rows() -> Self {
        Self {
            mode: LoadMode::ScalarRows,
            tracing: LoadTracingMode::Disabled,
        }
    }

    // Build one scalar paged mode contract with configurable tracing.
    pub(in crate::db::executor::load) const fn scalar_paged(tracing: LoadTracingMode) -> Self {
        Self {
            mode: LoadMode::ScalarPage,
            tracing,
        }
    }

    // Build one grouped paged mode contract with configurable tracing.
    pub(in crate::db::executor::load) const fn grouped_paged(tracing: LoadTracingMode) -> Self {
        Self {
            mode: LoadMode::GroupedPage,
            tracing,
        }
    }

    // Validate one mode tuple so wrappers cannot silently drift.
    fn validate(self) -> Result<(), InternalError> {
        if matches!(
            (self.mode, self.tracing),
            (LoadMode::ScalarRows, LoadTracingMode::Enabled)
        ) {
            Err(crate::db::executor::load::invariant(
                "scalar rows load mode must not request tracing output",
            ))
        } else {
            Ok(())
        }
    }

    // Resolve entrypoint-selected mode into the requested scalar/grouped execution shape.
    pub(in crate::db::executor::load) const fn requested_shape(
        self,
    ) -> RequestedLoadExecutionShape {
        match self.mode {
            LoadMode::ScalarRows | LoadMode::ScalarPage => RequestedLoadExecutionShape::Scalar,
            LoadMode::GroupedPage => RequestedLoadExecutionShape::Grouped,
        }
    }
}

///
/// LoadExecutionContext
///
/// Canonical execution artifacts normalized before staged orchestration.
/// Owns immutable entrypoint mode contracts consumed by pipeline stages.
///

struct LoadExecutionContext {
    mode: LoadExecutionMode,
}

impl LoadExecutionContext {
    // Construct one immutable execution context from one normalized mode contract.
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
    cursor: PreparedLoadCursor,
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
/// LoadExecutionSurface
///
/// Finalized load output surface for entrypoint wrappers.
/// Encodes one terminal response shape so wrapper adapters do not carry
/// payload/trace pairing branches.
///

pub(in crate::db::executor::load) enum LoadExecutionSurface<E: EntityKind> {
    ScalarRows(EntityResponse<E>),
    ScalarPage(CursorPage<E>),
    ScalarPageWithTrace(CursorPage<E>, Option<ExecutionTrace>),
    GroupedPageWithTrace(GroupedCursorPage, Option<ExecutionTrace>),
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
    pub(in crate::db::executor::load) fn execute_load(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
        execution_mode: LoadExecutionMode,
    ) -> Result<LoadExecutionSurface<E>, InternalError> {
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
            return Err(crate::db::executor::load::invariant(
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
        let load_mode = context.mode.mode;
        let LoadAccessInputs { plan, cursor } = access_inputs;
        let (payload, trace) = match cursor {
            PreparedLoadCursor::Scalar(resolved_continuation) => {
                let (page, trace) = self.execute_scalar_path(
                    plan,
                    *resolved_continuation,
                    matches!(load_mode, LoadMode::ScalarRows),
                )?;
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

    // Apply paging contracts over staged payload artifacts.
    fn apply_paging(mut state: LoadPayloadState<E>) -> Result<LoadPayloadState<E>, InternalError> {
        let payload = match state.context.mode.mode {
            LoadMode::ScalarRows => {
                let mut page = Self::expect_scalar_payload(
                    state.payload,
                    "unpaged load execution mode must carry scalar payload",
                )?;
                // Unpaged scalar execution intentionally suppresses continuation payload.
                page.next_cursor = None;
                LoadExecutionPayload::Scalar(page)
            }
            LoadMode::ScalarPage => LoadExecutionPayload::Scalar(Self::expect_scalar_payload(
                state.payload,
                "scalar page load mode must carry scalar payload",
            )?),
            LoadMode::GroupedPage => LoadExecutionPayload::Grouped(Self::expect_grouped_payload(
                state.payload,
                "grouped page load mode must carry grouped payload",
            )?),
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
    ) -> Result<LoadExecutionSurface<E>, InternalError> {
        match state.context.mode.mode {
            LoadMode::ScalarRows => {
                let page = Self::expect_scalar_payload(
                    state.payload,
                    "rows load surface mode must carry scalar payload",
                )?;

                Ok(LoadExecutionSurface::ScalarRows(page.items))
            }
            LoadMode::ScalarPage => {
                let page = Self::expect_scalar_payload(
                    state.payload,
                    "scalar page load mode must carry scalar payload",
                )?;

                if matches!(state.context.mode.tracing, LoadTracingMode::Enabled) {
                    Ok(LoadExecutionSurface::ScalarPageWithTrace(page, state.trace))
                } else {
                    Ok(LoadExecutionSurface::ScalarPage(page))
                }
            }
            LoadMode::GroupedPage => {
                let page = Self::expect_grouped_payload(
                    state.payload,
                    "grouped page load mode must carry grouped payload",
                )?;

                Ok(LoadExecutionSurface::GroupedPageWithTrace(
                    page,
                    state.trace,
                ))
            }
        }
    }

    // Extract scalar payload at one stage boundary and classify mismatches.
    fn expect_scalar_payload(
        payload: LoadExecutionPayload<E>,
        mismatch_message: &'static str,
    ) -> Result<CursorPage<E>, InternalError> {
        match payload {
            LoadExecutionPayload::Scalar(page) => Ok(page),
            LoadExecutionPayload::Grouped(_) => {
                Err(crate::db::executor::load::invariant(mismatch_message))
            }
        }
    }

    // Extract grouped payload at one stage boundary and classify mismatches.
    fn expect_grouped_payload(
        payload: LoadExecutionPayload<E>,
        mismatch_message: &'static str,
    ) -> Result<GroupedCursorPage, InternalError> {
        match payload {
            LoadExecutionPayload::Grouped(page) => Ok(page),
            LoadExecutionPayload::Scalar(_) => {
                Err(crate::db::executor::load::invariant(mismatch_message))
            }
        }
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
