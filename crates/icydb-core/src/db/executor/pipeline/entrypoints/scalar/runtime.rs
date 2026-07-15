//! Module: executor::pipeline::entrypoints::scalar::runtime
//! Responsibility: scalar route runtime bundle construction.
//! Does not own: scalar execution, sink execution, or page finalization.
//! Boundary: converts prepared scalar plan inputs into one runtime bundle.

use std::rc::Rc;

#[cfg(feature = "diagnostics")]
use crate::db::diagnostics::measure_local_instruction_delta;
use crate::{
    db::{
        Db,
        cursor::ValidatedCursor,
        executor::{
            EntityAuthority, ExecutionPreparation, ExecutionRoutePlan, PreparedLoadPlan,
            PreparedScalarPlanCore, PreparedScalarRuntimeHandoff, RetainedSlotLayout,
            ScalarContinuationContext,
            pipeline::contracts::{
                CursorEmissionMode, PreparedExecutionProjection, ProjectionMaterializationMode,
            },
            planning::route::{RoutePlanRequest, build_execution_route_plan},
            projection::PreparedProjectionContract,
            validate_executor_plan_for_authority,
        },
        query::plan::AccessPlannedQuery,
        registry::StoreHandle,
    },
    error::InternalError,
    traits::CanisterKind,
};

///
/// PreparedScalarRouteRuntime
///
/// PreparedScalarRouteRuntime is the generic-free scalar runtime bundle emitted
/// once the typed boundary resolves store authority, route planning, lowered
/// specs, and continuation inputs.
/// Kernel dispatch consumes this bundle directly so the scalar lane no longer
/// carries `LoadExecutor<E>` or `PreparedExecutionPlan<E>` behind a runtime adapter.
/// Runtime construction is intentionally centralized in this module:
/// entrypoint adapters build this bundle through `prepare_scalar_route_runtime_from_inputs`,
/// while execution and sink modules only consume an already-prepared bundle.
///

pub(in crate::db::executor) struct PreparedScalarRouteRuntime {
    pub(super) store: StoreHandle,
    pub(super) authority: EntityAuthority,
    pub(super) plan_core: PreparedScalarPlanCore,
    pub(super) route_plan: ExecutionRoutePlan,
    pub(super) prep: ExecutionPreparation,
    pub(super) projection: PreparedExecutionProjection,
    pub(super) continuation: ScalarContinuationContext,
    pub(super) unpaged_rows_mode: bool,
    pub(super) cursor_emission: CursorEmissionMode,
    pub(super) projection_runtime_mode: ProjectionMaterializationMode,
    pub(super) suppress_route_scan_hints: bool,
    pub(super) debug: bool,
}

impl PreparedScalarRouteRuntime {
    // Return the entity path needed by finalization before the runtime bundle is
    // consumed by execution.
    pub(super) const fn entity_path(&self) -> &'static str {
        self.authority.entity_path()
    }
}

///
/// ScalarRuntimePreparePhaseAttribution
///
/// ScalarRuntimePreparePhaseAttribution records the diagnostics-only phase
/// split for building an initial scalar runtime bundle. Keeping this beside
/// normal runtime preparation prevents attributed entrypoints from rebuilding
/// the scalar handoff contract independently.
///

#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ScalarRuntimePreparePhaseAttribution {
    pub(super) continuation_signature: u64,
    pub(super) scalar_runtime_handoff: u64,
    pub(super) route_plan: u64,
    pub(super) runtime_prepare: u64,
}

///
/// InitialScalarPlanRuntimeOptions
///
/// InitialScalarPlanRuntimeOptions records the per-surface knobs for no-cursor
/// scalar runtime preparation from a prepared load plan.
///

pub(super) struct InitialScalarPlanRuntimeOptions {
    pub(super) unpaged_rows_mode: bool,
    pub(super) projection_runtime_mode: ProjectionMaterializationMode,
    pub(super) suppress_route_scan_hints: bool,
}

impl InitialScalarPlanRuntimeOptions {
    pub(super) const fn unpaged_rows(
        projection_runtime_mode: ProjectionMaterializationMode,
    ) -> Self {
        Self::unpaged_rows_with_route_scan_hints(projection_runtime_mode, false)
    }

    pub(super) const fn unpaged_rows_with_route_scan_hints(
        projection_runtime_mode: ProjectionMaterializationMode,
        suppress_route_scan_hints: bool,
    ) -> Self {
        Self {
            unpaged_rows_mode: true,
            projection_runtime_mode,
            suppress_route_scan_hints,
        }
    }

    pub(super) const fn materialized_rows() -> Self {
        Self {
            unpaged_rows_mode: false,
            projection_runtime_mode: ProjectionMaterializationMode::None,
            suppress_route_scan_hints: true,
        }
    }
}

// Prepare an initial no-cursor scalar runtime from a prepared load plan,
// including the shared continuation-signature and scalar handoff extraction.
pub(super) fn prepare_initial_scalar_route_runtime_from_plan<C>(
    db: &Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
    options: InitialScalarPlanRuntimeOptions,
) -> Result<PreparedScalarRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let continuation_signature = plan.continuation_signature_for_runtime()?;
    let prepared = plan.into_scalar_runtime_handoff(
        options.projection_runtime_mode,
        CursorEmissionMode::Suppress,
    )?;

    prepare_initial_scalar_route_runtime_from_handoff(
        db,
        debug,
        prepared,
        ScalarContinuationContext::for_runtime(ValidatedCursor::none(), continuation_signature),
        options,
    )
}

// Prepare an initial no-cursor scalar runtime with the same phase split as the
// perf attribution surface. The measured path deliberately follows the same
// helper chain as normal initial runtime setup after each phase boundary.
#[cfg(feature = "diagnostics")]
pub(super) fn prepare_initial_scalar_route_runtime_from_plan_with_phase_attribution<C>(
    db: &Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
    options: InitialScalarPlanRuntimeOptions,
) -> Result<
    (
        PreparedScalarRouteRuntime,
        ScalarRuntimePreparePhaseAttribution,
    ),
    InternalError,
>
where
    C: CanisterKind,
{
    let (continuation_signature_local_instructions, continuation_signature) =
        measure_local_instruction_delta(|| plan.continuation_signature_for_runtime());
    let continuation_signature = continuation_signature?;
    let (scalar_runtime_handoff_local_instructions, prepared) =
        measure_local_instruction_delta(|| {
            plan.into_scalar_runtime_handoff(
                options.projection_runtime_mode,
                CursorEmissionMode::Suppress,
            )
        });
    let prepared = prepared?;
    let continuation =
        ScalarContinuationContext::for_runtime(ValidatedCursor::none(), continuation_signature);
    let (route_plan_local_instructions, prebuilt_route_plan) =
        measure_local_instruction_delta(|| {
            prepare_initial_scalar_route_plan_from_handoff(&prepared)
        });
    let prebuilt_route_plan = Some(prebuilt_route_plan?);
    let InitialScalarPlanRuntimeOptions {
        unpaged_rows_mode,
        projection_runtime_mode,
        suppress_route_scan_hints,
    } = options;
    let (runtime_prepare_local_instructions, prepared) = measure_local_instruction_delta(|| {
        prepare_scalar_route_runtime_from_inputs(
            db,
            debug,
            prepared.authority,
            prepared.execution_preparation,
            prepared.prepared_projection_contract,
            prepared.retained_slot_layout,
            prepared.plan_core,
            ScalarPreparedRuntimeOptions::initial_suppressed(
                continuation,
                unpaged_rows_mode,
                projection_runtime_mode,
                prebuilt_route_plan,
                suppress_route_scan_hints,
            ),
        )
    });
    let prepared = prepared?;

    Ok((
        prepared,
        ScalarRuntimePreparePhaseAttribution {
            continuation_signature: continuation_signature_local_instructions,
            scalar_runtime_handoff: scalar_runtime_handoff_local_instructions,
            route_plan: route_plan_local_instructions,
            runtime_prepare: runtime_prepare_local_instructions,
        },
    ))
}

// Prepare an initial no-cursor scalar runtime from a prepared load plan while
// replacing the retained-slot layout for this execution only.
#[cfg(feature = "sql")]
pub(super) fn prepare_initial_scalar_route_runtime_from_plan_with_retained_slot_layout<C>(
    db: &Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
    retained_slot_layout: RetainedSlotLayout,
    options: InitialScalarPlanRuntimeOptions,
) -> Result<PreparedScalarRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let continuation_signature = plan.continuation_signature_for_runtime()?;
    let prepared = plan.into_scalar_runtime_handoff_with_retained_slot_layout(
        options.projection_runtime_mode,
        CursorEmissionMode::Suppress,
        retained_slot_layout,
    )?;

    prepare_initial_scalar_route_runtime_from_handoff(
        db,
        debug,
        prepared,
        ScalarContinuationContext::for_runtime(ValidatedCursor::none(), continuation_signature),
        options,
    )
}

// Prepare a resumed cursor-aware scalar runtime from a prepared load plan.
// This keeps resumed projection materialization and cursor-emission policy in
// the same runtime boundary as initial scalar setup.
pub(super) fn prepare_resumed_scalar_route_runtime_from_plan<C>(
    db: &Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
    continuation: ScalarContinuationContext,
    unpaged_rows_mode: bool,
) -> Result<PreparedScalarRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let prepared = plan.into_scalar_runtime_handoff(
        ProjectionMaterializationMode::SharedValidation,
        CursorEmissionMode::Emit,
    )?;

    prepare_scalar_route_runtime_from_inputs(
        db,
        debug,
        prepared.authority,
        prepared.execution_preparation,
        prepared.prepared_projection_contract,
        prepared.retained_slot_layout,
        prepared.plan_core,
        ScalarPreparedRuntimeOptions::resumed_emit(
            continuation,
            unpaged_rows_mode,
            ProjectionMaterializationMode::SharedValidation,
        ),
    )
}

// Prepare the SQL retained-slot initial page runtime from a shared prepared
// scalar handoff. This owns the projection materialization decision so the SQL
// entrypoint does not repeat runtime layout policy beside runtime setup.
#[cfg(feature = "sql")]
pub(super) fn prepare_initial_scalar_retained_slot_page_runtime_from_handoff<C>(
    db: &Db<C>,
    debug: bool,
    mut prepared: PreparedScalarRuntimeHandoff,
    suppress_route_scan_hints: bool,
) -> Result<PreparedScalarRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let continuation_signature = prepared.plan_core.continuation_signature_for_runtime()?;
    let continuation =
        ScalarContinuationContext::for_runtime(ValidatedCursor::none(), continuation_signature);
    let projection_runtime_mode =
        initial_retained_slot_projection_runtime_mode(&prepared, suppress_route_scan_hints);
    prepared.retained_slot_layout = initial_retained_slot_layout(
        &prepared,
        projection_runtime_mode,
        suppress_route_scan_hints,
    )?;

    prepare_initial_scalar_route_runtime_from_handoff(
        db,
        debug,
        prepared,
        continuation,
        InitialScalarPlanRuntimeOptions::unpaged_rows_with_route_scan_hints(
            projection_runtime_mode,
            suppress_route_scan_hints,
        ),
    )
}

#[cfg(feature = "sql")]
fn initial_retained_slot_projection_runtime_mode(
    prepared: &PreparedScalarRuntimeHandoff,
    suppress_route_scan_hints: bool,
) -> ProjectionMaterializationMode {
    if matches!(
        prepared.plan_core.plan().projection_is_model_identity(),
        Ok(true)
    ) && !suppress_route_scan_hints
    {
        ProjectionMaterializationMode::None
    } else if prepared
        .prepared_projection_contract
        .as_ref()
        .is_some_and(|shape| projection_contract_requires_data_rows(shape.as_ref()))
    {
        // Nested field-path projection still needs raw persisted row bytes.
        // Plain direct fields and scalar expressions can be evaluated from the
        // retained-slot contract, which avoids carrying full data rows through
        // ordered cursorless SQL pages.
        ProjectionMaterializationMode::None
    } else {
        ProjectionMaterializationMode::RetainSlotRows
    }
}

#[cfg(feature = "sql")]
fn initial_retained_slot_layout(
    prepared: &PreparedScalarRuntimeHandoff,
    projection_runtime_mode: ProjectionMaterializationMode,
    suppress_route_scan_hints: bool,
) -> Result<Option<RetainedSlotLayout>, InternalError> {
    if prepared.plan_core.plan().projection_is_model_identity()? && !suppress_route_scan_hints {
        Ok(None)
    } else if projection_runtime_mode.validate_projection()
        || projection_runtime_mode.retain_slot_rows()
    {
        prepared.plan_core.get_or_init_scalar_layout(
            prepared.authority.clone(),
            projection_runtime_mode,
            CursorEmissionMode::Suppress,
        )
    } else {
        Ok(prepared.retained_slot_layout.clone())
    }
}

#[cfg(feature = "sql")]
fn projection_contract_requires_data_rows(shape: &PreparedProjectionContract) -> bool {
    shape.scalar_projection_contains_field_path()
}

// Prepare an initial no-cursor scalar runtime from one structural handoff.
// This keeps repeated initial-route planning and runtime option wiring out of
// the materialized, retained-slot, and aggregate row-sink entrypoints.
pub(super) fn prepare_initial_scalar_route_runtime_from_handoff<C>(
    db: &Db<C>,
    debug: bool,
    prepared: PreparedScalarRuntimeHandoff,
    continuation: ScalarContinuationContext,
    options: InitialScalarPlanRuntimeOptions,
) -> Result<PreparedScalarRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let InitialScalarPlanRuntimeOptions {
        unpaged_rows_mode,
        projection_runtime_mode,
        suppress_route_scan_hints,
    } = options;
    let prebuilt_route_plan = Some(prepare_initial_scalar_route_plan_from_handoff(&prepared)?);

    prepare_scalar_route_runtime_from_inputs(
        db,
        debug,
        prepared.authority,
        prepared.execution_preparation,
        prepared.prepared_projection_contract,
        prepared.retained_slot_layout,
        prepared.plan_core,
        ScalarPreparedRuntimeOptions::initial_suppressed(
            continuation,
            unpaged_rows_mode,
            projection_runtime_mode,
            prebuilt_route_plan,
            suppress_route_scan_hints,
        ),
    )
}

// Return the cached deterministic initial route plan for an already-prepared
// scalar handoff. Diagnostics can measure this same helper without duplicating
// the route-plan extraction contract.
pub(super) fn prepare_initial_scalar_route_plan_from_handoff(
    prepared: &PreparedScalarRuntimeHandoff,
) -> Result<ExecutionRoutePlan, InternalError> {
    prepared
        .plan_core
        .get_or_init_initial_scalar_route_plan(prepared.authority.clone())
}

///
/// ScalarRoutePlanFamily
///
/// ScalarRoutePlanFamily selects whether one scalar prepared runtime should
/// derive an initial route plan or retain a resumed continuation-aware route
/// plan during shared preparation.
/// Scalar entrypoint families use this to keep route-plan selection on one
/// helper instead of rebuilding authority/store setup in parallel flows.
///

pub(super) enum ScalarRoutePlanFamily {
    Initial,
    Resumed,
}

///
/// ScalarPlanValidationMode
///
/// ScalarPlanValidationMode records whether scalar runtime preparation still
/// needs to run the executor authority/access validation guard.
/// Prepared-load entrypoints already cross the shared planning boundary before
/// reaching this helper, while raw retained-slot helpers still require the
/// guard at the executor boundary.
///

pub(super) enum ScalarPlanValidationMode {
    Required,
    AlreadyValidated,
}

///
/// ScalarPreparedRuntimeOptions
///
/// ScalarPreparedRuntimeOptions records the per-entrypoint knobs that still
/// vary after a caller has already resolved structural authority, logical
/// plan ownership, and lowered index specs.
/// The shared scalar preparation helper consumes this once so initial,
/// resumed, retained-slot, and materialized entrypoints all follow one build
/// path.
///

pub(super) struct ScalarPreparedRuntimeOptions {
    pub(super) continuation: ScalarContinuationContext,
    pub(super) unpaged_rows_mode: bool,
    pub(super) cursor_emission: CursorEmissionMode,
    pub(super) projection_runtime_mode: ProjectionMaterializationMode,
    pub(super) route_plan_family: ScalarRoutePlanFamily,
    pub(super) prebuilt_route_plan: Option<ExecutionRoutePlan>,
    pub(super) suppress_route_scan_hints: bool,
    pub(super) plan_validation: ScalarPlanValidationMode,
}

impl ScalarPreparedRuntimeOptions {
    pub(super) const fn initial_suppressed(
        continuation: ScalarContinuationContext,
        unpaged_rows_mode: bool,
        projection_runtime_mode: ProjectionMaterializationMode,
        prebuilt_route_plan: Option<ExecutionRoutePlan>,
        suppress_route_scan_hints: bool,
    ) -> Self {
        Self {
            continuation,
            unpaged_rows_mode,
            cursor_emission: CursorEmissionMode::Suppress,
            projection_runtime_mode,
            route_plan_family: ScalarRoutePlanFamily::Initial,
            prebuilt_route_plan,
            suppress_route_scan_hints,
            plan_validation: ScalarPlanValidationMode::AlreadyValidated,
        }
    }

    pub(super) const fn resumed_emit(
        continuation: ScalarContinuationContext,
        unpaged_rows_mode: bool,
        projection_runtime_mode: ProjectionMaterializationMode,
    ) -> Self {
        Self {
            continuation,
            unpaged_rows_mode,
            cursor_emission: CursorEmissionMode::Emit,
            projection_runtime_mode,
            route_plan_family: ScalarRoutePlanFamily::Resumed,
            prebuilt_route_plan: None,
            suppress_route_scan_hints: false,
            plan_validation: ScalarPlanValidationMode::Required,
        }
    }
}

// Build the shared scalar runtime bundle once after the caller has already
// resolved the store, route plan, continuation policy, and output mode for
// this scalar execution family. Keep this constructor private so the public
// scalar subtree has exactly one runtime preparation seam.
#[expect(clippy::too_many_arguments)]
fn build_prepared_scalar_route_runtime(
    store: StoreHandle,
    authority: EntityAuthority,
    prep: ExecutionPreparation,
    prepared_projection_validation: Option<Rc<PreparedProjectionContract>>,
    prepared_retained_slot_layout: Option<RetainedSlotLayout>,
    plan_core: PreparedScalarPlanCore,
    route_plan: ExecutionRoutePlan,
    continuation: ScalarContinuationContext,
    unpaged_rows_mode: bool,
    cursor_emission: CursorEmissionMode,
    projection_runtime_mode: ProjectionMaterializationMode,
    suppress_route_scan_hints: bool,
    debug: bool,
) -> Result<PreparedScalarRouteRuntime, InternalError> {
    let projection = PreparedExecutionProjection::compile(
        authority.clone(),
        plan_core.plan(),
        prepared_projection_validation,
        prepared_retained_slot_layout,
        projection_runtime_mode,
        cursor_emission,
    )?;

    Ok(PreparedScalarRouteRuntime {
        store,
        authority,
        plan_core,
        route_plan,
        prep,
        projection,
        continuation,
        unpaged_rows_mode,
        cursor_emission,
        projection_runtime_mode,
        suppress_route_scan_hints,
        debug,
    })
}

// Prepare one scalar runtime bundle after the caller has already resolved the
// structural inputs that stay constant across initial, resumed, retained-slot,
// and materialized scalar entrypoint families.
#[expect(clippy::too_many_arguments)]
pub(super) fn prepare_scalar_route_runtime_from_inputs<C>(
    db: &Db<C>,
    debug: bool,
    authority: EntityAuthority,
    prep: ExecutionPreparation,
    prepared_projection_validation: Option<Rc<PreparedProjectionContract>>,
    prepared_retained_slot_layout: Option<RetainedSlotLayout>,
    plan_core: PreparedScalarPlanCore,
    options: ScalarPreparedRuntimeOptions,
) -> Result<PreparedScalarRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let ScalarPreparedRuntimeOptions {
        continuation,
        unpaged_rows_mode,
        cursor_emission,
        projection_runtime_mode,
        route_plan_family,
        prebuilt_route_plan,
        suppress_route_scan_hints,
        plan_validation,
    } = options;

    // Phase 1: resolve structural store authority and derive the route plan.
    let logical_plan = plan_core.plan();
    if matches!(plan_validation, ScalarPlanValidationMode::Required) {
        validate_executor_plan_for_authority(&authority, logical_plan)?;
    }
    let store = db.recovered_store(authority.store_path())?;
    let mut route_plan = match route_plan_family {
        ScalarRoutePlanFamily::Initial => match prebuilt_route_plan {
            Some(route_plan) => route_plan,
            None => build_initial_scalar_route_plan(logical_plan, authority.clone())?,
        },
        ScalarRoutePlanFamily::Resumed => build_execution_route_plan(
            logical_plan,
            RoutePlanRequest::Load {
                continuation: &continuation,
                probe_fetch_hint: None,
                authority: Some(authority.clone()),
                load_terminal_fast_path: None,
            },
        )?,
    };

    // Phase 2: apply any route-local hint adjustments required by the caller.
    if suppress_route_scan_hints {
        route_plan.scan_hints.physical_fetch_hint = None;
        route_plan.scan_hints.load_scan_budget_hint = None;
    }

    // Phase 3: hand off one canonical prepared runtime bundle to scalar execution.
    build_prepared_scalar_route_runtime(
        store,
        authority,
        prep,
        prepared_projection_validation,
        prepared_retained_slot_layout,
        plan_core,
        route_plan,
        continuation,
        unpaged_rows_mode,
        cursor_emission,
        projection_runtime_mode,
        suppress_route_scan_hints,
        debug,
    )
}

// Build the deterministic no-cursor load route for initial scalar execution.
// This isolates the reusable route-plan input shape from the resumed cursor path,
// where route derivation must stay tied to the resolved continuation.
pub(super) fn build_initial_scalar_route_plan(
    logical_plan: &AccessPlannedQuery,
    authority: EntityAuthority,
) -> Result<ExecutionRoutePlan, InternalError> {
    build_execution_route_plan(
        logical_plan,
        RoutePlanRequest::Load {
            continuation: &ScalarContinuationContext::initial(),
            probe_fetch_hint: None,
            authority: Some(authority),
            load_terminal_fast_path: None,
        },
    )
}
