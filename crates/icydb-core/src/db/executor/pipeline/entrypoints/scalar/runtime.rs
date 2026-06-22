//! Module: executor::pipeline::entrypoints::scalar::runtime
//! Responsibility: scalar route runtime bundle construction.
//! Does not own: scalar execution, sink execution, or page finalization.
//! Boundary: converts prepared scalar plan inputs into one runtime bundle.

use std::sync::Arc;

use crate::{
    db::{
        Db,
        cursor::ValidatedCursor,
        executor::{
            EntityAuthority, ExecutionPlan, ExecutionPreparation, PreparedLoadPlan,
            PreparedScalarPlanCore, PreparedScalarRuntimeHandoff, ScalarContinuationContext,
            pipeline::contracts::{
                CursorEmissionMode, PreparedExecutionProjection, ProjectionMaterializationMode,
            },
            planning::route::{RoutePlanRequest, build_execution_route_plan},
            validate_executor_plan_for_authority,
        },
        query::plan::AccessPlannedQuery,
        registry::StoreHandle,
    },
    error::InternalError,
    traits::CanisterKind,
};

pub(super) type ScalarProjectionRuntimeMode = ProjectionMaterializationMode;

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
    pub(super) route_plan: ExecutionPlan,
    pub(super) prep: ExecutionPreparation,
    pub(super) projection: PreparedExecutionProjection,
    pub(super) continuation: ScalarContinuationContext,
    pub(super) unpaged_rows_mode: bool,
    pub(super) cursor_emission: CursorEmissionMode,
    pub(super) projection_runtime_mode: ScalarProjectionRuntimeMode,
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
/// InitialScalarPlanRuntimeOptions
///
/// InitialScalarPlanRuntimeOptions records the per-surface knobs for no-cursor
/// scalar runtime preparation from a prepared load plan.
///

pub(super) struct InitialScalarPlanRuntimeOptions {
    pub(super) unpaged_rows_mode: bool,
    pub(super) projection_runtime_mode: ScalarProjectionRuntimeMode,
    pub(super) suppress_route_scan_hints: bool,
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

    prepare_initial_scalar_route_runtime_from_plan_handoff(
        db,
        debug,
        prepared,
        options,
        continuation_signature,
    )
}

// Prepare an initial no-cursor scalar runtime from a prepared load plan while
// replacing the retained-slot layout for this execution only.
#[cfg(feature = "sql")]
pub(super) fn prepare_initial_scalar_route_runtime_from_plan_with_retained_slot_layout<C>(
    db: &Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
    retained_slot_layout: crate::db::executor::RetainedSlotLayout,
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

    prepare_initial_scalar_route_runtime_from_plan_handoff(
        db,
        debug,
        prepared,
        options,
        continuation_signature,
    )
}

fn prepare_initial_scalar_route_runtime_from_plan_handoff<C>(
    db: &Db<C>,
    debug: bool,
    prepared: PreparedScalarRuntimeHandoff,
    options: InitialScalarPlanRuntimeOptions,
    continuation_signature: crate::db::cursor::ContinuationSignature,
) -> Result<PreparedScalarRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let InitialScalarPlanRuntimeOptions {
        unpaged_rows_mode,
        projection_runtime_mode,
        suppress_route_scan_hints,
    } = options;
    let continuation =
        ScalarContinuationContext::for_runtime(ValidatedCursor::none(), continuation_signature);

    prepare_initial_scalar_route_runtime_from_handoff(
        db,
        debug,
        prepared,
        InitialScalarRuntimeOptions {
            continuation,
            unpaged_rows_mode,
            projection_runtime_mode,
            suppress_route_scan_hints,
        },
    )
}

///
/// InitialScalarRuntimeOptions
///
/// InitialScalarRuntimeOptions records the per-surface knobs for no-cursor
/// scalar runtime preparation after a caller has already produced a structural
/// scalar runtime handoff.
///

pub(super) struct InitialScalarRuntimeOptions {
    pub(super) continuation: ScalarContinuationContext,
    pub(super) unpaged_rows_mode: bool,
    pub(super) projection_runtime_mode: ScalarProjectionRuntimeMode,
    pub(super) suppress_route_scan_hints: bool,
}

// Prepare an initial no-cursor scalar runtime from one structural handoff.
// This keeps repeated initial-route planning and runtime option wiring out of
// the materialized, retained-slot, and aggregate row-sink entrypoints.
pub(super) fn prepare_initial_scalar_route_runtime_from_handoff<C>(
    db: &Db<C>,
    debug: bool,
    prepared: PreparedScalarRuntimeHandoff,
    options: InitialScalarRuntimeOptions,
) -> Result<PreparedScalarRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let InitialScalarRuntimeOptions {
        continuation,
        unpaged_rows_mode,
        projection_runtime_mode,
        suppress_route_scan_hints,
    } = options;
    let prebuilt_route_plan = Some(
        prepared
            .plan_core
            .get_or_init_initial_scalar_route_plan(prepared.authority.clone())?,
    );

    prepare_scalar_route_runtime_from_inputs(
        db,
        debug,
        prepared.authority,
        prepared.execution_preparation,
        prepared.prepared_projection_contract,
        prepared.retained_slot_layout,
        prepared.plan_core,
        ScalarPreparedRuntimeOptions {
            continuation,
            unpaged_rows_mode,
            cursor_emission: CursorEmissionMode::Suppress,
            projection_runtime_mode,
            route_plan_family: ScalarRoutePlanFamily::Initial,
            prebuilt_route_plan,
            suppress_route_scan_hints,
            plan_validation: ScalarPlanValidationMode::AlreadyValidated,
        },
    )
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
    pub(super) projection_runtime_mode: ScalarProjectionRuntimeMode,
    pub(super) route_plan_family: ScalarRoutePlanFamily,
    pub(super) prebuilt_route_plan: Option<ExecutionPlan>,
    pub(super) suppress_route_scan_hints: bool,
    pub(super) plan_validation: ScalarPlanValidationMode,
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
    prepared_projection_validation: Option<
        Arc<crate::db::executor::projection::PreparedProjectionContract>,
    >,
    prepared_retained_slot_layout: Option<crate::db::executor::RetainedSlotLayout>,
    plan_core: PreparedScalarPlanCore,
    route_plan: ExecutionPlan,
    continuation: ScalarContinuationContext,
    unpaged_rows_mode: bool,
    cursor_emission: CursorEmissionMode,
    projection_runtime_mode: ScalarProjectionRuntimeMode,
    suppress_route_scan_hints: bool,
    debug: bool,
) -> PreparedScalarRouteRuntime {
    let projection = PreparedExecutionProjection::compile(
        authority.clone(),
        plan_core.plan(),
        prepared_projection_validation,
        prepared_retained_slot_layout,
        projection_runtime_mode,
        cursor_emission,
    );

    PreparedScalarRouteRuntime {
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
    }
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
    prepared_projection_validation: Option<
        Arc<crate::db::executor::projection::PreparedProjectionContract>,
    >,
    prepared_retained_slot_layout: Option<crate::db::executor::RetainedSlotLayout>,
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
    Ok(build_prepared_scalar_route_runtime(
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
    ))
}

// Build the deterministic no-cursor load route for initial scalar execution.
// This isolates the reusable route-plan input shape from the resumed cursor path,
// where route derivation must stay tied to the resolved continuation.
pub(super) fn build_initial_scalar_route_plan(
    logical_plan: &AccessPlannedQuery,
    authority: EntityAuthority,
) -> Result<ExecutionPlan, InternalError> {
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
