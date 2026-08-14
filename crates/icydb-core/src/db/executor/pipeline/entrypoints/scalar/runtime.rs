//! Module: executor::pipeline::entrypoints::scalar::runtime
//! Responsibility: scalar route runtime bundle construction.
//! Does not own: scalar execution, sink execution, or page finalization.
//! Boundary: converts prepared scalar plan inputs into one runtime bundle.

use std::rc::Rc;

use crate::{
    db::{
        Db,
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
/// carries frontend-specific state behind a runtime adapter.
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
    pub(super) enforced_scan_probe_limit: Option<usize>,
    pub(super) debug: bool,
}

impl PreparedScalarRouteRuntime {
    // Clone the entity path needed after the runtime bundle is consumed.
    pub(super) fn entity_path_handle(&self) -> std::rc::Rc<str> {
        self.authority.entity_path_handle()
    }

    /// Attach one execution-only cap-plus-one scan probe.
    #[must_use]
    pub(super) const fn with_enforced_scan_probe_limit(mut self, probe_limit: usize) -> Self {
        self.enforced_scan_probe_limit = Some(probe_limit);
        self
    }
}

///
/// InitialScalarPlanRuntimeOptions
///
/// InitialScalarPlanRuntimeOptions records the per-surface knobs for no-cursor
/// scalar runtime preparation from a prepared load plan.
///

pub(super) struct InitialScalarPlanRuntimeOptions {
    unpaged_rows_mode: bool,
    projection_runtime_mode: ProjectionMaterializationMode,
    cursor_emission: CursorEmissionMode,
    suppress_route_scan_hints: bool,
}

impl InitialScalarPlanRuntimeOptions {
    pub(super) const fn unpaged_rows(
        projection_runtime_mode: ProjectionMaterializationMode,
    ) -> Self {
        Self::unpaged_rows_with_route_scan_hints(
            projection_runtime_mode,
            CursorEmissionMode::Suppress,
            false,
        )
    }

    pub(super) const fn unpaged_rows_with_route_scan_hints(
        projection_runtime_mode: ProjectionMaterializationMode,
        cursor_emission: CursorEmissionMode,
        suppress_route_scan_hints: bool,
    ) -> Self {
        Self {
            unpaged_rows_mode: true,
            projection_runtime_mode,
            cursor_emission,
            suppress_route_scan_hints,
        }
    }
}

// Prepare an initial no-cursor scalar runtime from a prepared load plan,
// including the shared continuation-signature and scalar handoff extraction.

// Prepare an initial no-cursor scalar runtime with the same phase split as the
// perf attribution surface. The measured path deliberately follows the same
// helper chain as normal initial runtime setup after each phase boundary.

// Prepare an initial no-cursor scalar runtime from a prepared load plan while
// replacing the retained-slot layout for this execution only.
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
    let prepared = plan.into_scalar_runtime_handoff_with_retained_slot_layout(
        options.projection_runtime_mode,
        options.cursor_emission,
        retained_slot_layout,
    )?;

    prepare_initial_scalar_route_runtime_from_handoff(
        db,
        debug,
        prepared,
        ScalarContinuationContext::initial(),
        options,
    )
}

// Prepare a resumed cursor-aware scalar runtime from a prepared load plan.
// This keeps resumed projection materialization and cursor-emission policy in
// the same runtime boundary as initial scalar setup.

pub(super) fn prepare_resumed_scalar_retained_slot_page_runtime_from_handoff<C>(
    db: &Db<C>,
    debug: bool,
    mut prepared: PreparedScalarRuntimeHandoff,
    continuation: ScalarContinuationContext,
    cursor_emission: CursorEmissionMode,
) -> Result<PreparedScalarRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let projection_runtime_mode = initial_retained_slot_projection_runtime_mode(&prepared, false);
    prepared.retained_slot_layout =
        initial_retained_slot_layout(&prepared, projection_runtime_mode, cursor_emission, false)?;

    prepare_scalar_route_runtime_from_inputs(
        db,
        debug,
        prepared.authority,
        prepared.execution_preparation,
        prepared.prepared_projection_contract,
        prepared.retained_slot_layout,
        prepared.plan_core,
        ScalarPreparedRuntimeOptions::resumed(
            continuation,
            projection_runtime_mode,
            cursor_emission,
        ),
    )
}

// Prepare the SQL retained-slot initial page runtime from a shared prepared
// scalar handoff. This owns the projection materialization decision so the SQL
// entrypoint does not repeat runtime layout policy beside runtime setup.
pub(super) fn prepare_initial_scalar_retained_slot_page_runtime_from_handoff<C>(
    db: &Db<C>,
    debug: bool,
    mut prepared: PreparedScalarRuntimeHandoff,
    cursor_emission: CursorEmissionMode,
    suppress_route_scan_hints: bool,
) -> Result<PreparedScalarRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let continuation = ScalarContinuationContext::initial();
    let projection_runtime_mode =
        initial_retained_slot_projection_runtime_mode(&prepared, suppress_route_scan_hints);
    prepared.retained_slot_layout = initial_retained_slot_layout(
        &prepared,
        projection_runtime_mode,
        cursor_emission,
        suppress_route_scan_hints,
    )?;

    prepare_initial_scalar_route_runtime_from_handoff(
        db,
        debug,
        prepared,
        continuation,
        InitialScalarPlanRuntimeOptions::unpaged_rows_with_route_scan_hints(
            projection_runtime_mode,
            cursor_emission,
            suppress_route_scan_hints,
        ),
    )
}

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

fn initial_retained_slot_layout(
    prepared: &PreparedScalarRuntimeHandoff,
    projection_runtime_mode: ProjectionMaterializationMode,
    cursor_emission: CursorEmissionMode,
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
            cursor_emission,
        )
    } else {
        Ok(prepared.retained_slot_layout.clone())
    }
}

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
        cursor_emission,
        suppress_route_scan_hints,
    } = options;
    let prebuilt_route_plan = prepare_initial_scalar_route_plan_from_handoff(&prepared);

    prepare_scalar_route_runtime_from_inputs(
        db,
        debug,
        prepared.authority,
        prepared.execution_preparation,
        prepared.prepared_projection_contract,
        prepared.retained_slot_layout,
        prepared.plan_core,
        ScalarPreparedRuntimeOptions::initial(
            continuation,
            unpaged_rows_mode,
            projection_runtime_mode,
            cursor_emission,
            prebuilt_route_plan,
            suppress_route_scan_hints,
        ),
    )
}

// Return the cached deterministic initial route plan for an already-prepared
// scalar handoff. Diagnostics can measure this same helper without duplicating
// the route-plan extraction contract.
fn prepare_initial_scalar_route_plan_from_handoff(
    prepared: &PreparedScalarRuntimeHandoff,
) -> ExecutionRoutePlan {
    prepared
        .plan_core
        .get_or_init_initial_scalar_route_plan(prepared.authority.clone())
}

///
/// ScalarRouteSource
///
/// ScalarRouteSource keeps each route family with the state required to
/// resolve it. Initial execution carries its already-prepared deterministic
/// route and continuation together.
///

#[expect(
    clippy::large_enum_variant,
    reason = "the initial hot route remains inline to avoid one allocation per scalar execution"
)]
enum ScalarRouteSource {
    Initial {
        route_plan: ExecutionRoutePlan,
        continuation: ScalarContinuationContext,
    },
    Resumed {
        continuation: ScalarContinuationContext,
    },
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

struct ScalarPreparedRuntimeOptions {
    unpaged_rows_mode: bool,
    cursor_emission: CursorEmissionMode,
    projection_runtime_mode: ProjectionMaterializationMode,
    route_source: ScalarRouteSource,
    suppress_route_scan_hints: bool,
}

impl ScalarPreparedRuntimeOptions {
    const fn initial(
        continuation: ScalarContinuationContext,
        unpaged_rows_mode: bool,
        projection_runtime_mode: ProjectionMaterializationMode,
        cursor_emission: CursorEmissionMode,
        route_plan: ExecutionRoutePlan,
        suppress_route_scan_hints: bool,
    ) -> Self {
        Self {
            unpaged_rows_mode,
            cursor_emission,
            projection_runtime_mode,
            route_source: ScalarRouteSource::Initial {
                route_plan,
                continuation,
            },
            suppress_route_scan_hints,
        }
    }

    const fn resumed(
        continuation: ScalarContinuationContext,
        projection_runtime_mode: ProjectionMaterializationMode,
        cursor_emission: CursorEmissionMode,
    ) -> Self {
        Self {
            unpaged_rows_mode: false,
            cursor_emission,
            projection_runtime_mode,
            route_source: ScalarRouteSource::Resumed { continuation },
            suppress_route_scan_hints: false,
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
        enforced_scan_probe_limit: None,
        debug,
    })
}

// Prepare one scalar runtime bundle after the caller has already resolved the
// structural inputs that stay constant across initial, resumed, retained-slot,
// and materialized scalar entrypoint families.
#[expect(clippy::too_many_arguments)]
fn prepare_scalar_route_runtime_from_inputs<C>(
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
        unpaged_rows_mode,
        cursor_emission,
        projection_runtime_mode,
        route_source,
        suppress_route_scan_hints,
    } = options;

    // Phase 1: validate structural authority once, resolve the store, and
    // consume the variant-owned route source.
    let logical_plan = plan_core.plan();
    validate_executor_plan_for_authority(&authority, logical_plan)?;
    let store = db.recovered_store(authority.store_path())?;
    let (route_plan, continuation) = match route_source {
        ScalarRouteSource::Initial {
            route_plan,
            continuation,
        } => (route_plan, continuation),
        ScalarRouteSource::Resumed { continuation } => {
            let route_plan = build_execution_route_plan(
                logical_plan,
                RoutePlanRequest::Load {
                    continuation: continuation.clone(),
                    probe_fetch_hint: None,
                    authority: Some(Box::new(authority.clone())),
                    load_terminal_fast_path: None,
                },
            );
            (route_plan, continuation)
        }
    };

    // Phase 2: hand off one canonical prepared runtime bundle. Execution owns
    // the single final route-hint normalization pass.
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
