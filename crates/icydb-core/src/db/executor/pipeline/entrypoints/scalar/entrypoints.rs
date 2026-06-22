//! Module: executor::pipeline::entrypoints::scalar::entrypoints
//! Responsibility: public scalar executor entrypoints and typed boundary adapters.
//! Does not own: scalar route execution loops, aggregate sink kernels, or finalization internals.
//! Boundary: shapes caller inputs into prepared scalar runtimes and delegates execution.

use crate::db::executor::aggregate::PreparedAggregateStreamingInputs;
#[cfg(feature = "sql")]
use crate::db::{
    executor::{
        PreparedScalarRuntimeHandoff,
        pipeline::entrypoints::scalar::streaming::execute_prepared_scalar_kernel_row_sink_execution,
        terminal::KernelRow,
    },
    query::plan::expr::CompiledExpr,
};
use crate::{
    db::{
        Db, PersistedRow,
        executor::{
            EntityAuthority, LoadCursorInput, PreparedLoadPlan, ScalarContinuationContext,
            StoreResolver,
            pipeline::{
                contracts::{CursorEmissionMode, CursorPage, LoadExecutor, StructuralCursorPage},
                entrypoints::scalar::{
                    materialized::{
                        execute_prepared_scalar_route_runtime,
                        execute_prepared_scalar_structural_page,
                    },
                    runtime::{
                        InitialScalarPlanRuntimeOptions, PreparedScalarRouteRuntime,
                        ScalarPlanValidationMode, ScalarPreparedRuntimeOptions,
                        ScalarProjectionRuntimeMode, ScalarRoutePlanFamily,
                        prepare_initial_scalar_route_runtime_from_plan,
                        prepare_scalar_route_runtime_from_inputs,
                    },
                },
                orchestrator::LoadExecutionSurface,
            },
            terminal::{RowLayout, decode_data_rows_into_cursor_page},
            validate_executor_plan_for_authority,
        },
        predicate::MissingRowPolicy,
        query::plan::{AccessPlannedQuery, OrderSpec, PageSpec},
        registry::StoreHandle,
    },
    error::InternalError,
    traits::{CanisterKind, EntityKind, EntityValue},
};

#[cfg(any(feature = "diagnostics", feature = "sql"))]
use crate::db::cursor::ValidatedCursor;
#[cfg(feature = "diagnostics")]
use crate::db::executor::pipeline::entrypoints::scalar::diagnostics::{
    ScalarExecutePhaseAttribution, execute_prepared_scalar_route_runtime_with_phase_attribution,
};
#[cfg(feature = "sql")]
use crate::db::executor::pipeline::entrypoints::scalar::runtime::{
    InitialScalarRuntimeOptions, prepare_initial_scalar_route_runtime_from_handoff,
    prepare_initial_scalar_route_runtime_from_plan_with_retained_slot_layout,
};

///
/// PreparedScalarMaterializedBoundary
///
/// PreparedScalarMaterializedBoundary is the neutral typed boundary payload for
/// non-aggregate scalar materialized terminal families.
/// It owns structural runtime authority, logical plan state, and lowered specs
/// needed to execute structural scalar materialization without reusing
/// `PreparedExecutionPlan<E>` as the internal working contract.
///

pub(in crate::db::executor) struct PreparedScalarMaterializedBoundary<'ctx> {
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) store: StoreHandle,
    pub(in crate::db::executor) store_resolver: StoreResolver<'ctx>,
    pub(in crate::db::executor) plan: PreparedLoadPlan,
}

impl PreparedScalarMaterializedBoundary<'_> {
    /// Borrow the prepared logical plan behind this materialized boundary.
    #[must_use]
    pub(in crate::db::executor) fn logical_plan(&self) -> &AccessPlannedQuery {
        self.plan.logical_plan()
    }

    /// Borrow the canonical lowered index-prefix specs prepared with this plan.
    pub(in crate::db::executor) fn index_prefix_specs(
        &self,
    ) -> Result<&[crate::db::executor::LoweredIndexPrefixSpec], InternalError> {
        self.plan.index_prefix_specs()
    }

    /// Borrow the canonical lowered index-range specs prepared with this plan.
    pub(in crate::db::executor) fn index_range_specs(
        &self,
    ) -> Result<&[crate::db::executor::LoweredIndexRangeSpec], InternalError> {
        self.plan.index_range_specs()
    }

    /// Borrow scalar row-consistency policy for boundary-owned row reads.
    #[must_use]
    pub(in crate::db::executor) fn consistency(&self) -> MissingRowPolicy {
        crate::db::executor::traversal::row_read_consistency_for_plan(self.logical_plan())
    }

    /// Borrow scalar ORDER BY contract at the non-aggregate scalar boundary.
    #[must_use]
    pub(in crate::db::executor) fn order_spec(&self) -> Option<&OrderSpec> {
        self.logical_plan().scalar_plan().order.as_ref()
    }

    /// Borrow scalar pagination contract at the non-aggregate scalar boundary.
    #[must_use]
    pub(in crate::db::executor) fn page_spec(&self) -> Option<&PageSpec> {
        self.logical_plan().scalar_plan().page.as_ref()
    }

    /// Return whether the boundary still has a residual filter.
    #[must_use]
    pub(in crate::db::executor) fn has_predicate(&self) -> bool {
        self.logical_plan().has_residual_filter_expr()
            || self.logical_plan().has_residual_filter_predicate()
    }
}

impl<E> LoadExecutor<E>
where
    E: PersistedRow + EntityValue,
{
    // Execute one traced paged scalar load and materialize traced page output.
    pub(in crate::db::executor) fn execute_load_scalar_page_with_trace(
        &self,
        plan: PreparedLoadPlan,
        cursor: LoadCursorInput,
    ) -> Result<(CursorPage<E>, Option<crate::db::executor::ExecutionTrace>), InternalError> {
        let row_layout = plan.authority().row_layout();
        let surface = self.execute_load_surface(
            plan,
            cursor,
            crate::db::executor::pipeline::entrypoints::LoadSurfaceMode::scalar_paged(
                crate::db::executor::pipeline::entrypoints::LoadTracingMode::Enabled,
            ),
        )?;

        Self::expect_scalar_traced_surface(&row_layout, surface)
    }

    // Project one traced paged scalar load surface and classify shape mismatches.
    fn expect_scalar_traced_surface(
        row_layout: &RowLayout,
        surface: LoadExecutionSurface,
    ) -> Result<(CursorPage<E>, Option<crate::db::executor::ExecutionTrace>), InternalError> {
        match surface {
            LoadExecutionSurface::ScalarPageWithTrace(page, trace) => {
                let (data_rows, next_cursor) = page.into_data_rows_and_cursor();

                Ok((
                    decode_data_rows_into_cursor_page::<E>(row_layout, data_rows, next_cursor)?,
                    trace,
                ))
            }
            LoadExecutionSurface::GroupedPageWithTrace(..) => {
                Err(InternalError::query_executor_invariant())
            }
        }
    }
}

// Execute one unpaged scalar rows path once per canister and return the
// structural page at the typed boundary.
pub(in crate::db::executor) fn execute_prepared_scalar_rows_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
) -> Result<StructuralCursorPage, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: build one dedicated initial scalar runtime bundle for the
    // query-only canister rows surface.
    let prepared = prepare_initial_scalar_route_runtime_from_plan(
        db,
        debug,
        plan,
        InitialScalarPlanRuntimeOptions {
            unpaged_rows_mode: true,
            projection_runtime_mode: ScalarProjectionRuntimeMode::None,
            suppress_route_scan_hints: false,
        },
    )?;

    // Phase 2: execute the shared scalar runtime and return the structural page.
    let (page, _) = execute_prepared_scalar_route_runtime(prepared)?;

    Ok(page)
}

/// Execute one unpaged scalar rows path once per canister while reporting the
/// internal runtime/finalize split for perf-only fluent attribution.
#[cfg(feature = "diagnostics")]
pub(in crate::db::executor) fn execute_prepared_scalar_rows_for_canister_with_phase_attribution<C>(
    db: &Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
) -> Result<(StructuralCursorPage, ScalarExecutePhaseAttribution), InternalError>
where
    C: CanisterKind,
{
    // Phase 1: build one dedicated initial scalar runtime bundle for the
    // query-only canister rows surface.
    let (continuation_signature_local_instructions, continuation_signature) =
        crate::db::diagnostics::measure_local_instruction_delta(|| {
            plan.continuation_signature_for_runtime()
        });
    let continuation_signature = continuation_signature?;
    let (scalar_runtime_handoff_local_instructions, prepared) =
        crate::db::diagnostics::measure_local_instruction_delta(|| {
            plan.into_scalar_runtime_handoff(
                ScalarProjectionRuntimeMode::None,
                CursorEmissionMode::Suppress,
            )
        });
    let prepared = prepared?;
    let continuation =
        ScalarContinuationContext::for_runtime(ValidatedCursor::none(), continuation_signature);
    let (route_plan_local_instructions, prebuilt_route_plan) =
        crate::db::diagnostics::measure_local_instruction_delta(|| {
            prepared
                .plan_core
                .get_or_init_initial_scalar_route_plan(prepared.authority.clone())
        });
    let prebuilt_route_plan = Some(prebuilt_route_plan?);
    let (runtime_prepare_local_instructions, prepared) =
        crate::db::diagnostics::measure_local_instruction_delta(|| {
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
                    unpaged_rows_mode: true,
                    cursor_emission: CursorEmissionMode::Suppress,
                    projection_runtime_mode: ScalarProjectionRuntimeMode::None,
                    route_plan_family: ScalarRoutePlanFamily::Initial,
                    prebuilt_route_plan,
                    suppress_route_scan_hints: false,
                    plan_validation: ScalarPlanValidationMode::AlreadyValidated,
                },
            )
        });
    let prepared = prepared?;

    // Phase 2: execute the shared scalar runtime and return the structural page.
    let (page, _, mut phase_attribution) =
        execute_prepared_scalar_route_runtime_with_phase_attribution(prepared)?;
    phase_attribution.continuation_signature_local_instructions =
        continuation_signature_local_instructions;
    phase_attribution.scalar_runtime_handoff_local_instructions =
        scalar_runtime_handoff_local_instructions;
    phase_attribution.route_plan_local_instructions = route_plan_local_instructions;
    phase_attribution.runtime_prepare_local_instructions = runtime_prepare_local_instructions;

    Ok((page, phase_attribution))
}

/// Execute one retained-slot initial scalar rows path from prepared runtime handoff.
///
/// This entrypoint keeps SQL scalar projection execution inside the prepared
/// plan resident boundary. It consumes the scalar preparation, retained-slot
/// layout, lowered access specs, and logical plan handle produced by
/// `SharedPreparedExecutionPlan` instead of rebuilding them from a raw
/// `AccessPlannedQuery`.
#[cfg(feature = "sql")]
pub(in crate::db::executor) fn execute_initial_scalar_retained_slot_page_from_runtime_handoff_for_canister<
    C,
>(
    db: &Db<C>,
    debug: bool,
    prepared: PreparedScalarRuntimeHandoff,
    suppress_route_scan_hints: bool,
) -> Result<StructuralCursorPage, InternalError>
where
    C: CanisterKind,
{
    let continuation_signature = prepared.plan_core.continuation_signature_for_runtime()?;
    let continuation =
        ScalarContinuationContext::for_runtime(ValidatedCursor::none(), continuation_signature);
    let mut prepared = prepared;
    let projection_requires_data_rows = prepared
        .prepared_projection_contract
        .as_ref()
        .is_some_and(|shape| projection_contract_requires_data_rows(shape.as_ref()));
    let identity_projection_passthrough =
        prepared.plan_core.plan().projection_is_model_identity() && !suppress_route_scan_hints;
    let projection_runtime_mode = if identity_projection_passthrough {
        ScalarProjectionRuntimeMode::None
    } else if projection_requires_data_rows {
        // Nested field-path projection still needs raw persisted row bytes.
        // Plain direct fields and scalar expressions can be evaluated from the
        // retained-slot contract, which avoids carrying full data rows through
        // ordered cursorless SQL pages.
        ScalarProjectionRuntimeMode::None
    } else {
        ScalarProjectionRuntimeMode::RetainSlotRows
    };
    let retained_slot_layout = if identity_projection_passthrough {
        None
    } else if projection_runtime_mode.validate_projection()
        || projection_runtime_mode.retain_slot_rows()
    {
        prepared.plan_core.get_or_init_scalar_layout(
            prepared.authority.clone(),
            projection_runtime_mode,
            CursorEmissionMode::Suppress,
        )
    } else {
        prepared.retained_slot_layout.clone()
    };
    prepared.retained_slot_layout = retained_slot_layout;

    // Phase 1: prepare the scalar route runtime from plan-resident handoff.
    let prepared = prepare_initial_scalar_route_runtime_from_handoff(
        db,
        debug,
        prepared,
        InitialScalarRuntimeOptions {
            continuation,
            unpaged_rows_mode: true,
            projection_runtime_mode,
            suppress_route_scan_hints,
        },
    )?;
    let (page, _) = execute_prepared_scalar_route_runtime(prepared)?;

    Ok(page)
}

#[cfg(feature = "sql")]
fn projection_contract_requires_data_rows(
    shape: &crate::db::executor::projection::PreparedProjectionContract,
) -> bool {
    shape
        .scalar_projection_exprs()
        .iter()
        .any(CompiledExpr::contains_field_path)
}

/// Execute one prepared scalar plan with a caller-owned retained-slot layout and
/// feed post-access kernel rows into an aggregate reducer sink.
#[cfg(feature = "sql")]
pub(in crate::db::executor) fn execute_prepared_scalar_aggregate_kernel_row_sink_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
    retained_slot_layout: crate::db::executor::RetainedSlotLayout,
    row_sink: impl FnMut(&KernelRow) -> Result<(), InternalError>,
) -> Result<(), InternalError>
where
    C: CanisterKind,
{
    // Phase 1: preserve the prepared scalar access/window plan while replacing
    // only the retained-slot decode layout for this terminal-owned execution.
    let prepared = prepare_initial_scalar_route_runtime_from_plan_with_retained_slot_layout(
        db,
        debug,
        plan,
        retained_slot_layout,
        InitialScalarPlanRuntimeOptions {
            unpaged_rows_mode: true,
            projection_runtime_mode: ScalarProjectionRuntimeMode::RetainSlotRows,
            suppress_route_scan_hints: false,
        },
    )?;

    // Phase 2: execute through the scalar runtime up to the post-access/window
    // row boundary, then feed aggregate reducers without retained-slot page
    // payload construction escaping back to adapter code.
    execute_prepared_scalar_kernel_row_sink_execution(prepared, row_sink)?;

    Ok(())
}

// Execute one fully materialized scalar rows path from already-resolved typed
// boundary inputs without re-entering the generic `execute(plan)` wrapper.
fn execute_scalar_materialized_rows_boundary<E>(
    executor: &LoadExecutor<E>,
    plan: PreparedLoadPlan,
) -> Result<StructuralCursorPage, InternalError>
where
    E: EntityKind + EntityValue,
{
    // Phase 1: execute the shared scalar runtime through the same prepared
    // route bundle used by the other scalar entrypoint families.
    let prepared = prepare_initial_scalar_route_runtime_from_plan(
        &executor.db,
        executor.debug,
        plan,
        InitialScalarPlanRuntimeOptions {
            unpaged_rows_mode: false,
            projection_runtime_mode: ScalarProjectionRuntimeMode::None,
            suppress_route_scan_hints: true,
        },
    )?;
    let (page, _) = execute_prepared_scalar_structural_page(prepared)?;

    Ok(page)
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Consume one typed scalar plan into the neutral non-aggregate
    // materialized-terminal boundary payload.
    pub(in crate::db::executor) fn prepare_scalar_materialized_boundary(
        &self,
        plan: PreparedLoadPlan,
    ) -> Result<PreparedScalarMaterializedBoundary<'_>, InternalError> {
        validate_executor_plan_for_authority(&plan.authority(), plan.logical_plan())?;
        let store = self.db.recovered_store(plan.authority().store_path())?;
        let store_resolver = self.db.store_resolver();

        // Validate the canonical lowered specs once while retaining the prepared
        // load plan for any later materialized-page fallback.
        let _ = plan.index_prefix_specs()?;
        let _ = plan.index_range_specs()?;
        let authority = plan.authority();

        Ok(PreparedScalarMaterializedBoundary {
            authority,
            store,
            store_resolver,
            plan,
        })
    }

    // Scalar execution spine:
    // 1) resolve typed boundary inputs once
    // 2) build one structural scalar execution stage
    // 3) execute the shared scalar runtime
    // 4) finalize typed page + observability
    pub(in crate::db::executor) fn prepare_scalar_route_runtime(
        &self,
        plan: PreparedLoadPlan,
        continuation: ScalarContinuationContext,
        unpaged_rows_mode: bool,
    ) -> Result<PreparedScalarRouteRuntime, InternalError> {
        let prepared = plan.into_scalar_runtime_handoff(
            ScalarProjectionRuntimeMode::SharedValidation,
            CursorEmissionMode::Emit,
        )?;

        prepare_scalar_route_runtime_from_inputs(
            &self.db,
            self.debug,
            prepared.authority,
            prepared.execution_preparation,
            prepared.prepared_projection_contract,
            prepared.retained_slot_layout,
            prepared.plan_core,
            ScalarPreparedRuntimeOptions {
                continuation,
                unpaged_rows_mode,
                cursor_emission: CursorEmissionMode::Emit,
                projection_runtime_mode: ScalarProjectionRuntimeMode::SharedValidation,
                route_plan_family: ScalarRoutePlanFamily::Resumed,
                prebuilt_route_plan: None,
                suppress_route_scan_hints: false,
                plan_validation: ScalarPlanValidationMode::Required,
            },
        )
    }

    // Materialize one scalar page structurally from one already-prepared
    // aggregate/load stage without forcing typed entity reconstruction.
    pub(in crate::db::executor) fn execute_scalar_materialized_page_stage(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
    ) -> Result<StructuralCursorPage, InternalError> {
        let plan = PreparedLoadPlan::from_valid_shared_residents(
            prepared.authority,
            prepared.logical_plan,
            prepared.schema_fingerprint,
            prepared.index_prefix_specs,
            prepared.index_range_specs,
        );

        execute_scalar_materialized_rows_boundary(self, plan)
    }

    // Materialize one scalar page structurally from the neutral non-aggregate
    // prepared boundary without forcing typed entity response assembly.
    pub(in crate::db::executor) fn execute_scalar_materialized_page_boundary(
        &self,
        prepared: PreparedScalarMaterializedBoundary<'_>,
    ) -> Result<StructuralCursorPage, InternalError> {
        execute_scalar_materialized_rows_boundary(self, prepared.plan)
    }
}
