use crate::{
    db::{
        cursor::CursorBoundary,
        executor::{
            OrderedKeyStream, ScalarContinuationContext,
            pipeline::contracts::{CursorEmissionMode, MaterializedExecutionPayload, PageCursor},
            projection::PreparedSlotProjectionValidation,
            route::{LoadOrderRouteContract, access_order_satisfied_by_route_contract},
            terminal::page::{
                KernelRow, KernelRowPayloadMode, ResidualPredicateScanMode, RetainedSlotLayout,
                ScalarMaterializationCapabilities, ScalarRowRuntimeHandle,
                post_scan::{
                    FinalPayloadStrategy, StructuralPostScanPageWindowStrategy,
                    StructuralPostScanTailStrategy, required_prepared_projection_validation,
                },
                resolved_order_required,
                scan::{KernelRowScanRequest, ScalarPageKernelRequest},
            },
            window::compute_page_keep_count,
        },
        predicate::MissingRowPolicy,
        query::plan::{AccessPlannedQuery, EffectiveRuntimeFilterProgram, ResolvedOrder},
    },
    error::InternalError,
};

///
/// ScalarMaterializationPlan
///
/// ScalarMaterializationPlan freezes the scalar page policy resolved from one
/// raw capability bundle.
/// The terminal runtime executes this plan directly so payload mode,
/// residual timing, and direct-lane eligibility are decided once up front.
///

pub(super) struct ScalarMaterializationPlan<'a> {
    direct_data_row_path: Option<DirectDataRowPath<'a>>,
    kernel_row_scan_strategy: KernelRowScanStrategy<'a>,
    post_access_strategy: PostAccessStrategy<'a>,
    post_scan_tail: StructuralPostScanTailStrategy<'a>,
    cursor_emission: CursorEmissionMode,
}

impl<'a> ScalarMaterializationPlan<'a> {
    // Return the optional direct raw-row fast path already selected for this
    // scalar materialization plan.
    pub(super) const fn direct_data_row_path(&self) -> Option<DirectDataRowPath<'a>> {
        self.direct_data_row_path
    }

    // Build the shared scalar page-kernel request from one already-resolved
    // materialization plan so the terminal runtime does not re-read raw
    // capabilities after policy resolution.
    pub(super) fn kernel_request<'r>(
        &self,
        key_stream: &'a mut dyn OrderedKeyStream,
        scan_budget_hint: Option<usize>,
        load_order_route_contract: LoadOrderRouteContract,
        consistency: MissingRowPolicy,
        continuation: &'a ScalarContinuationContext,
        row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
    ) -> ScalarPageKernelRequest<'a, 'r> {
        ScalarPageKernelRequest {
            key_stream,
            scan_budget_hint,
            load_order_route_contract,
            consistency,
            scan_strategy: self.kernel_row_scan_strategy,
            continuation,
            row_runtime,
        }
    }

    // Return the post-access strategy already frozen into this plan.
    pub(super) const fn post_access_strategy(&self) -> PostAccessStrategy<'a> {
        self.post_access_strategy
    }

    // Return the outward cursor-emission mode already frozen into this plan.
    pub(super) const fn cursor_emission(&self) -> CursorEmissionMode {
        self.cursor_emission
    }

    // Apply the remaining shared post-scan tail before cursor derivation and
    // final payload shaping.
    pub(super) fn apply_post_scan_tail(
        &self,
        plan: &AccessPlannedQuery,
        rows: &mut Vec<KernelRow>,
    ) -> Result<(), InternalError> {
        self.post_scan_tail.apply(plan, rows)
    }

    // Finalize the structural payload through the already-resolved tail
    // strategy instead of re-reading payload family state in the terminal.
    pub(super) fn finalize_payload(
        &self,
        rows: Vec<KernelRow>,
        next_cursor: Option<PageCursor>,
    ) -> Result<MaterializedExecutionPayload, InternalError> {
        self.post_scan_tail.finalize_payload(rows, next_cursor)
    }
}

///
/// CursorlessShortPathPlan
///
/// CursorlessShortPathPlan freezes the cursorless structural short-path policy
/// under the same scalar materialization boundary as the main page path.
/// It owns kernel scan choice, row keep-cap behavior, projection validation,
/// and final payload family so the row collector consumes one resolved plan.
///

pub(in crate::db::executor) struct CursorlessShortPathPlan<'a> {
    scan_strategy: KernelRowScanStrategy<'a>,
    row_keep_cap: Option<usize>,
    post_scan_tail: StructuralPostScanTailStrategy<'a>,
}

impl<'a> CursorlessShortPathPlan<'a> {
    // Build the canonical structural scan request for this cursorless short
    // path so row-collector execution does not rebuild the same request
    // envelope from separate keep-cap and scan-strategy fields.
    pub(in crate::db::executor) fn scan_request<'r>(
        &self,
        key_stream: &'a mut dyn OrderedKeyStream,
        scan_budget_hint: Option<usize>,
        consistency: MissingRowPolicy,
        row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
    ) -> KernelRowScanRequest<'a, 'r> {
        KernelRowScanRequest {
            key_stream,
            scan_budget_hint,
            consistency,
            scan_strategy: self.scan_strategy,
            row_keep_cap: self.row_keep_cap,
            row_runtime,
        }
    }

    // Materialize one already-scanned cursorless short-path row set through
    // the shared post-access tail and outward payload family.
    pub(in crate::db::executor) fn materialize_rows(
        &self,
        plan: &AccessPlannedQuery,
        mut rows: Vec<KernelRow>,
    ) -> Result<(MaterializedExecutionPayload, usize), InternalError> {
        self.post_scan_tail.apply(plan, &mut rows)?;

        let post_access_rows = rows.len();
        let payload = self.post_scan_tail.finalize_payload(rows, None)?;

        Ok((payload, post_access_rows))
    }
}

///
/// ResolvedScalarStructuralPolicy
///
/// ResolvedScalarStructuralPolicy captures the scalar structural execution
/// policy shared by the main scalar page path and the cursorless short path.
/// It freezes residual predicate timing, kernel scan choice, projection
/// validation ownership, and final payload family once from one capability
/// bundle so sibling materialization plans do not each reinterpret them.
///

struct ResolvedScalarStructuralPolicy<'a> {
    residual_predicate_scan_mode: ResidualPredicateScanMode,
    kernel_row_scan_strategy: KernelRowScanStrategy<'a>,
    projection_validation: Option<&'a PreparedSlotProjectionValidation>,
    final_payload_strategy: FinalPayloadStrategy,
}

impl<'a> ResolvedScalarStructuralPolicy<'a> {
    // Return the residual predicate timing already frozen into this shared
    // scalar structural policy.
    const fn residual_predicate_scan_mode(&self) -> ResidualPredicateScanMode {
        self.residual_predicate_scan_mode
    }

    // Return the structural scan strategy already selected for this policy.
    const fn kernel_row_scan_strategy(&self) -> KernelRowScanStrategy<'a> {
        self.kernel_row_scan_strategy
    }

    // Build one shared structural post-scan tail from the already-resolved
    // projection validation and final payload family.
    const fn post_scan_tail(
        &self,
        page_window_strategy: StructuralPostScanPageWindowStrategy,
    ) -> StructuralPostScanTailStrategy<'a> {
        StructuralPostScanTailStrategy::new(
            page_window_strategy,
            self.projection_validation,
            self.final_payload_strategy,
        )
    }
}

// Resolve the scalar page execution plan once from one raw capability bundle
// so later phases consume one stable policy object instead of branching on
// retained-layout presence repeatedly.
pub(super) fn resolve_scalar_materialization_plan<'a>(
    plan: &'a AccessPlannedQuery,
    capabilities: ScalarMaterializationCapabilities<'a>,
) -> Result<ScalarMaterializationPlan<'a>, InternalError> {
    let structural_policy = resolve_scalar_structural_policy(
        plan,
        capabilities,
        select_kernel_row_payload_mode(
            capabilities.retain_slot_rows,
            capabilities.cursor_emission,
            capabilities.retained_slot_layout,
        ),
    )?;
    let direct_data_row_path = resolve_direct_data_row_path(
        plan,
        capabilities.validate_projection,
        capabilities.retain_slot_rows,
        capabilities.retained_slot_layout,
        capabilities.residual_filter_program,
        capabilities.cursor_emission,
        structural_policy.residual_predicate_scan_mode(),
    )?;
    let post_access_strategy = resolve_post_access_strategy(
        plan,
        capabilities.residual_filter_program,
        structural_policy.residual_predicate_scan_mode(),
        capabilities.cursor_emission,
        capabilities.retain_slot_rows,
    )?;

    Ok(ScalarMaterializationPlan {
        direct_data_row_path,
        kernel_row_scan_strategy: structural_policy.kernel_row_scan_strategy(),
        post_access_strategy,
        post_scan_tail: structural_policy
            .post_scan_tail(StructuralPostScanPageWindowStrategy::NotPresent),
        cursor_emission: capabilities.cursor_emission,
    })
}

// Resolve the cursorless structural short-path contract under the same scalar
// materialization boundary as the main page path, so row-collector execution
// consumes one resolved plan instead of reinterpreting raw capabilities.
pub(in crate::db::executor) fn resolve_cursorless_short_path_plan<'a>(
    plan: &'a AccessPlannedQuery,
    cursor_boundary: Option<&CursorBoundary>,
    capabilities: ScalarMaterializationCapabilities<'a>,
) -> Result<Option<CursorlessShortPathPlan<'a>>, InternalError> {
    let logical = plan.scalar_plan();
    let generic_short_path = logical.mode.is_load()
        && cursor_boundary.is_none()
        && logical.predicate.is_none()
        && logical.order.is_none()
        && logical.page.is_none();
    let retained_slot_short_path = logical.mode.is_load()
        && capabilities.retain_slot_rows
        && cursor_boundary.is_none()
        && !logical.distinct
        && (logical.order.is_none() || access_order_satisfied_by_route_contract(plan));
    if !(generic_short_path || retained_slot_short_path) {
        return Ok(None);
    }

    let structural_policy = resolve_scalar_structural_policy(
        plan,
        capabilities,
        select_cursorless_short_path_payload_mode(
            capabilities.retain_slot_rows,
            cursor_boundary,
            capabilities.retained_slot_layout,
        ),
    )?;

    Ok(Some(CursorlessShortPathPlan {
        scan_strategy: structural_policy.kernel_row_scan_strategy(),
        row_keep_cap: cursorless_short_path_keep_cap(
            plan,
            cursor_boundary,
            capabilities.retain_slot_rows,
        ),
        post_scan_tail: structural_policy
            .post_scan_tail(StructuralPostScanPageWindowStrategy::CursorlessRetainedWindow),
    }))
}

// Resolve the scalar structural execution policy shared by both the main
// scalar page path and the cursorless short path.
fn resolve_scalar_structural_policy<'a>(
    plan: &AccessPlannedQuery,
    capabilities: ScalarMaterializationCapabilities<'a>,
    kernel_payload_mode: KernelRowPayloadMode,
) -> Result<ResolvedScalarStructuralPolicy<'a>, InternalError> {
    let residual_predicate_scan_mode = ResidualPredicateScanMode::from_plan_and_layout(
        plan.has_residual_filter(),
        capabilities.retained_slot_layout,
        capabilities.residual_filter_program,
    );
    let kernel_row_scan_strategy = resolve_kernel_row_scan_strategy(
        kernel_payload_mode,
        capabilities.residual_filter_program,
        residual_predicate_scan_mode,
        capabilities.retained_slot_layout,
    )?;

    Ok(ResolvedScalarStructuralPolicy {
        residual_predicate_scan_mode,
        kernel_row_scan_strategy,
        projection_validation: if capabilities.validate_projection {
            Some(required_prepared_projection_validation(
                capabilities.prepared_projection_validation,
            )?)
        } else {
            None
        },
        final_payload_strategy: FinalPayloadStrategy::from_retain_slot_rows(
            capabilities.retain_slot_rows,
        ),
    })
}

///
/// DirectDataRowPath
///
/// DirectDataRowPath captures one executor-owned raw `DataRow` fast path.
/// It lets scalar materialization choose one direct-lane strategy once, then
/// run one shared execution shell instead of scattering residual-timing and
/// retained-layout checks across sibling branches.
///

#[derive(Clone, Copy)]
pub(super) enum DirectDataRowPath<'a> {
    Plain {
        row_keep_cap: Option<usize>,
    },
    Filtered {
        row_keep_cap: Option<usize>,
        filter_program: &'a EffectiveRuntimeFilterProgram,
        retained_slot_layout: &'a RetainedSlotLayout,
    },
    MaterializedOrder {
        residual_predicate_scan_mode: ResidualPredicateScanMode,
        resolved_order: &'a ResolvedOrder,
        filter_program: Option<&'a EffectiveRuntimeFilterProgram>,
        retained_slot_layout: Option<&'a RetainedSlotLayout>,
    },
}

///
/// KernelRowScanStrategy
///
/// KernelRowScanStrategy is the resolved structural scan strategy for the
/// non-direct scalar page lane.
/// It removes the raw `(payload_mode, residual_predicate_scan_mode)` pairing
/// from the hot execution loop by freezing one concrete retained/data-row scan
/// contract up front.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum KernelRowScanStrategy<'a> {
    DataRows,
    RetainedFullRows {
        retained_slot_layout: &'a RetainedSlotLayout,
    },
    RetainedFullRowsFiltered {
        filter_program: &'a EffectiveRuntimeFilterProgram,
        retained_slot_layout: &'a RetainedSlotLayout,
    },
    SlotOnlyRows {
        retained_slot_layout: &'a RetainedSlotLayout,
    },
    SlotOnlyRowsFiltered {
        filter_program: &'a EffectiveRuntimeFilterProgram,
        retained_slot_layout: &'a RetainedSlotLayout,
    },
}

///
/// PostAccessPredicateStrategy
///
/// PostAccessPredicateStrategy captures whether residual filter evaluation is
/// absent, already handled during scan, or still deferred to the
/// post-access kernel-row phase.
///

#[derive(Clone, Copy)]
pub(super) enum PostAccessPredicateStrategy<'a> {
    NotPresent,
    AppliedDuringScan,
    Deferred {
        filter_program: &'a EffectiveRuntimeFilterProgram,
    },
}

impl PostAccessPredicateStrategy<'_> {
    // Return whether post-access still owns residual filter evaluation.
    pub(super) const fn requires_post_access_filtering(self) -> bool {
        matches!(self, Self::Deferred { .. })
    }
}

///
/// PostAccessStrategy
///
/// PostAccessStrategy freezes the remaining post-scan policy for scalar
/// kernel rows.
/// It owns residual predicate handling plus distinct-window deferral so the
/// post-access executor does not interpret raw mode flags directly.
///

#[derive(Clone, Copy)]
pub(super) struct PostAccessStrategy<'a> {
    pub(super) predicate_strategy: PostAccessPredicateStrategy<'a>,
    pub(super) defer_retained_slot_distinct_window: bool,
}

// Resolve whether the scalar materializer can stay entirely on the direct
// `DataRow` lane and, if so, which direct-lane strategy owns the scan.
fn resolve_direct_data_row_path<'a>(
    plan: &'a AccessPlannedQuery,
    validate_projection: bool,
    retain_slot_rows: bool,
    retained_slot_layout: Option<&'a RetainedSlotLayout>,
    residual_filter_program: Option<&'a EffectiveRuntimeFilterProgram>,
    cursor_emission: CursorEmissionMode,
    residual_predicate_scan_mode: ResidualPredicateScanMode,
) -> Result<Option<DirectDataRowPath<'a>>, InternalError> {
    let logical = plan.scalar_plan();

    // Phase 1: direct raw-row lanes are only valid for cursorless load paths
    // that do not need projection validation or retained-slot surfaces.
    let direct_load_surface_eligible = logical.mode.is_load()
        && !logical.distinct
        && !validate_projection
        && !retain_slot_rows
        && !cursor_emission.enabled();
    if !direct_load_surface_eligible {
        return Ok(None);
    }

    // Phase 2: route-ordered paths can stay direct only when scan-time
    // residual timing and retained-layout availability already match.
    if access_order_satisfied_by_route_contract(plan) {
        return Ok(match residual_predicate_scan_mode {
            ResidualPredicateScanMode::Absent if retained_slot_layout.is_none() => {
                Some(DirectDataRowPath::Plain {
                    row_keep_cap: direct_data_row_keep_cap(plan),
                })
            }
            ResidualPredicateScanMode::AppliedDuringScan => {
                residual_filter_program.zip(retained_slot_layout).map(
                    |(filter_program, retained_slot_layout)| DirectDataRowPath::Filtered {
                        row_keep_cap: direct_data_row_keep_cap(plan),
                        filter_program,
                        retained_slot_layout,
                    },
                )
            }
            ResidualPredicateScanMode::Absent | ResidualPredicateScanMode::DeferredPostAccess => {
                None
            }
        });
    }

    // Phase 3: non-route-ordered direct lanes are only valid when an
    // in-memory order window can run on raw data rows after scan-time
    // residual filtering has already been settled.
    let materialized_order_direct_eligible = logical
        .order
        .as_ref()
        .is_some_and(|order| !order.fields.is_empty())
        && retained_slot_layout.is_some()
        && (matches!(
            residual_predicate_scan_mode,
            ResidualPredicateScanMode::Absent
        ) || (matches!(
            residual_predicate_scan_mode,
            ResidualPredicateScanMode::AppliedDuringScan
        ) && residual_filter_program.is_some()));
    if !materialized_order_direct_eligible {
        return Ok(None);
    }

    Ok(Some(DirectDataRowPath::MaterializedOrder {
        residual_predicate_scan_mode,
        resolved_order: resolved_order_required(plan)?,
        filter_program: residual_filter_program,
        retained_slot_layout,
    }))
}

// Resolve one concrete kernel-row scan strategy from the payload mode and
// residual timing already selected for the scalar materialization plan.
fn resolve_kernel_row_scan_strategy<'a>(
    payload_mode: KernelRowPayloadMode,
    residual_filter_program: Option<&'a EffectiveRuntimeFilterProgram>,
    residual_predicate_scan_mode: ResidualPredicateScanMode,
    retained_slot_layout: Option<&'a RetainedSlotLayout>,
) -> Result<KernelRowScanStrategy<'a>, InternalError> {
    match (payload_mode, residual_predicate_scan_mode) {
        (
            KernelRowPayloadMode::DataRowOnly,
            ResidualPredicateScanMode::Absent | ResidualPredicateScanMode::DeferredPostAccess,
        ) => Ok(KernelRowScanStrategy::DataRows),
        (KernelRowPayloadMode::DataRowOnly, ResidualPredicateScanMode::AppliedDuringScan) => {
            Err(InternalError::query_executor_invariant(
                "data-row-only kernel rows must not apply residual predicates during scan",
            ))
        }
        (KernelRowPayloadMode::FullRowRetained, ResidualPredicateScanMode::Absent) => {
            Ok(KernelRowScanStrategy::RetainedFullRows {
                retained_slot_layout: retained_slot_layout.ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "retained full-row kernel rows require one retained-slot layout",
                    )
                })?,
            })
        }
        (KernelRowPayloadMode::FullRowRetained, ResidualPredicateScanMode::AppliedDuringScan) => {
            Ok(KernelRowScanStrategy::RetainedFullRowsFiltered {
                filter_program: residual_filter_program.ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "retained full-row kernel rows require one residual filter program",
                    )
                })?,
                retained_slot_layout: retained_slot_layout.ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "retained full-row kernel rows require one retained-slot layout",
                    )
                })?,
            })
        }
        (KernelRowPayloadMode::FullRowRetained, ResidualPredicateScanMode::DeferredPostAccess) => {
            Err(InternalError::query_executor_invariant(
                "retained full-row kernel rows must apply residual predicates during scan",
            ))
        }
        (KernelRowPayloadMode::SlotsOnly, ResidualPredicateScanMode::Absent) => {
            Ok(KernelRowScanStrategy::SlotOnlyRows {
                retained_slot_layout: retained_slot_layout.ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "slot-only kernel rows require one retained-slot layout",
                    )
                })?,
            })
        }
        (KernelRowPayloadMode::SlotsOnly, ResidualPredicateScanMode::AppliedDuringScan) => {
            Ok(KernelRowScanStrategy::SlotOnlyRowsFiltered {
                filter_program: residual_filter_program.ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "slot-only kernel rows require one residual filter program",
                    )
                })?,
                retained_slot_layout: retained_slot_layout.ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "slot-only kernel rows require one retained-slot layout",
                    )
                })?,
            })
        }
        (KernelRowPayloadMode::SlotsOnly, ResidualPredicateScanMode::DeferredPostAccess) => {
            Err(InternalError::query_executor_invariant(
                "slot-only kernel rows must apply residual predicates during scan",
            ))
        }
    }
}

// Resolve the scalar post-access execution contract once from the residual
// predicate timing and the distinct-window shape already chosen for this plan.
fn resolve_post_access_strategy<'a>(
    plan: &AccessPlannedQuery,
    residual_filter_program: Option<&'a EffectiveRuntimeFilterProgram>,
    residual_predicate_scan_mode: ResidualPredicateScanMode,
    cursor_emission: CursorEmissionMode,
    retain_slot_rows: bool,
) -> Result<PostAccessStrategy<'a>, InternalError> {
    let predicate_strategy = match residual_predicate_scan_mode {
        ResidualPredicateScanMode::Absent => PostAccessPredicateStrategy::NotPresent,
        ResidualPredicateScanMode::AppliedDuringScan => {
            PostAccessPredicateStrategy::AppliedDuringScan
        }
        ResidualPredicateScanMode::DeferredPostAccess => PostAccessPredicateStrategy::Deferred {
            filter_program: residual_filter_program.ok_or_else(|| {
                InternalError::query_executor_invariant(
                    "deferred post-access filtering requires one residual filter program",
                )
            })?,
        },
    };

    Ok(PostAccessStrategy {
        predicate_strategy,
        defer_retained_slot_distinct_window: plan.scalar_plan().distinct
            && !cursor_emission.enabled()
            && retain_slot_rows,
    })
}

// Select one kernel payload mode before scanning so the row loop does not
// branch on retained/data-row shape per key.
const fn select_kernel_row_payload_mode(
    retain_slot_rows: bool,
    cursor_emission: CursorEmissionMode,
    retained_slot_layout: Option<&RetainedSlotLayout>,
) -> KernelRowPayloadMode {
    select_scalar_structural_payload_mode(
        retain_slot_rows,
        !cursor_emission.enabled(),
        retained_slot_layout,
    )
}

// Select one structural payload mode from the already-resolved slot-retention
// and cursor-suppression capabilities shared by scalar page and short-path
// materialization.
const fn select_scalar_structural_payload_mode(
    retain_slot_rows: bool,
    suppress_cursor: bool,
    retained_slot_layout: Option<&RetainedSlotLayout>,
) -> KernelRowPayloadMode {
    if retain_slot_rows && suppress_cursor {
        KernelRowPayloadMode::SlotsOnly
    } else if retained_slot_layout.is_some() {
        KernelRowPayloadMode::FullRowRetained
    } else {
        KernelRowPayloadMode::DataRowOnly
    }
}

// Return the number of kept rows the cursorless retained-slot path must stage
// before later pagination becomes redundant.
fn cursorless_short_path_keep_cap(
    plan: &AccessPlannedQuery,
    cursor_boundary: Option<&CursorBoundary>,
    retain_slot_rows: bool,
) -> Option<usize> {
    if !retain_slot_rows || cursor_boundary.is_some() {
        return None;
    }

    let page = plan.scalar_plan().page.as_ref()?;
    let limit = page.limit?;
    let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);

    Some(offset.saturating_add(limit))
}

// Select one row payload mode before cursorless row collection so the scan
// loop does not branch on data-vs-slot materialization per row.
const fn select_cursorless_short_path_payload_mode(
    retain_slot_rows: bool,
    cursor_boundary: Option<&CursorBoundary>,
    retained_slot_layout: Option<&RetainedSlotLayout>,
) -> KernelRowPayloadMode {
    select_scalar_structural_payload_mode(
        retain_slot_rows,
        cursor_boundary.is_none(),
        retained_slot_layout,
    )
}

// Return the maximum number of route-ordered direct data rows worth staging
// before the final cursorless page window runs. These lanes never need one
// continuation lookahead row, so `offset + limit` is the real working set.
fn direct_data_row_keep_cap(plan: &AccessPlannedQuery) -> Option<usize> {
    let page = plan.scalar_plan().page.as_ref()?;
    let limit = page.limit?;

    Some(compute_page_keep_count(page.offset, limit))
}
