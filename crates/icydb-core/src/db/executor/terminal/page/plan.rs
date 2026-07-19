use crate::{
    db::{
        cursor::CursorBoundary,
        executor::{
            ExecutionKernel, OrderedKeyStreamBox, ScalarContinuationContext,
            pipeline::contracts::{
                CursorEmissionMode, PageCursor, ScalarMaterializationCapabilities,
                StructuralCursorPage,
            },
            projection::PreparedProjectionContract,
            route::{
                LoadOrderRouteMode, access_order_satisfied_by_route_mode,
                branch_set_page_keep_cap_shape_supported,
            },
            terminal::page::{
                KernelRow, KernelRowPayloadMode, RetainedSlotLayout, ScalarRowRuntimeHandle,
                post_scan::{
                    StructuralPostScanPageWindowStrategy, StructuralPostScanTailStrategy,
                    required_prepared_projection_validation,
                },
                scan::{KernelRowScanRequest, ScalarPageKernelRequest},
            },
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
/// residual filtering, and direct-lane eligibility are decided once up front.
///

pub(super) struct ScalarMaterializationPlan<'a> {
    direct_data_row_path: Option<DirectDataRowPath<'a>>,
    kernel_row_scan_strategy: KernelRowScanStrategy<'a>,
    defer_retained_slot_distinct_window: bool,
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
    #[expect(
        clippy::too_many_arguments,
        reason = "kernel request assembly keeps the scalar materialization boundary explicit"
    )]
    pub(super) fn kernel_request<'r>(
        &self,
        plan: &'a AccessPlannedQuery,
        key_stream: &'a mut OrderedKeyStreamBox,
        scan_budget_hint: Option<usize>,
        load_order_route_mode: LoadOrderRouteMode,
        consistency: MissingRowPolicy,
        continuation: &'a ScalarContinuationContext,
        row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
    ) -> Result<ScalarPageKernelRequest<'a, 'r>, InternalError> {
        Ok(ScalarPageKernelRequest {
            key_stream,
            scan_budget_hint,
            row_keep_cap: self.branch_set_page_scan_keep_cap(plan, continuation),
            order_window: self.bounded_materialized_order_scan_window(plan, continuation)?,
            load_order_route_mode,
            consistency,
            scan_strategy: self.kernel_row_scan_strategy,
            continuation,
            row_runtime,
        })
    }

    // Return whether retained-slot DISTINCT pagination remains deferred until
    // the post-scan tail has materialized final rows.
    pub(super) const fn defer_retained_slot_distinct_window(&self) -> bool {
        self.defer_retained_slot_distinct_window
    }

    // Bound branch-set page materialization at the merged lookahead window.
    // The branch route already proves final primary-key order for this slice,
    // so no post-scan ordering or predicate phase can reveal earlier rows.
    fn branch_set_page_scan_keep_cap(
        &self,
        plan: &AccessPlannedQuery,
        continuation: &ScalarContinuationContext,
    ) -> Option<usize> {
        let logical = plan.scalar_plan();
        let branch_set_page = plan
            .access_shape_facts()
            .single_path_facts()
            .as_ref()
            .is_some_and(branch_set_page_keep_cap_shape_supported);
        if !logical.mode.is_load()
            || logical.distinct
            || logical
                .order
                .as_ref()
                .is_none_or(|order| order.fields.is_empty())
            || !access_order_satisfied_by_route_mode(plan)
            || !branch_set_page
            || self.kernel_row_scan_strategy.applies_residual_filter()
            || self.defer_retained_slot_distinct_window
        {
            return None;
        }

        let limit = logical.page.as_ref()?.limit?;
        if limit == 0 {
            return Some(0);
        }

        Some(
            continuation
                .keep_count_for_limit_window(plan, limit)
                .saturating_add(1),
        )
    }

    // Bound unordered retained-row materialization at the same top-K page
    // window later consumed by post-access ordering. This is only valid once
    // residual filtering runs during row scan, before a row can count toward
    // the ordered window.
    fn bounded_materialized_order_scan_window(
        &self,
        plan: &'a AccessPlannedQuery,
        continuation: &ScalarContinuationContext,
    ) -> Result<Option<KernelRowOrderWindow<'a>>, InternalError> {
        let logical = plan.scalar_plan();
        if !logical.mode.is_load()
            || logical.distinct
            || access_order_satisfied_by_route_mode(plan)
            || self.defer_retained_slot_distinct_window
            || !self.kernel_row_scan_strategy.materializes_slots()
        {
            return Ok(None);
        }

        if logical
            .order
            .as_ref()
            .is_none_or(|order| order.fields.is_empty())
        {
            return Ok(None);
        }
        let resolved_order = plan.require_resolved_order()?;

        Ok(
            ExecutionKernel::bounded_order_keep_count(plan, continuation.cursor_boundary()).map(
                |keep_count| KernelRowOrderWindow {
                    resolved_order,
                    keep_count,
                },
            ),
        )
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
    ) -> Result<StructuralCursorPage, InternalError> {
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
    row_skip_count: usize,
    post_scan_tail: StructuralPostScanTailStrategy<'a>,
}

impl<'a> CursorlessShortPathPlan<'a> {
    // Build the canonical structural scan request for this cursorless short
    // path so row-collector execution does not rebuild the same request
    // envelope from separate keep-cap and scan-strategy fields.
    pub(in crate::db::executor) const fn scan_request<'r>(
        &self,
        key_stream: &'a mut OrderedKeyStreamBox,
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
            row_skip_count: self.row_skip_count,
            order_window: None,
            row_runtime,
        }
    }

    // Materialize one already-scanned cursorless short-path row set through
    // the shared post-access tail and outward payload family.
    pub(in crate::db::executor) fn materialize_rows(
        &self,
        plan: &AccessPlannedQuery,
        mut rows: Vec<KernelRow>,
    ) -> Result<(StructuralCursorPage, usize), InternalError> {
        self.post_scan_tail.apply_with_pre_applied_page_window(
            plan,
            &mut rows,
            self.row_skip_count != 0,
        )?;

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
/// It freezes the kernel scan choice, projection
/// validation ownership, and final payload family once from one capability
/// bundle so sibling materialization plans do not each reinterpret them.
///

struct ResolvedScalarStructuralPolicy<'a> {
    kernel_row_scan_strategy: KernelRowScanStrategy<'a>,
    projection_validation: Option<&'a PreparedProjectionContract>,
    retain_slot_rows: bool,
}

impl<'a> ResolvedScalarStructuralPolicy<'a> {
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
            self.retain_slot_rows,
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
    )?;
    let defer_retained_slot_distinct_window = plan.scalar_plan().distinct
        && !capabilities.cursor_emission.enabled()
        && capabilities.retain_slot_rows;

    Ok(ScalarMaterializationPlan {
        direct_data_row_path,
        kernel_row_scan_strategy: structural_policy.kernel_row_scan_strategy(),
        defer_retained_slot_distinct_window,
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
        && (logical.order.is_none() || access_order_satisfied_by_route_mode(plan));
    if !(generic_short_path || retained_slot_short_path) {
        return Ok(None);
    }

    let structural_policy = resolve_scalar_structural_policy(
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
        row_skip_count: cursorless_short_path_skip_count(
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
fn resolve_scalar_structural_policy(
    capabilities: ScalarMaterializationCapabilities<'_>,
    kernel_payload_mode: KernelRowPayloadMode,
) -> Result<ResolvedScalarStructuralPolicy<'_>, InternalError> {
    let kernel_row_scan_strategy = resolve_kernel_row_scan_strategy(
        kernel_payload_mode,
        capabilities.residual_filter_program,
        capabilities.retained_slot_layout,
    )?;

    Ok(ResolvedScalarStructuralPolicy {
        kernel_row_scan_strategy,
        projection_validation: if capabilities.validate_projection {
            Some(required_prepared_projection_validation(
                capabilities.prepared_projection_validation,
            )?)
        } else {
            None
        },
        retain_slot_rows: capabilities.retain_slot_rows,
    })
}

///
/// DirectDataRowPath
///
/// DirectDataRowPath captures one executor-owned raw `DataRow` fast path.
/// It lets scalar materialization choose one direct-lane strategy once, then
/// run one shared execution shell instead of scattering residual-filter and
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
    },
    MaterializedOrder {
        resolved_order: &'a ResolvedOrder,
        filter_program: Option<&'a EffectiveRuntimeFilterProgram>,
    },
}

///
/// KernelRowScanStrategy
///
/// KernelRowScanStrategy is the resolved structural scan strategy for the
/// non-direct scalar page lane.
/// It freezes one concrete filtered or unfiltered retained/data-row contract
/// before the hot execution loop.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum KernelRowScanStrategy<'a> {
    DataRows,
    DataRowsFiltered {
        filter_program: &'a EffectiveRuntimeFilterProgram,
    },
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

impl KernelRowScanStrategy<'_> {
    // Return whether this concrete scan strategy applies the planner-selected
    // residual filter while each raw row is open.
    const fn applies_residual_filter(self) -> bool {
        matches!(
            self,
            Self::DataRowsFiltered { .. }
                | Self::RetainedFullRowsFiltered { .. }
                | Self::SlotOnlyRowsFiltered { .. }
        )
    }

    // Return whether rows emitted by this scan strategy carry decoded slots
    // usable by post-access ordering and cursor comparisons.
    const fn materializes_slots(self) -> bool {
        !matches!(self, Self::DataRows | Self::DataRowsFiltered { .. })
    }
}

///
/// KernelRowOrderWindow
///
/// KernelRowOrderWindow carries the scan-time top-K retained-row order window
/// for direct-field and expression-backed resolved orders.
/// The scan uses it only to reduce the retained working set; post-access
/// ordering still owns final canonical ordering and pagination.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct KernelRowOrderWindow<'a> {
    pub(in crate::db::executor) resolved_order: &'a ResolvedOrder,
    pub(in crate::db::executor) keep_count: usize,
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

    // Phase 2: route-ordered paths can stay direct when no later phase needs
    // retained slots. The direct row reader applies the canonical residual
    // filter program against the opened raw row when one exists.
    if access_order_satisfied_by_route_mode(plan) {
        if retained_slot_layout.is_some() {
            return Ok(None);
        }
        return Ok(Some(match residual_filter_program {
            Some(filter_program) => DirectDataRowPath::Filtered {
                row_keep_cap: plan.direct_data_row_keep_cap(),
                filter_program,
            },
            None => DirectDataRowPath::Plain {
                row_keep_cap: plan.direct_data_row_keep_cap(),
            },
        }));
    }

    // Phase 3: non-route-ordered direct lanes are only valid when an
    // in-memory order window can run on raw data rows after scan-time
    // residual filtering has already been settled.
    let materialized_order_direct_eligible = logical
        .order
        .as_ref()
        .is_some_and(|order| !order.fields.is_empty());
    if !materialized_order_direct_eligible {
        return Ok(None);
    }

    Ok(Some(DirectDataRowPath::MaterializedOrder {
        resolved_order: plan.require_resolved_order()?,
        filter_program: residual_filter_program,
    }))
}

// Resolve one concrete kernel-row scan strategy from the payload mode and
// optional residual program already selected for scalar materialization.
fn resolve_kernel_row_scan_strategy<'a>(
    payload_mode: KernelRowPayloadMode,
    residual_filter_program: Option<&'a EffectiveRuntimeFilterProgram>,
    retained_slot_layout: Option<&'a RetainedSlotLayout>,
) -> Result<KernelRowScanStrategy<'a>, InternalError> {
    match (payload_mode, residual_filter_program) {
        (KernelRowPayloadMode::DataRowOnly, None) => Ok(KernelRowScanStrategy::DataRows),
        (KernelRowPayloadMode::DataRowOnly, Some(filter_program)) => {
            Ok(KernelRowScanStrategy::DataRowsFiltered { filter_program })
        }
        (KernelRowPayloadMode::FullRowRetained, None) => {
            Ok(KernelRowScanStrategy::RetainedFullRows {
                retained_slot_layout: retained_slot_layout
                    .ok_or_else(InternalError::query_executor_invariant)?,
            })
        }
        (KernelRowPayloadMode::FullRowRetained, Some(filter_program)) => {
            Ok(KernelRowScanStrategy::RetainedFullRowsFiltered {
                filter_program,
                retained_slot_layout: retained_slot_layout
                    .ok_or_else(InternalError::query_executor_invariant)?,
            })
        }
        (KernelRowPayloadMode::SlotsOnly, None) => Ok(KernelRowScanStrategy::SlotOnlyRows {
            retained_slot_layout: retained_slot_layout
                .ok_or_else(InternalError::query_executor_invariant)?,
        }),
        (KernelRowPayloadMode::SlotsOnly, Some(filter_program)) => {
            Ok(KernelRowScanStrategy::SlotOnlyRowsFiltered {
                filter_program,
                retained_slot_layout: retained_slot_layout
                    .ok_or_else(InternalError::query_executor_invariant)?,
            })
        }
    }
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

// Return the number of matched retained-slot rows the cursorless short path can
// skip during collection because the route already satisfies final ordering.
fn cursorless_short_path_skip_count(
    plan: &AccessPlannedQuery,
    cursor_boundary: Option<&CursorBoundary>,
    retain_slot_rows: bool,
) -> usize {
    if !retain_slot_rows || cursor_boundary.is_some() {
        return 0;
    }

    plan.scalar_plan()
        .page
        .as_ref()
        .map_or(0, |page| usize::try_from(page.offset).unwrap_or(usize::MAX))
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
