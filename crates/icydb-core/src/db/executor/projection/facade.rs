//! Module: executor::projection::facade
//! Responsibility: executor-owned structural projection execution coordination.
//! Does not own: SQL DTO shaping, projection label policy, or diagnostic counter storage.
//! Boundary: accepts prepared projection intent and returns structural projected rows.

use super::covering::try_execute_ordered_distinct_group_seek_for_canister;
use crate::{
    db::{
        Db,
        executor::{
            CoveringProjectionMetricsRecorder, ExecutionPreparation, PageWorkEnvelope,
            ProductionScalarOutputWork, ProjectionMaterializationMetricsRecorder,
            SharedPreparedExecutionPlan, SharedPreparedProjectionRuntimeHandoff,
            StructuralCursorPage,
            budget::{
                charge_runtime_value_rows, prepared_read_execution_context,
                with_read_execution_budget,
            },
            pipeline::execute_initial_scalar_retained_slot_page_from_runtime_handoff_for_canister,
            pipeline::execute_resumed_scalar_retained_slot_page_from_runtime_handoff_for_canister,
            planning::preparation::slot_map_for_model_plan,
            projection::{
                DistinctProjectionRuntime, MaterializedProjectionRows,
                PreparedCoveringProjectionRuntime, ProjectionDistinctStrategy,
                ProjectionDistinctWindow, project, project_admitted_page, project_distinct,
                projection_distinct_strategy,
                try_execute_prepared_covering_projection_rows_for_canister,
            },
            terminal::RetainedSlotRow,
            with_production_scalar_page_work,
        },
        index::predicate::IndexPredicateExecution,
    },
    error::InternalError,
    traits::CanisterKind,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource, DiagnosticExecutionLane, DiagnosticFactTag,
    SqlWriteBoundaryCode,
};

/// Enforced scanned-key ceiling for one structural projection execution.
#[derive(Clone, Copy)]
pub(in crate::db) struct StructuralProjectionScanBudget {
    max_scanned_keys: usize,
    probe_limit: usize,
}

impl StructuralProjectionScanBudget {
    /// Build one positive budget with a checked cap-plus-one overflow probe.
    #[must_use]
    pub(in crate::db) const fn try_new(max_scanned_keys: usize) -> Option<Self> {
        if max_scanned_keys == 0 {
            return None;
        }
        let Some(probe_limit) = max_scanned_keys.checked_add(1) else {
            return None;
        };

        Some(Self {
            max_scanned_keys,
            probe_limit,
        })
    }

    const fn exceeded_by(self, scanned_keys: usize) -> bool {
        scanned_keys > self.max_scanned_keys
    }

    const fn probe_limit(self) -> usize {
        self.probe_limit
    }

    const fn max_scanned_keys(self) -> usize {
        self.max_scanned_keys
    }
}

///
/// StructuralProjectionRequest
///
/// StructuralProjectionRequest carries the generic-free projection execution
/// intent needed after admission/lowering has produced one shared prepared plan.
/// Adapter layers may attach diagnostic callbacks, but executor owns all path
/// selection and row materialization decisions after this boundary.
///

pub(in crate::db) struct StructuralProjectionRequest {
    debug: bool,
    prepared_plan: SharedPreparedExecutionPlan,
    covering_metrics: CoveringProjectionMetricsRecorder,
    materialization_metrics: ProjectionMaterializationMetricsRecorder,
    scan_budget: Option<StructuralProjectionScanBudget>,
    execution_lane: DiagnosticExecutionLane,
    continuation: crate::db::executor::ScalarContinuationContext,
    cursor_page_row_limit: Option<usize>,
    page_work_envelope: Option<PageWorkEnvelope>,
    distinct_output_offset: usize,
}

impl StructuralProjectionRequest {
    /// Build one structural projection request from adapter-provided runtime
    /// switches and diagnostic callback bundles.
    pub(in crate::db) const fn new(
        debug: bool,
        prepared_plan: SharedPreparedExecutionPlan,
        covering_metrics: CoveringProjectionMetricsRecorder,
        materialization_metrics: ProjectionMaterializationMetricsRecorder,
        execution_lane: DiagnosticExecutionLane,
    ) -> Self {
        Self {
            debug,
            prepared_plan,
            covering_metrics,
            materialization_metrics,
            scan_budget: None,
            execution_lane,
            continuation: crate::db::executor::ScalarContinuationContext::initial(),
            cursor_page_row_limit: None,
            page_work_envelope: None,
            distinct_output_offset: 0,
        }
    }

    /// Attach one fail-closed scanned-key ceiling to this execution.
    #[must_use]
    pub(in crate::db) const fn with_scan_budget(
        mut self,
        scan_budget: StructuralProjectionScanBudget,
    ) -> Self {
        self.scan_budget = Some(scan_budget);
        self
    }

    /// Attach one authenticated scalar continuation boundary.
    #[must_use]
    pub(in crate::db) fn with_continuation(
        mut self,
        continuation: crate::db::executor::ScalarContinuationContext,
    ) -> Self {
        self.continuation = continuation;
        self
    }

    /// Retain canonical order inputs needed to emit one authenticated cursor.
    #[must_use]
    pub(in crate::db) const fn with_cursor_emission(mut self, page_row_limit: usize) -> Self {
        self.cursor_page_row_limit = Some(page_row_limit);
        self
    }

    /// Attach the finite work envelope governing one cursor-emitting page.
    #[must_use]
    pub(in crate::db) const fn with_page_work_envelope(
        mut self,
        envelope: PageWorkEnvelope,
    ) -> Self {
        self.page_work_envelope = Some(envelope);
        self
    }

    /// Attach the number of DISTINCT output rows already emitted by an
    /// authenticated continuation. Global DISTINCT replays from the beginning
    /// and skips exactly this many completed output rows.
    #[must_use]
    pub(in crate::db) const fn with_distinct_output_offset(mut self, offset: usize) -> Self {
        self.distinct_output_offset = offset;
        self
    }
}

/// Materialized projection page plus canonical cursor progress.
pub(in crate::db) struct StructuralProjectionPage {
    pub(in crate::db) rows: MaterializedProjectionRows,
    pub(in crate::db) scanned_keys: usize,
    pub(in crate::db) last_emitted_logical: Option<crate::db::cursor::CursorBoundary>,
    pub(in crate::db) last_consumed_physical: Option<Vec<u8>>,
    pub(in crate::db) has_more: bool,
}

/// Execute one prepared structural projection request through the executor-owned
/// projection coordinator.
pub(in crate::db) fn execute_structural_projection_rows<C>(
    db: &Db<C>,
    request: StructuralProjectionRequest,
) -> Result<MaterializedProjectionRows, InternalError>
where
    C: CanisterKind,
{
    let context = prepared_read_execution_context(&request.prepared_plan, request.execution_lane);
    with_read_execution_budget(db.request_execution_scope(), context, || {
        execute_structural_projection_rows_inner(db, request).map(|page| page.rows)
    })
}

/// Execute one bounded scalar projection page with canonical cursor progress.
pub(in crate::db) fn execute_structural_projection_page<C>(
    db: &Db<C>,
    request: StructuralProjectionRequest,
) -> Result<StructuralProjectionPage, InternalError>
where
    C: CanisterKind,
{
    let context = prepared_read_execution_context(&request.prepared_plan, request.execution_lane);
    with_read_execution_budget(db.request_execution_scope(), context, || {
        execute_structural_projection_rows_inner(db, request)
    })
}

#[expect(
    clippy::too_many_lines,
    reason = "one coordinator keeps covering, retained-slot, cursor-boundary, and projection ownership explicit"
)]
fn execute_structural_projection_rows_inner<C>(
    db: &Db<C>,
    request: StructuralProjectionRequest,
) -> Result<StructuralProjectionPage, InternalError>
where
    C: CanisterKind,
{
    let StructuralProjectionRequest {
        debug,
        prepared_plan,
        covering_metrics,
        materialization_metrics,
        scan_budget,
        execution_lane: _,
        continuation,
        cursor_page_row_limit,
        page_work_envelope,
        distinct_output_offset,
    } = request;
    let emit_cursor = cursor_page_row_limit.is_some();
    let distinct = prepared_plan.logical_plan().scalar_plan().distinct;

    // Negative preflight may decline before store access, but execution never
    // reconstructs positive eligibility; only the prepared contract admits.
    let group_seek = if distinct
        && scan_budget.is_none()
        && !continuation.has_progress()
        && !emit_cursor
        && page_work_envelope.is_none()
        && distinct_output_offset == 0
    {
        let covering = prepared_plan.projection_covering_read_execution_plan();
        if let Some(covering) = covering {
            try_execute_ordered_distinct_group_seek_for_canister(
                db,
                prepared_plan.authority(),
                prepared_plan.index_range_specs(),
                &covering,
            )?
            .map(|representatives| (representatives, covering))
        } else {
            None
        }
    } else {
        None
    };

    // Phase 1: choose the covering projection lane only for non-DISTINCT
    // requests. DISTINCT must see final projected rows in scalar execution order
    // before executor-owned deduplication and windowing.
    if !distinct && scan_budget.is_none() && !continuation.has_progress() && !emit_cursor {
        let covering = prepared_plan.projection_covering_read_execution_plan();
        let index_prefix_specs = prepared_plan.index_prefix_specs();
        let index_range_specs = prepared_plan.index_range_specs();
        let covering_execution_preparation = prepared_plan
            .logical_plan()
            .has_residual_filter_predicate()
            .then(|| {
                ExecutionPreparation::from_plan(
                    prepared_plan.logical_plan(),
                    slot_map_for_model_plan(prepared_plan.logical_plan()),
                )
            });
        let index_predicate_execution = covering_execution_preparation
            .as_ref()
            .and_then(ExecutionPreparation::strict_mode)
            .map(|program| IndexPredicateExecution {
                program,
                rejected_keys_counter: None,
            });

        if let Some(projected) = try_execute_prepared_covering_projection_rows_for_canister(
            db,
            prepared_plan.authority(),
            PreparedCoveringProjectionRuntime::new(
                prepared_plan.logical_plan(),
                index_prefix_specs,
                index_range_specs,
                index_predicate_execution,
                covering_metrics,
            ),
            covering,
            || prepared_plan.hybrid_covering_read_plan(),
        )? {
            charge_runtime_value_rows(projected.value_rows())?;
            let scanned_keys = usize::try_from(projected.row_count()).unwrap_or(usize::MAX);
            return Ok(StructuralProjectionPage {
                rows: projected,
                scanned_keys,
                last_emitted_logical: None,
                last_consumed_physical: None,
                has_more: false,
            });
        }
    }

    let SharedPreparedProjectionRuntimeHandoff {
        authority,
        prepared_projection_contract,
        scalar_runtime,
    } = prepared_plan.into_projection_runtime_handoff()?;
    let authored_page = group_seek
        .is_none()
        .then(|| scalar_runtime.plan_core.plan().scalar_plan().page.as_ref())
        .flatten();
    let authored_offset =
        authored_page.map_or(0, |page| usize::try_from(page.offset).unwrap_or(usize::MAX));
    let authored_limit = authored_page
        .and_then(|page| page.limit)
        .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));
    let page_entry_limit = if emit_cursor
        && !distinct
        && scalar_page_physical_progress_eligible(scalar_runtime.plan_core.plan())
    {
        page_work_envelope
            .and_then(|envelope| {
                envelope.limit(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited)
            })
            .and_then(|limit| usize::try_from(limit).ok())
    } else {
        None
    };
    if page_entry_limit == Some(0) {
        return Err(InternalError::page_unit_too_large(
            DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
            0,
            1,
        ));
    }

    let row_layout = authority.row_layout()?;
    let prepared_projection = prepared_projection_contract
        .as_deref()
        .ok_or_else(InternalError::query_executor_invariant)?;
    let resolved_order = (emit_cursor || (distinct && group_seek.is_none()))
        .then(|| {
            scalar_runtime
                .plan_core
                .plan()
                .require_resolved_order()
                .cloned()
        })
        .transpose()?;
    let distinct_strategy = if group_seek.is_some() {
        Some(ProjectionDistinctStrategy::OrderedAdjacent)
    } else if distinct {
        let order = resolved_order
            .as_ref()
            .ok_or_else(InternalError::query_executor_invariant)?;
        Some(projection_distinct_strategy(prepared_projection, order))
    } else {
        None
    };
    let distinct_window = group_seek
        .as_ref()
        .map(|(_, covering)| {
            let contract = covering
                .ordered_distinct_group_seek_contract()
                .ok_or_else(InternalError::query_executor_invariant)?;
            let (offset, limit) = contract.output_window();
            Ok::<_, InternalError>(ProjectionDistinctWindow::new(offset, Some(limit)))
        })
        .transpose()?
        .or_else(|| {
            distinct_strategy.map(|strategy| {
                let offset = match strategy {
                    ProjectionDistinctStrategy::OrderedAdjacent if continuation.has_progress() => 0,
                    ProjectionDistinctStrategy::OrderedAdjacent => authored_offset,
                    ProjectionDistinctStrategy::GlobalReplay => {
                        authored_offset.saturating_add(distinct_output_offset)
                    }
                };
                ProjectionDistinctWindow::new(offset, cursor_page_row_limit.or(authored_limit))
            })
        });
    let execution_continuation = if matches!(
        distinct_strategy,
        Some(ProjectionDistinctStrategy::GlobalReplay)
    ) {
        crate::db::executor::ScalarContinuationContext::initial()
    } else {
        continuation
    };
    let scalar_runtime = if distinct {
        scalar_runtime.into_scalar_page_suppressed()
    } else {
        scalar_runtime
    };

    let group_seek_page = group_seek
        .map(|(representatives, covering)| {
            let contract = covering
                .ordered_distinct_group_seek_contract()
                .ok_or_else(InternalError::query_executor_invariant)?;
            let projected_slot = contract.projected_slot();
            let retained_slot_layout = scalar_runtime
                .retained_slot_layout
                .as_ref()
                .ok_or_else(InternalError::query_executor_invariant)?;
            let value_index = retained_slot_layout
                .value_index_for_slot(projected_slot)
                .ok_or_else(InternalError::query_executor_invariant)?;
            let scanned_keys = representatives.len();
            let mut slot_rows = Vec::with_capacity(scanned_keys);
            for representative in representatives {
                let mut values = vec![None; retained_slot_layout.retained_value_count()];
                let slot = values
                    .get_mut(value_index)
                    .ok_or_else(InternalError::query_executor_invariant)?;
                *slot = Some(representative);
                slot_rows.push(RetainedSlotRow::from_indexed_values(
                    retained_slot_layout,
                    values,
                ));
            }
            Ok::<_, InternalError>((
                StructuralCursorPage::new_with_slot_rows(slot_rows),
                scanned_keys,
            ))
        })
        .transpose()?;

    // Phase 2: execute the canonical scalar retained-slot path and let the
    // projection materializer choose slot-row, data-row, or scalar fallback
    // shaping behind the executor boundary.
    let execute_scalar_page = || {
        if execution_continuation.has_progress() {
            execute_resumed_scalar_retained_slot_page_from_runtime_handoff_for_canister(
                db,
                debug,
                scalar_runtime,
                execution_continuation,
                emit_cursor,
                page_entry_limit,
            )
        } else {
            execute_initial_scalar_retained_slot_page_from_runtime_handoff_for_canister(
                db,
                debug,
                scalar_runtime,
                emit_cursor,
                distinct,
                min_optional_limits(
                    scan_budget.map(StructuralProjectionScanBudget::probe_limit),
                    page_entry_limit,
                ),
            )
        }
    };
    let ((page, scanned_keys), production_page_work_exhausted, scan_receipt) =
        if let Some(page) = group_seek_page {
            (page, false, None)
        } else if let Some(envelope) = page_work_envelope.filter(|_| page_entry_limit.is_some()) {
            let production = with_production_scalar_page_work(envelope, execute_scalar_page)?;
            (
                production.value,
                production.envelope_stopped,
                Some(production.receipt),
            )
        } else {
            (execute_scalar_page()?, false, None)
        };
    if let Some(scan_budget) = scan_budget
        && scan_budget.exceeded_by(scanned_keys)
    {
        return Err(sql_scan_budget_exceeded_error(scan_budget, scanned_keys));
    }

    let scan_page_work_exhausted = production_page_work_exhausted
        || page_entry_limit.is_some_and(|limit| scanned_keys >= limit);
    let scanned_physical_anchor = if scan_page_work_exhausted {
        page.last_scanned_physical_anchor()?
    } else {
        None
    };
    let mut output_work = page_work_envelope
        .map(|envelope| ProductionScalarOutputWork::new(envelope, scan_receipt))
        .transpose()?;
    let (mut rows, last_emitted_logical, has_more) = if let Some(strategy) = distinct_strategy {
        let projected = project_distinct(
            row_layout,
            prepared_projection,
            strategy,
            distinct_window.ok_or_else(InternalError::query_executor_invariant)?,
            page,
            DistinctProjectionRuntime::new(
                emit_cursor.then_some(resolved_order.as_ref()).flatten(),
                output_work.as_mut(),
                materialization_metrics,
            ),
        )?;
        projected.into_parts()
    } else if let Some(output_work) = output_work.as_mut() {
        let projected = project_admitted_page(
            row_layout,
            prepared_projection,
            page,
            resolved_order.as_ref(),
            cursor_page_row_limit,
            output_work,
            materialization_metrics,
        )?;
        projected.into_parts()
    } else {
        let (last_emitted_logical, has_more) = match cursor_page_row_limit {
            Some(page_row_limit) => {
                let retained_count = page.row_count().min(page_row_limit);
                let boundary = retained_count
                    .checked_sub(1)
                    .map(|row_index| {
                        page.cursor_boundary_at(
                            row_index,
                            &row_layout,
                            resolved_order
                                .as_ref()
                                .ok_or_else(InternalError::query_executor_invariant)?,
                        )
                    })
                    .transpose()?
                    .flatten();
                (boundary, page.row_count() >= page_row_limit)
            }
            None => (None, false),
        };
        let rows = project(
            row_layout,
            prepared_projection,
            page,
            materialization_metrics,
        )?;
        (
            rows,
            last_emitted_logical,
            has_more || scan_page_work_exhausted,
        )
    };

    if output_work.is_none() {
        charge_runtime_value_rows(rows.value_rows())?;
    } else if output_work.as_ref().is_some_and(|work| {
        work.receipt()
            .observed(DiagnosticExecutionBudgetResource::ResultRows)
            != u64::from(rows.row_count())
    }) {
        return Err(InternalError::query_executor_invariant());
    }

    if output_work.is_none()
        && distinct_strategy.is_none()
        && let Some(page_row_limit) = cursor_page_row_limit
    {
        rows.truncate(page_row_limit);
    }

    let output_page_work_exhausted = output_work
        .as_ref()
        .is_some_and(ProductionScalarOutputWork::envelope_stopped);
    let last_consumed_physical = if output_page_work_exhausted {
        None
    } else {
        scanned_physical_anchor
    };
    let has_more = has_more || scan_page_work_exhausted || output_page_work_exhausted;

    Ok(StructuralProjectionPage {
        rows,
        scanned_keys,
        last_emitted_logical,
        last_consumed_physical,
        has_more,
    })
}

fn scalar_page_physical_progress_eligible(
    plan: &crate::db::query::plan::AccessPlannedQuery,
) -> bool {
    plan.primary_key_names()
        .ok()
        .is_some_and(|primary_key_names| {
            plan.scalar_plan().order.as_ref().is_some_and(|order| {
                order
                    .primary_key_only_direction_fields(primary_key_names.as_slice())
                    .is_some()
            })
        })
}

const fn min_optional_limits(left: Option<usize>, right: Option<usize>) -> Option<usize> {
    match (left, right) {
        (Some(left), Some(right)) => Some(if left < right { left } else { right }),
        (Some(limit), None) | (None, Some(limit)) => Some(limit),
        (None, None) => None,
    }
}

fn sql_scan_budget_exceeded_error(
    scan_budget: StructuralProjectionScanBudget,
    scanned_keys: usize,
) -> InternalError {
    InternalError::query_sql_write_boundary_with_facts(
        SqlWriteBoundaryCode::ExactUpdateScanBudgetExceeded,
        vec![
            (DiagnosticFactTag::ActualCount, scanned_keys as u64),
            (
                DiagnosticFactTag::Limit,
                scan_budget.max_scanned_keys() as u64,
            ),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::{StructuralProjectionScanBudget, sql_scan_budget_exceeded_error};
    use icydb_diagnostic_code::DiagnosticFactTag;

    #[test]
    fn sql_scan_budget_error_retains_exact_usage_and_limit() {
        let budget = StructuralProjectionScanBudget::try_new(4)
            .expect("positive non-max scan budget should be valid");
        let error = sql_scan_budget_exceeded_error(budget, 5);

        assert_eq!(
            error.diagnostic_facts(),
            vec![
                (DiagnosticFactTag::ActualCount, 5),
                (DiagnosticFactTag::Limit, 4),
            ],
        );
    }
}
