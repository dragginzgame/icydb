//! Module: executor::projection::facade
//! Responsibility: executor-owned structural projection execution coordination.
//! Does not own: SQL DTO shaping, projection label policy, or diagnostic counter storage.
//! Boundary: accepts prepared projection intent and returns structural projected rows.

use crate::{
    db::{
        Db,
        executor::{
            CoveringProjectionMetricsRecorder, ExecutionPreparation,
            ProjectionMaterializationMetricsRecorder, SharedPreparedExecutionPlan,
            SharedPreparedProjectionRuntimeHandoff,
            pipeline::execute_initial_scalar_retained_slot_page_from_runtime_handoff_for_canister,
            planning::preparation::slot_map_for_model_plan,
            projection::{
                MaterializedProjectionRows, PreparedCoveringProjectionRuntime,
                ProjectionDistinctWindow, project, project_distinct,
                try_execute_prepared_covering_projection_rows_for_canister,
            },
        },
        index::predicate::IndexPredicateExecution,
    },
    error::InternalError,
    traits::CanisterKind,
};
use icydb_diagnostic_code::SqlWriteBoundaryCode;

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
}

impl StructuralProjectionRequest {
    /// Build one structural projection request from adapter-provided runtime
    /// switches and diagnostic callback bundles.
    pub(in crate::db) const fn new(
        debug: bool,
        prepared_plan: SharedPreparedExecutionPlan,
        covering_metrics: CoveringProjectionMetricsRecorder,
        materialization_metrics: ProjectionMaterializationMetricsRecorder,
    ) -> Self {
        Self {
            debug,
            prepared_plan,
            covering_metrics,
            materialization_metrics,
            scan_budget: None,
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
    let StructuralProjectionRequest {
        debug,
        prepared_plan,
        covering_metrics,
        materialization_metrics,
        scan_budget,
    } = request;
    let distinct = prepared_plan.logical_plan().scalar_plan().distinct;

    // Phase 1: choose the covering projection lane only for non-DISTINCT
    // requests. DISTINCT must see final projected rows in scalar execution order
    // before executor-owned deduplication and windowing.
    if !distinct && scan_budget.is_none() {
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
            return Ok(projected);
        }
    }

    let SharedPreparedProjectionRuntimeHandoff {
        authority,
        prepared_projection_contract,
        scalar_runtime,
    } = prepared_plan.into_projection_runtime_handoff()?;
    let distinct_window = distinct.then(|| {
        ProjectionDistinctWindow::from_page(
            scalar_runtime.plan_core.plan().scalar_plan().page.as_ref(),
        )
    });
    let scalar_runtime = if distinct {
        scalar_runtime.into_scalar_page_suppressed()
    } else {
        scalar_runtime
    };

    // Phase 2: execute the canonical scalar retained-slot path and let the
    // projection materializer choose slot-row, data-row, or scalar fallback
    // shaping behind the executor boundary.
    let row_layout = authority.row_layout()?;
    let prepared_projection = prepared_projection_contract
        .as_deref()
        .ok_or_else(InternalError::query_executor_invariant)?;
    let (page, scanned_keys) =
        execute_initial_scalar_retained_slot_page_from_runtime_handoff_for_canister(
            db,
            debug,
            scalar_runtime,
            distinct,
            scan_budget.map(StructuralProjectionScanBudget::probe_limit),
        )?;
    if scan_budget.is_some_and(|budget| budget.exceeded_by(scanned_keys)) {
        return Err(InternalError::query_sql_write_boundary(
            SqlWriteBoundaryCode::ExactUpdateScanBudgetExceeded,
        ));
    }

    let rows = if distinct {
        project_distinct(
            row_layout,
            prepared_projection,
            distinct_window.ok_or_else(InternalError::query_executor_invariant)?,
            page,
            materialization_metrics,
        )?
    } else {
        project(
            row_layout,
            prepared_projection,
            page,
            materialization_metrics,
        )?
    };

    Ok(rows)
}
