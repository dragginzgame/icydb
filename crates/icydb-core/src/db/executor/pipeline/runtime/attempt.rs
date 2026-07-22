//! Module: executor::pipeline::runtime::attempt
//! Responsibility: route-attempt key-stream resolution and materialization orchestration.
//! Does not own: execution-input construction or route planning.
//! Boundary: executes one already-assembled `ExecutionInputs` snapshot.

#[cfg(feature = "sql")]
use crate::db::executor::pipeline::contracts::KernelRowsExecutionAttempt;
use crate::{
    db::{
        executor::{
            ExecutionRoutePlan, OrderedKeyStreamBox, ScalarContinuationContext,
            pipeline::{
                contracts::{
                    ExecutionInputs, ExecutionOutcomeMetrics, MaterializedExecutionAttempt,
                    ResolvedExecutionKeyStream, StructuralCursorPage,
                },
                operators::decorate_resolved_execution_key_stream,
                runtime::ExecutionMaterializationContract,
            },
        },
        index::IndexCompilePolicy,
    },
    error::InternalError,
};

type MaterializedExecutionPayloadResult = (StructuralCursorPage, usize, usize);

///
/// ExecutionAttemptKernel
///
/// ExecutionAttemptKernel owns route-attempt orchestration for one immutable
/// execution-input snapshot.
/// It keeps execution behavior in the runtime layer while `ExecutionInputs`
/// remains the data snapshot consumed by scalar and aggregate paths.
///

pub(in crate::db::executor) struct ExecutionAttemptKernel<'a> {
    pub(in crate::db::executor::pipeline::runtime) inputs: &'a ExecutionInputs<'a>,
}

impl<'a> ExecutionAttemptKernel<'a> {
    /// Build one route-attempt kernel over an already-assembled execution input snapshot.
    #[must_use]
    pub(in crate::db::executor) const fn new(inputs: &'a ExecutionInputs<'a>) -> Self {
        Self { inputs }
    }

    // Build the shared materialization contract once so the two outward
    // request shapes stay aligned on predicate/projection/retained-slot wiring.
    fn materialization_contract<'req>(
        &'req self,
        route_plan: &ExecutionRoutePlan,
    ) -> ExecutionMaterializationContract<'req> {
        ExecutionMaterializationContract {
            plan: self.inputs.plan(),
            residual_filter_program: self.inputs.residual_filter_program(),
            scan_budget_hint: route_plan.scan_hints.load_scan_budget_hint,
            load_order_route_mode: route_plan.load_order_route_mode(),
            validate_projection: self.inputs.validate_projection(),
            retain_slot_rows: self.inputs.retain_slot_rows(),
            retained_slot_layout: self.inputs.retained_slot_layout(),
            prepared_projection_validation: self.inputs.prepared_projection_validation(),
        }
    }

    /// Materialize one resolved scalar key stream through the aligned
    /// row-collector or canonical page runtime lane owned by this route
    /// attempt kernel.
    pub(in crate::db::executor) fn materialize_resolved_execution_stream<'req>(
        &'req self,
        route_plan: &ExecutionRoutePlan,
        continuation: &'req ScalarContinuationContext,
        key_stream: &'req mut OrderedKeyStreamBox,
    ) -> Result<MaterializedExecutionPayloadResult, InternalError> {
        self.materialization_contract(route_plan)
            .materialize_resolved_execution_stream(
                self.inputs.runtime(),
                self.inputs.emit_cursor(),
                self.inputs.consistency(),
                continuation,
                route_plan.direction(),
                key_stream,
            )
    }

    /// Resolve one execution key stream under the canonical DISTINCT
    /// decoration contract for this prepared execution-input boundary.
    pub(in crate::db::executor) fn resolve_execution_key_stream(
        &self,
        route_plan: &ExecutionRoutePlan,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<ResolvedExecutionKeyStream, InternalError> {
        let resolved =
            self.resolve_execution_key_stream_without_distinct(route_plan, predicate_compile_mode)?;

        Ok(decorate_resolved_execution_key_stream(
            resolved,
            self.inputs.plan(),
            self.inputs.stream_bindings().direction(),
        ))
    }

    /// Materialize one route-plan candidate end to end from resolved key
    /// stream decoration through structural page materialization.
    pub(in crate::db::executor) fn materialize_route_attempt(
        &self,
        route_plan: &ExecutionRoutePlan,
        continuation: &ScalarContinuationContext,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<MaterializedExecutionAttempt, InternalError> {
        let mut resolved = self.resolve_execution_key_stream(route_plan, predicate_compile_mode)?;
        self.apply_enforced_scan_probe(resolved.key_stream_mut());
        let (payload, keys_scanned, post_access_rows) = self
            .materialize_resolved_execution_stream(
                route_plan,
                continuation,
                resolved.key_stream_mut(),
            )?;
        let rows_scanned = resolved.rows_scanned_override().unwrap_or(keys_scanned);

        Ok(MaterializedExecutionAttempt {
            payload,
            metrics: ExecutionOutcomeMetrics {
                rows_scanned,
                post_access_rows,
                optimization: resolved.optimization(),
                index_predicate_applied: resolved.index_predicate_applied(),
                index_predicate_keys_rejected: resolved.index_predicate_keys_rejected(),
                distinct_keys_deduped: resolved.distinct_keys_deduped(),
            },
        })
    }

    /// Materialize one route-plan candidate into post-access scalar kernel rows.
    #[cfg(feature = "sql")]
    pub(in crate::db::executor) fn materialize_route_attempt_kernel_rows(
        &self,
        route_plan: &ExecutionRoutePlan,
        continuation: &ScalarContinuationContext,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<KernelRowsExecutionAttempt, InternalError> {
        let mut resolved = self.resolve_execution_key_stream(route_plan, predicate_compile_mode)?;
        self.apply_enforced_scan_probe(resolved.key_stream_mut());
        let mut attempt = self
            .materialization_contract(route_plan)
            .materialize_resolved_execution_stream_to_kernel_rows(
                self.inputs.runtime(),
                self.inputs.consistency(),
                continuation,
                route_plan.direction(),
                resolved.key_stream_mut(),
            )?;
        attempt.metrics.rows_scanned = resolved
            .rows_scanned_override()
            .unwrap_or(attempt.metrics.rows_scanned);
        attempt.metrics.optimization = resolved.optimization();
        attempt.metrics.index_predicate_applied = resolved.index_predicate_applied();
        attempt.metrics.index_predicate_keys_rejected = resolved.index_predicate_keys_rejected();
        attempt.metrics.distinct_keys_deduped = resolved.distinct_keys_deduped();

        Ok(attempt)
    }

    // Apply a hard execution-only scan probe outside route-owned advisory
    // hints. The caller rejects any cap-plus-one result before consuming the
    // partial payload, so materialized fallback routes remain fail-closed.
    fn apply_enforced_scan_probe(&self, key_stream: &mut OrderedKeyStreamBox) {
        let Some(probe_limit) = self.inputs.enforced_scan_probe_limit() else {
            return;
        };
        let inner = std::mem::replace(key_stream, OrderedKeyStreamBox::empty());
        *key_stream = OrderedKeyStreamBox::budgeted(inner, probe_limit);
    }
}
