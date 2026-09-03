//! Module: executor::pipeline::runtime::attempt
//! Responsibility: route-attempt key-stream resolution and materialization orchestration.
//! Does not own: execution-input construction or route planning.
//! Boundary: executes one already-assembled `ExecutionInputs` snapshot.

use crate::db::executor::pipeline::contracts::KernelRowsExecutionAttempt;
use crate::{
    db::{
        executor::{
            ExecutionRoutePlan, OrderedKeyStreamBox, ScalarContinuationContext,
            pipeline::{
                contracts::{
                    ExecutionInputs, ExecutionOutcomeMetrics, MaterializedExecutionAttempt,
                    ResolvedExecutionKeyStream, ScalarPageMaterialization,
                },
                operators::decorate_resolved_execution_key_stream,
                runtime::ExecutionMaterializationContract,
            },
        },
        index::IndexCompilePolicy,
    },
    error::InternalError,
};
use std::{cell::RefCell, rc::Rc};

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
        continuation: ScalarContinuationContext,
        key_stream: &'req mut OrderedKeyStreamBox,
    ) -> Result<ScalarPageMaterialization, InternalError> {
        self.materialization_contract(route_plan)
            .materialize_resolved_execution_stream(
                self.inputs.runtime(),
                self.inputs.emit_cursor(),
                self.inputs.consistency(),
                continuation,
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
        continuation: ScalarContinuationContext,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<MaterializedExecutionAttempt, InternalError> {
        let mut resolved = self.resolve_execution_key_stream(route_plan, predicate_compile_mode)?;
        self.apply_enforced_scan_probe(resolved.key_stream_mut());
        let last_scanned_key = (self.inputs.emit_cursor()
            && self.inputs.enforced_scan_probe_limit().is_some())
        .then(|| {
            let last_scanned_key = Rc::new(RefCell::new(None));
            let inner = std::mem::replace(resolved.key_stream_mut(), OrderedKeyStreamBox::empty());
            *resolved.key_stream_mut() =
                OrderedKeyStreamBox::observed(inner, Rc::clone(&last_scanned_key));
            last_scanned_key
        });
        let ScalarPageMaterialization {
            payload,
            rows_scanned: keys_scanned,
            post_access_rows,
        } = self.materialize_resolved_execution_stream(
            route_plan,
            continuation,
            resolved.key_stream_mut(),
        )?;
        let payload = match last_scanned_key {
            Some(last_scanned_key) => payload.with_last_scanned_key(
                last_scanned_key
                    .try_borrow_mut()
                    .map_err(|_| InternalError::query_executor_invariant())?
                    .take(),
            ),
            None => payload,
        };
        let rows_scanned = resolved.rows_scanned_override().unwrap_or(keys_scanned);

        Ok(MaterializedExecutionAttempt {
            payload,
            metrics: ExecutionOutcomeMetrics {
                rows_scanned,
                post_access_rows,
            },
        })
    }

    /// Materialize one route-plan candidate into post-access scalar kernel rows.
    pub(in crate::db::executor) fn materialize_route_attempt_kernel_rows(
        &self,
        route_plan: &ExecutionRoutePlan,
        continuation: ScalarContinuationContext,
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
                resolved.key_stream_mut(),
            )?;
        attempt.metrics.rows_scanned = resolved
            .rows_scanned_override()
            .unwrap_or(attempt.metrics.rows_scanned);
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
