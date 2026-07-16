//! Module: executor::aggregate::terminals
//! Responsibility: aggregate terminal API adapters over kernel aggregate execution.
//! Does not own: aggregate dispatch internals or fast-path eligibility derivation.
//! Boundary: user-facing aggregate terminal helpers on `LoadExecutor`.

use super::count_terminal::{
    execute_count_primary_key_cardinality_terminal_request, execute_scalar_terminal_preflight,
    try_prepare_scalar_terminal_preflight,
};
use crate::{
    db::{
        direction::Direction,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutableAccess, ExecutionKernel,
            PreparedAggregatePlan, PreparedExecutionPlan, TraversalRuntime,
            aggregate::{
                AccessPlannedQuery, AggregateFoldMode, AggregateKind,
                FieldSlot as PlannedFieldSlot, PreparedAggregateSpec,
                PreparedAggregateStreamingInputs, PreparedAggregateTargetField,
                PreparedFieldOrderSensitiveTerminalOp, PreparedOrderSensitiveTerminalBoundary,
                PreparedOrderSensitiveTerminalOp, PreparedScalarTerminalBoundary,
                PreparedScalarTerminalOp, PreparedScalarTerminalStrategy, ScalarAggregateOutput,
                ScalarTerminalKind,
                field::{
                    AggregateFieldValueError,
                    resolve_orderable_aggregate_target_slot_from_planner_slot,
                },
            },
            pipeline::contracts::LoadExecutor,
            plan_metrics::record_rows_scanned_for_path,
            planning::route::{
                CountTerminalFastPathContract, derive_count_terminal_fast_path_contract_for_model,
                derive_exists_terminal_fast_path_direction_for_model,
            },
        },
        index::predicate::IndexPredicateExecution,
        predicate::MissingRowPolicy,
        query::builder::aggregate::{ScalarTerminalBoundaryOutput, ScalarTerminalBoundaryRequest},
        registry::StoreHandle,
    },
    entity::{EntityKind, EntityValue},
    error::InternalError,
    types::EntityTag,
};

///
/// IndexTerminalRuntime
///
/// IndexTerminalRuntime bundles the structural runtime inputs needed by
/// index-stream aggregate terminals. This keeps COUNT/EXISTS helpers
/// generic-free without widening the public aggregate boundary surface.
///

struct IndexTerminalRuntime<'a> {
    entity_tag: EntityTag,
    store: StoreHandle,
    logical_plan: &'a AccessPlannedQuery,
    strict_mode: Option<&'a crate::db::index::IndexPredicateProgram>,
    index_prefix_specs: &'a [crate::db::executor::LoweredIndexPrefixSpec],
    index_range_specs: &'a [crate::db::executor::LoweredIndexRangeSpec],
}

// Execute one prepared scalar terminal boundary through one shared
// zero-window, fast-path, and kernel dispatch boundary.
fn run_prepared_scalar_terminal_boundary<E>(
    executor: &LoadExecutor<E>,
    op: &PreparedScalarTerminalOp,
    strategy: PreparedScalarTerminalStrategy,
    window_provably_empty: bool,
    prepared: PreparedAggregateStreamingInputs<'_>,
) -> Result<ScalarAggregateOutput, InternalError>
where
    E: EntityKind + EntityValue,
{
    let kind = op.scalar_terminal_kind()?;
    if window_provably_empty {
        return Ok(kind.zero_output());
    }

    match strategy {
        PreparedScalarTerminalStrategy::KernelAggregate => {
            execute_kernel_terminal_request(executor, prepared, op)
        }
        PreparedScalarTerminalStrategy::CountPrimaryKeyCardinality => {
            execute_count_primary_key_cardinality_terminal_request(prepared)
        }
        PreparedScalarTerminalStrategy::ExistingRows { direction } => {
            execute_existing_rows_terminal_request(prepared, op, direction)
        }
    }
}

// Execute one kernel-owned scalar terminal request from prepared terminal metadata.
fn execute_kernel_terminal_request<E>(
    executor: &LoadExecutor<E>,
    prepared: PreparedAggregateStreamingInputs<'_>,
    op: &PreparedScalarTerminalOp,
) -> Result<ScalarAggregateOutput, InternalError>
where
    E: EntityKind + EntityValue,
{
    op.validate_kernel_request_kind()?;
    let aggregate = match op {
        PreparedScalarTerminalOp::Count => PreparedAggregateSpec::terminal(AggregateKind::Count),
        PreparedScalarTerminalOp::Exists => PreparedAggregateSpec::terminal(AggregateKind::Exists),
        PreparedScalarTerminalOp::IdTerminal { kind } => PreparedAggregateSpec::terminal(*kind),
        PreparedScalarTerminalOp::IdBySlot {
            kind,
            target_field_name,
            field_slot,
        } => PreparedAggregateSpec::field_target(
            *kind,
            PreparedAggregateTargetField::new(
                target_field_name.clone(),
                *field_slot,
                true,
                true,
                prepared
                    .authority
                    .is_scalar_primary_key_field(target_field_name.as_str()),
            ),
        ),
    };
    let state =
        ExecutionKernel::prepare_aggregate_execution_state_from_prepared(prepared, aggregate)?;

    let output = ExecutionKernel::execute_prepared_aggregate_state(executor, state)?;
    #[cfg(feature = "diagnostics")]
    super::terminal_attribution::record_kernel_aggregate_terminal_attribution();

    Ok(output)
}

// Execute one COUNT/EXISTS existing-row terminal through one streaming fold
// without materializing the effective window.
fn execute_existing_rows_terminal_request(
    prepared: PreparedAggregateStreamingInputs<'_>,
    op: &PreparedScalarTerminalOp,
    direction: Direction,
) -> Result<ScalarAggregateOutput, InternalError> {
    let runtime = IndexTerminalRuntime {
        entity_tag: prepared.authority.entity_tag(),
        store: prepared.store,
        logical_plan: &prepared.logical_plan,
        strict_mode: prepared.execution_preparation.strict_mode(),
        index_prefix_specs: prepared.index_prefix_specs.as_ref(),
        index_range_specs: prepared.index_range_specs.as_ref(),
    };
    let aggregate_kind = match op {
        PreparedScalarTerminalOp::Count => ScalarTerminalKind::Count,
        PreparedScalarTerminalOp::Exists => ScalarTerminalKind::Exists,
        PreparedScalarTerminalOp::IdTerminal { .. } | PreparedScalarTerminalOp::IdBySlot { .. } => {
            return Err(InternalError::query_executor_invariant());
        }
    };
    let aggregate_output = aggregate_index_terminal_output_with_runtime(
        runtime,
        aggregate_kind,
        direction,
    )
    .map(|(output, rows_scanned)| {
        record_rows_scanned_for_path(prepared.authority.entity_path(), rows_scanned);
        #[cfg(feature = "diagnostics")]
        super::terminal_attribution::record_existing_rows_terminal_attribution(rows_scanned);
        output
    })?;

    match op {
        PreparedScalarTerminalOp::Count => aggregate_output
            .into_count()
            .map(ScalarAggregateOutput::Count),
        PreparedScalarTerminalOp::Exists => aggregate_output
            .into_exists()
            .map(ScalarAggregateOutput::Exists),
        PreparedScalarTerminalOp::IdTerminal { .. } | PreparedScalarTerminalOp::IdBySlot { .. } => {
            Err(InternalError::query_executor_invariant())
        }
    }
}

// Resolve an index-backed key stream and execute one reducer kind.
fn aggregate_index_terminal_output_with_runtime(
    runtime: IndexTerminalRuntime<'_>,
    kind: ScalarTerminalKind,
    direction: Direction,
) -> Result<(ScalarAggregateOutput, usize), InternalError> {
    let IndexTerminalRuntime {
        entity_tag,
        store,
        logical_plan,
        strict_mode,
        index_prefix_specs,
        index_range_specs,
    } = runtime;

    // Phase 1: compile predicate/runtime inputs over the prepared logical plan.
    let index_predicate_execution = strict_mode.map(|program| IndexPredicateExecution {
        program,
        rejected_keys_counter: None,
    });

    // Phase 2: resolve the access key stream directly from index-backed bindings.
    let access = ExecutableAccess::from_executable_plan(
        logical_plan.access.executable_contract(),
        AccessStreamBindings::new(
            index_prefix_specs,
            index_range_specs,
            AccessScanContinuationInput::new(None, direction),
        ),
        None,
        index_predicate_execution,
    );
    let runtime = TraversalRuntime::new(store, entity_tag);
    let mut key_stream = runtime.ordered_key_stream_from_runtime_access(access)?;

    // Phase 3: fold through existing-row semantics and record scan metrics.
    let (aggregate_output, rows_scanned) = ExecutionKernel::run_streaming_aggregate_reducer(
        store,
        logical_plan,
        kind,
        direction,
        AggregateFoldMode::ExistingRows,
        &mut key_stream,
    )?;

    Ok((aggregate_output, rows_scanned))
}

// Execute one prepared order-sensitive terminal contract without consulting
// plan-owned slot resolution or aggregate setup again.
fn execute_prepared_order_sensitive_terminal_boundary<E>(
    executor: &LoadExecutor<E>,
    prepared_boundary: PreparedOrderSensitiveTerminalBoundary<'_>,
) -> Result<ScalarTerminalBoundaryOutput, InternalError>
where
    E: EntityKind + EntityValue,
{
    let PreparedOrderSensitiveTerminalBoundary { op, prepared } = prepared_boundary;

    match op {
        PreparedOrderSensitiveTerminalOp::ResponseOrder { kind } => {
            let aggregate_output = execute_kernel_terminal_request(
                executor,
                prepared,
                &PreparedScalarTerminalOp::IdTerminal { kind },
            )?;

            aggregate_output
                .into_optional_id_terminal(kind)
                .map(ScalarTerminalBoundaryOutput::Id)
        }
        PreparedOrderSensitiveTerminalOp::FieldOrder { field_slot, op } => match op {
            PreparedFieldOrderSensitiveTerminalOp::Nth { nth } => executor
                .execute_nth_field_aggregate_with_slot(prepared, field_slot, nth)
                .map(ScalarTerminalBoundaryOutput::Id),
            PreparedFieldOrderSensitiveTerminalOp::Median => executor
                .execute_median_field_aggregate_with_slot(prepared, field_slot)
                .map(ScalarTerminalBoundaryOutput::Id),
            PreparedFieldOrderSensitiveTerminalOp::MinMax => executor
                .execute_min_max_field_aggregate_with_slot(prepared, field_slot)
                .map(ScalarTerminalBoundaryOutput::IdPair),
        },
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Build the canonical rejection for requests that do not belong on the
    // scalar COUNT/EXISTS/id terminal boundary.
    fn scalar_terminal_boundary_request_unsupported() -> InternalError {
        InternalError::query_executor_invariant()
    }

    // Build the canonical rejection for requests that do not belong on the
    // order-sensitive first/last/nth/median/min-max terminal boundary.
    fn order_sensitive_terminal_boundary_request_required() -> InternalError {
        InternalError::query_executor_invariant()
    }

    // Resolve one planner field slot and package it into the shared
    // order-sensitive field-order terminal boundary shape.
    fn prepare_field_order_sensitive_terminal_boundary(
        target_field: PlannedFieldSlot,
        op: PreparedFieldOrderSensitiveTerminalOp,
    ) -> Result<PreparedOrderSensitiveTerminalOp, InternalError> {
        let field_slot = resolve_orderable_aggregate_target_slot_from_planner_slot(&target_field)
            .map_err(AggregateFieldValueError::into_internal_error)?;

        Ok(PreparedOrderSensitiveTerminalOp::FieldOrder { field_slot, op })
    }

    // Execute one scalar aggregate terminal family request from the typed API
    // boundary, lower plan-derived policy into one prepared terminal contract,
    // and then execute that prepared contract.
    pub(in crate::db) fn execute_scalar_terminal_request(
        &self,
        plan: PreparedExecutionPlan<E>,
        request: ScalarTerminalBoundaryRequest,
    ) -> Result<ScalarTerminalBoundaryOutput, InternalError> {
        let plan = plan.into_prepared_aggregate_plan();

        if let Some(preflight) = try_prepare_scalar_terminal_preflight(&plan, &request)
            && let Some(output) = execute_scalar_terminal_preflight(self, preflight)?
        {
            return Ok(output);
        }

        match request {
            ScalarTerminalBoundaryRequest::Count
            | ScalarTerminalBoundaryRequest::Exists
            | ScalarTerminalBoundaryRequest::IdBySlot { .. } => {
                let prepared = self.prepare_scalar_terminal_boundary(plan, request)?;

                self.execute_prepared_scalar_terminal_boundary(prepared)
            }
            ScalarTerminalBoundaryRequest::IdTerminal { kind } => match kind {
                AggregateKind::First | AggregateKind::Last => {
                    let prepared = self.prepare_order_sensitive_terminal_boundary(
                        plan,
                        ScalarTerminalBoundaryRequest::IdTerminal { kind },
                    )?;

                    execute_prepared_order_sensitive_terminal_boundary(self, prepared)
                }
                _ => {
                    let prepared = self.prepare_scalar_terminal_boundary(
                        plan,
                        ScalarTerminalBoundaryRequest::IdTerminal { kind },
                    )?;

                    self.execute_prepared_scalar_terminal_boundary(prepared)
                }
            },
            ScalarTerminalBoundaryRequest::NthBySlot { .. }
            | ScalarTerminalBoundaryRequest::MedianBySlot { .. }
            | ScalarTerminalBoundaryRequest::MinMaxBySlot { .. } => {
                let prepared = self.prepare_order_sensitive_terminal_boundary(plan, request)?;

                execute_prepared_order_sensitive_terminal_boundary(self, prepared)
            }
        }
    }

    // Lower one public scalar terminal request into one prepared terminal
    // boundary so execution no longer derives fast-path policy from the plan.
    fn prepare_scalar_terminal_boundary(
        &self,
        plan: PreparedAggregatePlan,
        request: ScalarTerminalBoundaryRequest,
    ) -> Result<PreparedScalarTerminalBoundary<'_>, InternalError> {
        let prepared = self.prepare_scalar_aggregate_boundary(plan)?;
        let boundary = match request {
            ScalarTerminalBoundaryRequest::Count => {
                let (strategy, window_provably_empty) = {
                    let lowered_access = prepared.lowered_access()?;
                    let strategy =
                        derive_count_terminal_fast_path_contract_for_model(
                            &prepared.logical_plan,
                            &lowered_access,
                            prepared.execution_preparation.strict_mode().is_some(),
                        )
                        .map_or(
                            PreparedScalarTerminalStrategy::KernelAggregate,
                            |contract| match contract {
                                CountTerminalFastPathContract::PrimaryKeyCardinality => {
                                    PreparedScalarTerminalStrategy::CountPrimaryKeyCardinality
                                }
                                CountTerminalFastPathContract::PrimaryKeyExistingRows(
                                    direction,
                                )
                                | CountTerminalFastPathContract::IndexCoveringExistingRows(
                                    direction,
                                ) => PreparedScalarTerminalStrategy::ExistingRows { direction },
                            },
                        );

                    (strategy, prepared.window_is_provably_empty(&lowered_access))
                };

                PreparedScalarTerminalBoundary {
                    op: PreparedScalarTerminalOp::Count,
                    strategy,
                    window_provably_empty,
                    prepared,
                }
            }
            ScalarTerminalBoundaryRequest::Exists => {
                let strategy = derive_exists_terminal_fast_path_direction_for_model(
                    &prepared.logical_plan,
                    prepared.execution_preparation.strict_mode().is_some(),
                )
                .map_or(
                    PreparedScalarTerminalStrategy::KernelAggregate,
                    |direction| PreparedScalarTerminalStrategy::ExistingRows { direction },
                );
                let strategy = if matches!(prepared.consistency(), MissingRowPolicy::Ignore) {
                    PreparedScalarTerminalStrategy::KernelAggregate
                } else {
                    strategy
                };

                PreparedScalarTerminalBoundary {
                    op: PreparedScalarTerminalOp::Exists,
                    strategy,
                    window_provably_empty: {
                        let lowered_access = prepared.lowered_access()?;
                        prepared.window_is_provably_empty(&lowered_access)
                    },
                    prepared,
                }
            }
            ScalarTerminalBoundaryRequest::IdTerminal { kind } => PreparedScalarTerminalBoundary {
                op: PreparedScalarTerminalOp::IdTerminal { kind },
                strategy: PreparedScalarTerminalStrategy::KernelAggregate,
                window_provably_empty: {
                    let lowered_access = prepared.lowered_access()?;
                    prepared.window_is_provably_empty(&lowered_access)
                },
                prepared,
            },
            ScalarTerminalBoundaryRequest::IdBySlot { kind, target_field } => {
                let field_slot =
                    resolve_orderable_aggregate_target_slot_from_planner_slot(&target_field)
                        .map_err(AggregateFieldValueError::into_internal_error)?;

                PreparedScalarTerminalBoundary {
                    op: PreparedScalarTerminalOp::IdBySlot {
                        kind,
                        target_field_name: target_field.field().to_string(),
                        field_slot,
                    },
                    strategy: PreparedScalarTerminalStrategy::KernelAggregate,
                    window_provably_empty: {
                        let lowered_access = prepared.lowered_access()?;
                        prepared.window_is_provably_empty(&lowered_access)
                    },
                    prepared,
                }
            }
            ScalarTerminalBoundaryRequest::NthBySlot { .. }
            | ScalarTerminalBoundaryRequest::MedianBySlot { .. }
            | ScalarTerminalBoundaryRequest::MinMaxBySlot { .. } => {
                return Err(Self::scalar_terminal_boundary_request_unsupported());
            }
        };

        Ok(boundary)
    }

    // Lower one order-sensitive terminal request into one prepared boundary so
    // execution consumes the same resolved slot metadata and aggregate setup
    // shape as an explicit family, rather than as ad hoc special cases.
    fn prepare_order_sensitive_terminal_boundary(
        &self,
        plan: PreparedAggregatePlan,
        request: ScalarTerminalBoundaryRequest,
    ) -> Result<PreparedOrderSensitiveTerminalBoundary<'_>, InternalError> {
        let op = match request {
            ScalarTerminalBoundaryRequest::IdTerminal { kind } => match kind {
                AggregateKind::First | AggregateKind::Last => {
                    PreparedOrderSensitiveTerminalOp::ResponseOrder { kind }
                }
                _ => {
                    return Err(Self::order_sensitive_terminal_boundary_request_required());
                }
            },
            ScalarTerminalBoundaryRequest::NthBySlot { target_field, nth } => {
                Self::prepare_field_order_sensitive_terminal_boundary(
                    target_field,
                    PreparedFieldOrderSensitiveTerminalOp::Nth { nth },
                )?
            }
            ScalarTerminalBoundaryRequest::MedianBySlot { target_field } => {
                Self::prepare_field_order_sensitive_terminal_boundary(
                    target_field,
                    PreparedFieldOrderSensitiveTerminalOp::Median,
                )?
            }
            ScalarTerminalBoundaryRequest::MinMaxBySlot { target_field } => {
                Self::prepare_field_order_sensitive_terminal_boundary(
                    target_field,
                    PreparedFieldOrderSensitiveTerminalOp::MinMax,
                )?
            }
            ScalarTerminalBoundaryRequest::Count
            | ScalarTerminalBoundaryRequest::Exists
            | ScalarTerminalBoundaryRequest::IdBySlot { .. } => {
                return Err(Self::order_sensitive_terminal_boundary_request_required());
            }
        };
        let prepared = self.prepare_scalar_aggregate_boundary(plan)?;

        Ok(PreparedOrderSensitiveTerminalBoundary { op, prepared })
    }

    // Execute one prepared scalar terminal contract without consulting plan-owned
    // fast-path policy.
    fn execute_prepared_scalar_terminal_boundary(
        &self,
        boundary: PreparedScalarTerminalBoundary<'_>,
    ) -> Result<ScalarTerminalBoundaryOutput, InternalError> {
        let PreparedScalarTerminalBoundary {
            op,
            strategy,
            window_provably_empty,
            prepared,
        } = boundary;
        let aggregate_output = run_prepared_scalar_terminal_boundary(
            self,
            &op,
            strategy,
            window_provably_empty,
            prepared,
        )?;

        match op {
            PreparedScalarTerminalOp::Count => aggregate_output
                .into_count()
                .map(ScalarTerminalBoundaryOutput::Count),
            PreparedScalarTerminalOp::Exists => aggregate_output
                .into_exists()
                .map(ScalarTerminalBoundaryOutput::Exists),
            PreparedScalarTerminalOp::IdTerminal { kind }
            | PreparedScalarTerminalOp::IdBySlot { kind, .. } => aggregate_output
                .into_optional_id_terminal(kind)
                .map(ScalarTerminalBoundaryOutput::Id),
        }
    }
}
