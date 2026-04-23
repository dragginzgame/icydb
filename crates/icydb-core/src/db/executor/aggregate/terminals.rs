//! Module: executor::aggregate::terminals
//! Responsibility: aggregate terminal API adapters over kernel aggregate execution.
//! Does not own: aggregate dispatch internals or fast-path eligibility derivation.
//! Boundary: user-facing aggregate terminal helpers on `LoadExecutor`.

use crate::{
    db::{
        access::ExecutionPathPayload,
        data::DataKey,
        direction::Direction,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutableAccess, ExecutionKernel,
            PreparedAggregatePlan, PreparedExecutionPlan, TraversalRuntime,
            aggregate::{
                AggregateFoldMode, AggregateKind, PreparedAggregateSpec,
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
                CountTerminalFastPathContract, ExistsTerminalFastPathContract,
                derive_count_terminal_fast_path_contract_for_model,
                derive_exists_terminal_fast_path_contract_for_model,
            },
        },
        index::predicate::IndexPredicateExecution,
        query::plan::{FieldSlot as PlannedFieldSlot, PageSpec},
        registry::StoreHandle,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue, KeyValueCodec},
    types::{EntityTag, Id},
    value::{StorageKey, storage_key_as_runtime_value},
};
use std::ops::Bound;

type IdPairTerminalOutput<E> = Option<(Id<E>, Id<E>)>;

///
/// ExistingRowsTerminalRuntime
///
/// ExistingRowsTerminalRuntime bundles the structural runtime inputs needed by
/// existing-row aggregate terminals.
/// This keeps COUNT/EXISTS helpers generic-free without widening the public
/// aggregate boundary surface.
///

struct ExistingRowsTerminalRuntime<'a> {
    entity_tag: EntityTag,
    store: StoreHandle,
    logical_plan: &'a crate::db::query::plan::AccessPlannedQuery,
    strict_mode: Option<&'a crate::db::index::IndexPredicateProgram>,
    index_prefix_specs: &'a [crate::db::executor::LoweredIndexPrefixSpec],
    index_range_specs: &'a [crate::db::executor::LoweredIndexRangeSpec],
}

// Typed boundary request for one public scalar aggregate terminal family call.
pub(in crate::db) enum ScalarTerminalBoundaryRequest {
    Count,
    Exists,
    IdTerminal {
        kind: AggregateKind,
    },
    IdBySlot {
        kind: AggregateKind,
        target_field: PlannedFieldSlot,
    },
    NthBySlot {
        target_field: PlannedFieldSlot,
        nth: usize,
    },
    MedianBySlot {
        target_field: PlannedFieldSlot,
    },
    MinMaxBySlot {
        target_field: PlannedFieldSlot,
    },
}

// Typed boundary output for one public scalar aggregate terminal family call.
pub(in crate::db) enum ScalarTerminalBoundaryOutput {
    Count(u32),
    Exists(bool),
    Id(Option<StorageKey>),
    IdPair(Option<(StorageKey, StorageKey)>),
}

impl ScalarTerminalBoundaryOutput {
    // Build one canonical scalar terminal boundary mismatch on the owner type.
    fn output_kind_mismatch(message: &'static str) -> InternalError {
        InternalError::query_executor_invariant(message)
    }

    // Decode COUNT boundary output while preserving request/output mismatch context.
    pub(in crate::db) fn into_count(self) -> Result<u32, InternalError> {
        match self {
            Self::Count(value) => Ok(value),
            _ => Err(Self::output_kind_mismatch(
                "scalar terminal boundary COUNT output kind mismatch",
            )),
        }
    }

    // Decode EXISTS boundary output while preserving request/output mismatch context.
    pub(in crate::db) fn into_exists(self) -> Result<bool, InternalError> {
        match self {
            Self::Exists(value) => Ok(value),
            _ => Err(Self::output_kind_mismatch(
                "scalar terminal boundary EXISTS output kind mismatch",
            )),
        }
    }

    // Decode id-returning boundary output while preserving request/output mismatch context.
    pub(in crate::db) fn into_id<E>(self) -> Result<Option<Id<E>>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match self {
            Self::Id(value) => value.map(decode_storage_key_to_id::<E>).transpose(),
            _ => Err(Self::output_kind_mismatch(
                "scalar terminal boundary id output kind mismatch",
            )),
        }
    }

    // Decode paired-id boundary output while preserving request/output mismatch context.
    pub(in crate::db) fn into_id_pair<E>(self) -> Result<IdPairTerminalOutput<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match self {
            Self::IdPair(value) => value
                .map(|(left, right)| {
                    Ok((
                        decode_storage_key_to_id::<E>(left)?,
                        decode_storage_key_to_id::<E>(right)?,
                    ))
                })
                .transpose(),
            _ => Err(Self::output_kind_mismatch(
                "scalar terminal boundary id-pair output kind mismatch",
            )),
        }
    }
}

// Execute one prepared scalar terminal boundary through one shared
// zero-window, fast-path, and kernel dispatch boundary.
fn run_prepared_scalar_terminal_boundary<E>(
    executor: &LoadExecutor<E>,
    op: &PreparedScalarTerminalOp,
    strategy: PreparedScalarTerminalStrategy,
    prepared: PreparedAggregateStreamingInputs<'_>,
) -> Result<ScalarAggregateOutput, InternalError>
where
    E: EntityKind + EntityValue,
{
    let kind = op.scalar_terminal_kind()?;
    if prepared.window_is_provably_empty() {
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
                target_field_name.as_str() == prepared.authority.primary_key_name(),
            ),
        ),
    };
    let state =
        ExecutionKernel::prepare_aggregate_execution_state_from_prepared(prepared, aggregate);

    ExecutionKernel::execute_prepared_aggregate_state(executor, state)
}

// Execute prepared COUNT through store-cardinality fast-path semantics.
fn execute_count_primary_key_cardinality_terminal_request(
    prepared: PreparedAggregateStreamingInputs<'_>,
) -> Result<ScalarAggregateOutput, InternalError> {
    let (count, rows_scanned) = aggregate_count_from_pk_cardinality_with_store(
        &prepared.logical_plan,
        prepared.authority.entity_tag(),
        prepared.store,
    )?;
    record_rows_scanned_for_path(prepared.authority.entity_path(), rows_scanned);

    Ok(ScalarAggregateOutput::Count(count))
}

// Execute one COUNT/EXISTS existing-row terminal through one streaming fold
// without materializing the effective window.
fn execute_existing_rows_terminal_request(
    prepared: PreparedAggregateStreamingInputs<'_>,
    op: &PreparedScalarTerminalOp,
    direction: Direction,
) -> Result<ScalarAggregateOutput, InternalError> {
    let runtime = ExistingRowsTerminalRuntime {
        entity_tag: prepared.authority.entity_tag(),
        store: prepared.store,
        logical_plan: &prepared.logical_plan,
        strict_mode: prepared.execution_preparation.strict_mode(),
        index_prefix_specs: prepared.index_prefix_specs.as_slice(),
        index_range_specs: prepared.index_range_specs.as_slice(),
    };
    let aggregate_kind = match op {
        PreparedScalarTerminalOp::Count => ScalarTerminalKind::Count,
        PreparedScalarTerminalOp::Exists => ScalarTerminalKind::Exists,
        PreparedScalarTerminalOp::IdTerminal { .. } | PreparedScalarTerminalOp::IdBySlot { .. } => {
            return Err(InternalError::query_executor_invariant(
                "existing-row terminal execution requires COUNT or EXISTS op",
            ));
        }
    };
    let aggregate_output =
        aggregate_existing_rows_terminal_output_with_runtime(runtime, aggregate_kind, direction)
            .map(|(output, rows_scanned)| {
                record_rows_scanned_for_path(prepared.authority.entity_path(), rows_scanned);
                output
            })?;

    match op {
        PreparedScalarTerminalOp::Count => aggregate_output
            .into_count("existing-row COUNT reducer result kind mismatch")
            .map(ScalarAggregateOutput::Count),
        PreparedScalarTerminalOp::Exists => aggregate_output
            .into_exists("existing-row EXISTS reducer result kind mismatch")
            .map(ScalarAggregateOutput::Exists),
        PreparedScalarTerminalOp::IdTerminal { .. } | PreparedScalarTerminalOp::IdBySlot { .. } => {
            Err(InternalError::query_executor_invariant(
                "existing-row terminal finalization requires COUNT or EXISTS op",
            ))
        }
    }
}

// Resolve an index-backed existing-row key stream and execute one reducer kind.
fn aggregate_existing_rows_terminal_output_with_runtime(
    runtime: ExistingRowsTerminalRuntime<'_>,
    kind: ScalarTerminalKind,
    direction: Direction,
) -> Result<(ScalarAggregateOutput, usize), InternalError> {
    let ExistingRowsTerminalRuntime {
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
    let access = ExecutableAccess::new(
        &logical_plan.access,
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

// Resolve COUNT for PK full-scan/key-range shapes from store cardinality while
// preserving canonical page-window and scan-accounting semantics.
fn aggregate_count_from_pk_cardinality_with_store(
    logical_plan: &crate::db::query::plan::AccessPlannedQuery,
    entity_tag: EntityTag,
    store: StoreHandle,
) -> Result<(u32, usize), InternalError> {
    // Phase 1: snapshot pagination + access payload before resolving store cardinality.
    let page = logical_plan.scalar_plan().page.as_ref();
    let access_strategy = logical_plan.access.resolve_strategy();
    let Some(path) = access_strategy.as_path() else {
        return Err(InternalError::query_executor_invariant(
            "pk cardinality COUNT fast path requires single-path access strategy",
        ));
    };

    // Phase 2: read candidate-row cardinality directly from primary storage.
    let available_rows = match path.payload() {
        ExecutionPathPayload::FullScan => {
            let start_raw = DataKey::lower_bound_for(entity_tag).to_raw()?;
            let end_raw = DataKey::upper_bound_for(entity_tag).to_raw()?;

            store.with_data(|data| {
                data.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                    .count()
            })
        }
        ExecutionPathPayload::KeyRange { start, end } => {
            let start_raw = DataKey::try_from_structural_key(entity_tag, start)?.to_raw()?;
            let end_raw = DataKey::try_from_structural_key(entity_tag, end)?.to_raw()?;

            store.with_data(|data| {
                data.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                    .count()
            })
        }
        _ => {
            return Err(InternalError::query_executor_invariant(
                "pk cardinality COUNT fast path requires full-scan or key-range access",
            ));
        }
    };

    // Phase 3: apply canonical COUNT window semantics and emit scan metrics.
    let (count, rows_scanned) = count_window_result_from_page(page, available_rows);

    Ok((count, rows_scanned))
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
                .into_optional_id_terminal(
                    kind,
                    "aggregate order-sensitive id-terminal result kind mismatch",
                )
                .map(ScalarTerminalBoundaryOutput::Id)
        }
        PreparedOrderSensitiveTerminalOp::FieldOrder {
            target_field_name,
            field_slot,
            op,
        } => match op {
            PreparedFieldOrderSensitiveTerminalOp::Nth { nth } => executor
                .execute_nth_field_aggregate_with_slot(
                    prepared,
                    &target_field_name,
                    field_slot,
                    nth,
                )
                .map(ScalarTerminalBoundaryOutput::Id),
            PreparedFieldOrderSensitiveTerminalOp::Median => executor
                .execute_median_field_aggregate_with_slot(prepared, &target_field_name, field_slot)
                .map(ScalarTerminalBoundaryOutput::Id),
            PreparedFieldOrderSensitiveTerminalOp::MinMax => executor
                .execute_min_max_field_aggregate_with_slot(prepared, &target_field_name, field_slot)
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
        InternalError::query_executor_invariant(
            "prepared scalar terminal boundary only supports COUNT/EXISTS/id terminals",
        )
    }

    // Build the canonical rejection for requests that do not belong on the
    // order-sensitive first/last/nth/median/min-max terminal boundary.
    fn order_sensitive_terminal_boundary_request_required() -> InternalError {
        InternalError::query_executor_invariant(
            "order-sensitive terminal boundary requires first/last/nth/median/min-max request",
        )
    }

    // Resolve one planner field slot and package it into the shared
    // order-sensitive field-order terminal boundary shape.
    fn prepare_field_order_sensitive_terminal_boundary(
        target_field: PlannedFieldSlot,
        op: PreparedFieldOrderSensitiveTerminalOp,
    ) -> Result<PreparedOrderSensitiveTerminalOp, InternalError> {
        let field_slot = resolve_orderable_aggregate_target_slot_from_planner_slot(&target_field)
            .map_err(AggregateFieldValueError::into_internal_error)?;

        Ok(PreparedOrderSensitiveTerminalOp::FieldOrder {
            target_field_name: target_field.field().to_string(),
            field_slot,
            op,
        })
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
            ScalarTerminalBoundaryRequest::Count => PreparedScalarTerminalBoundary {
                op: PreparedScalarTerminalOp::Count,
                strategy: derive_count_terminal_fast_path_contract_for_model(
                    &prepared.logical_plan,
                    prepared.execution_preparation.strict_mode().is_some(),
                )
                .map_or(
                    PreparedScalarTerminalStrategy::KernelAggregate,
                    |contract| match contract {
                        CountTerminalFastPathContract::PrimaryKeyCardinality => {
                            PreparedScalarTerminalStrategy::CountPrimaryKeyCardinality
                        }
                        CountTerminalFastPathContract::PrimaryKeyExistingRows(direction)
                        | CountTerminalFastPathContract::IndexCoveringExistingRows(direction) => {
                            PreparedScalarTerminalStrategy::ExistingRows { direction }
                        }
                    },
                ),
                prepared,
            },
            ScalarTerminalBoundaryRequest::Exists => PreparedScalarTerminalBoundary {
                op: PreparedScalarTerminalOp::Exists,
                strategy: derive_exists_terminal_fast_path_contract_for_model(
                    &prepared.logical_plan,
                    prepared.execution_preparation.strict_mode().is_some(),
                )
                .map_or(
                    PreparedScalarTerminalStrategy::KernelAggregate,
                    |contract| match contract {
                        ExistsTerminalFastPathContract::IndexCoveringExistingRows(direction) => {
                            PreparedScalarTerminalStrategy::ExistingRows { direction }
                        }
                    },
                ),
                prepared,
            },
            ScalarTerminalBoundaryRequest::IdTerminal { kind } => PreparedScalarTerminalBoundary {
                op: PreparedScalarTerminalOp::IdTerminal { kind },
                strategy: PreparedScalarTerminalStrategy::KernelAggregate,
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
            prepared,
        } = boundary;
        let aggregate_output =
            run_prepared_scalar_terminal_boundary(self, &op, strategy, prepared)?;

        match op {
            PreparedScalarTerminalOp::Count => aggregate_output
                .into_count("aggregate COUNT result kind mismatch")
                .map(ScalarTerminalBoundaryOutput::Count),
            PreparedScalarTerminalOp::Exists => aggregate_output
                .into_exists("aggregate EXISTS result kind mismatch")
                .map(ScalarTerminalBoundaryOutput::Exists),
            PreparedScalarTerminalOp::IdTerminal { kind }
            | PreparedScalarTerminalOp::IdBySlot { kind, .. } => aggregate_output
                .into_optional_id_terminal(kind, "aggregate id-terminal result kind mismatch")
                .map(ScalarTerminalBoundaryOutput::Id),
        }
    }
}

// Re-enter typed identity only at the terminal API boundary.
fn decode_storage_key_to_id<E>(key: StorageKey) -> Result<Id<E>, InternalError>
where
    E: EntityKind + EntityValue,
{
    let value = storage_key_as_runtime_value(&key);
    let decoded = <E::Key as KeyValueCodec>::from_key_value(&value).ok_or_else(|| {
        InternalError::store_corruption(format!(
            "scalar aggregate output primary key decode failed: {value:?}"
        ))
    })?;

    Ok(Id::from_key(decoded))
}

// Map one candidate cardinality and optional page contract to canonical COUNT
// result and scan accounting (`rows_scanned`) semantics.
fn count_window_result_from_page(page: Option<&PageSpec>, available_rows: usize) -> (u32, usize) {
    let Some(page) = page else {
        return (usize_to_u32_saturating(available_rows), available_rows);
    };
    let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);

    match page.limit {
        Some(0) => (0, 0),
        Some(limit) => {
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            let rows_scanned = available_rows.min(offset.saturating_add(limit));
            let count = available_rows.saturating_sub(offset).min(limit);

            (usize_to_u32_saturating(count), rows_scanned)
        }
        None => {
            let count = available_rows.saturating_sub(offset);
            (usize_to_u32_saturating(count), available_rows)
        }
    }
}

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}
