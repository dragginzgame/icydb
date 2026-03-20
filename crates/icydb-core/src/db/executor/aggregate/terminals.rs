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
            AccessScanContinuationInput, AccessStreamBindings, ExecutableAccess, ExecutablePlan,
            ExecutionKernel, ExecutionPreparation, StructuralTraversalRuntime,
            aggregate::{
                AggregateFoldMode, AggregateKind, PreparedAggregateStreamingInputs,
                PreparedAggregateStreamingInputsCore, PreparedScalarTerminalBoundary,
                PreparedScalarTerminalExecutionState, PreparedScalarTerminalOp,
                PreparedScalarTerminalStrategy, ScalarAggregateOutput,
                field::resolve_orderable_aggregate_target_slot_from_planner_slot,
            },
            pipeline::contracts::LoadExecutor,
            plan_metrics::record_rows_scanned_for_path,
            preparation::resolved_index_slots_for_access_path,
            route::{CountTerminalFastPathContract, ExistsTerminalFastPathContract},
        },
        index::predicate::IndexPredicateExecution,
        query::builder::aggregate::{field_target_extrema_expr_for_kind, terminal_expr_for_kind},
        query::plan::{FieldSlot as PlannedFieldSlot, PageSpec},
        registry::StoreHandle,
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::{EntityKind, EntityValue, FieldValue},
    types::{EntityTag, Id},
    value::StorageKey,
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
    model: &'static EntityModel,
    entity_tag: EntityTag,
    store: StoreHandle,
    logical_plan: &'a crate::db::query::plan::AccessPlannedQuery,
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
pub(in crate::db) enum ScalarTerminalBoundaryOutput<E: EntityKind + EntityValue> {
    Count(u32),
    Exists(bool),
    Id(Option<Id<E>>),
    IdPair(Option<(Id<E>, Id<E>)>),
}

impl<E> ScalarTerminalBoundaryOutput<E>
where
    E: EntityKind + EntityValue,
{
    // Decode COUNT boundary output while preserving request/output mismatch context.
    pub(in crate::db) fn into_count(self) -> Result<u32, InternalError> {
        match self {
            Self::Count(value) => Ok(value),
            _ => Err(crate::db::error::query_executor_invariant(
                "scalar terminal boundary COUNT output kind mismatch",
            )),
        }
    }

    // Decode EXISTS boundary output while preserving request/output mismatch context.
    pub(in crate::db) fn into_exists(self) -> Result<bool, InternalError> {
        match self {
            Self::Exists(value) => Ok(value),
            _ => Err(crate::db::error::query_executor_invariant(
                "scalar terminal boundary EXISTS output kind mismatch",
            )),
        }
    }

    // Decode id-returning boundary output while preserving request/output mismatch context.
    pub(in crate::db) fn into_id(self) -> Result<Option<Id<E>>, InternalError> {
        match self {
            Self::Id(value) => Ok(value),
            _ => Err(crate::db::error::query_executor_invariant(
                "scalar terminal boundary id output kind mismatch",
            )),
        }
    }

    // Decode paired-id boundary output while preserving request/output mismatch context.
    pub(in crate::db) fn into_id_pair(self) -> Result<IdPairTerminalOutput<E>, InternalError> {
        match self {
            Self::IdPair(value) => Ok(value),
            _ => Err(crate::db::error::query_executor_invariant(
                "scalar terminal boundary id-pair output kind mismatch",
            )),
        }
    }
}

// Return the canonical aggregate zero-output for one provably empty window.
fn aggregate_zero_output_if_window_empty_logical(
    prepared: &PreparedAggregateStreamingInputs<'_>,
    kind: AggregateKind,
) -> Option<ScalarAggregateOutput> {
    prepared
        .window_is_provably_empty()
        .then(|| kind.zero_output())
}

// Execute one prepared scalar terminal boundary through one shared
// zero-window, fast-path, and kernel dispatch boundary.
fn run_prepared_scalar_terminal_boundary<E>(
    executor: &LoadExecutor<E>,
    state: PreparedScalarTerminalExecutionState<'_>,
) -> Result<ScalarAggregateOutput, InternalError>
where
    E: EntityKind + EntityValue,
{
    let PreparedScalarTerminalExecutionState { boundary, prepared } = state;
    let kind = boundary.op.aggregate_kind();
    if let Some(aggregate_output) = aggregate_zero_output_if_window_empty_logical(&prepared, kind) {
        return Ok(aggregate_output);
    }

    match boundary.strategy {
        PreparedScalarTerminalStrategy::KernelAggregate => {
            execute_kernel_terminal_request(executor, prepared, boundary.op)
        }
        PreparedScalarTerminalStrategy::CountPrimaryKeyCardinality => {
            execute_count_primary_key_cardinality_terminal_request(prepared.into_core())
        }
        PreparedScalarTerminalStrategy::CountExistingRows {
            direction,
            covering,
        } => {
            execute_count_existing_rows_terminal_request(prepared.into_core(), direction, covering)
        }
        PreparedScalarTerminalStrategy::ExistsExistingRows { direction } => {
            execute_exists_existing_rows_terminal_request(prepared.into_core(), direction)
        }
    }
}

// Execute one kernel-owned scalar terminal request from prepared terminal metadata.
fn execute_kernel_terminal_request<E>(
    executor: &LoadExecutor<E>,
    prepared: PreparedAggregateStreamingInputs<'_>,
    op: PreparedScalarTerminalOp,
) -> Result<ScalarAggregateOutput, InternalError>
where
    E: EntityKind + EntityValue,
{
    let aggregate_expr = match op {
        PreparedScalarTerminalOp::Count => terminal_expr_for_kind(AggregateKind::Count),
        PreparedScalarTerminalOp::Exists => terminal_expr_for_kind(AggregateKind::Exists),
        PreparedScalarTerminalOp::IdTerminal { kind } => {
            if !matches!(
                kind,
                AggregateKind::Min
                    | AggregateKind::Max
                    | AggregateKind::First
                    | AggregateKind::Last
            ) {
                return Err(crate::db::error::query_executor_invariant(
                    "id terminal aggregate request requires MIN/MAX/FIRST/LAST kind",
                ));
            }

            terminal_expr_for_kind(kind)
        }
        PreparedScalarTerminalOp::IdBySlot {
            kind,
            target_field_name,
        } => {
            if !matches!(kind, AggregateKind::Min | AggregateKind::Max) {
                return Err(crate::db::error::query_executor_invariant(
                    "id-by-slot aggregate request requires MIN/MAX kind",
                ));
            }

            field_target_extrema_expr_for_kind(kind, &target_field_name)
        }
    };
    let state =
        ExecutionKernel::prepare_aggregate_execution_state_from_prepared(prepared, aggregate_expr);

    ExecutionKernel::execute_prepared_aggregate_state(executor, state)
}

// Execute prepared COUNT through store-cardinality fast-path semantics.
fn execute_count_primary_key_cardinality_terminal_request(
    prepared: PreparedAggregateStreamingInputsCore,
) -> Result<ScalarAggregateOutput, InternalError> {
    let (count, rows_scanned) = aggregate_count_from_pk_cardinality_with_store(
        &prepared.logical_plan,
        prepared.authority.entity_tag(),
        prepared.store,
    )?;
    record_rows_scanned_for_path(prepared.authority.entity_path(), rows_scanned);

    Ok(ScalarAggregateOutput::Count(count))
}

// Execute prepared COUNT through one streaming existing-row fold without
// materializing the effective window.
fn execute_count_existing_rows_terminal_request(
    prepared: PreparedAggregateStreamingInputsCore,
    direction: Direction,
    _covering: bool,
) -> Result<ScalarAggregateOutput, InternalError> {
    let count = expect_count_output(
        aggregate_existing_rows_terminal_output_with_runtime(
            ExistingRowsTerminalRuntime {
                model: prepared.authority.model(),
                entity_tag: prepared.authority.entity_tag(),
                store: prepared.store,
                logical_plan: &prepared.logical_plan,
                index_prefix_specs: prepared.index_prefix_specs.as_slice(),
                index_range_specs: prepared.index_range_specs.as_slice(),
            },
            AggregateKind::Count,
            direction,
        )
        .map(|(output, rows_scanned)| {
            record_rows_scanned_for_path(prepared.authority.entity_path(), rows_scanned);
            output
        })?,
        "existing-row COUNT reducer result kind mismatch",
    )?;

    Ok(ScalarAggregateOutput::Count(count))
}

// Execute prepared EXISTS through one streaming existing-row fold without
// materializing the effective window.
fn execute_exists_existing_rows_terminal_request(
    prepared: PreparedAggregateStreamingInputsCore,
    direction: Direction,
) -> Result<ScalarAggregateOutput, InternalError> {
    let exists = expect_exists_output(
        aggregate_existing_rows_terminal_output_with_runtime(
            ExistingRowsTerminalRuntime {
                model: prepared.authority.model(),
                entity_tag: prepared.authority.entity_tag(),
                store: prepared.store,
                logical_plan: &prepared.logical_plan,
                index_prefix_specs: prepared.index_prefix_specs.as_slice(),
                index_range_specs: prepared.index_range_specs.as_slice(),
            },
            AggregateKind::Exists,
            direction,
        )
        .map(|(output, rows_scanned)| {
            record_rows_scanned_for_path(prepared.authority.entity_path(), rows_scanned);
            output
        })?,
        "covering EXISTS reducer result kind mismatch",
    )?;

    Ok(ScalarAggregateOutput::Exists(exists))
}

// Resolve an index-backed existing-row key stream and execute one reducer kind.
fn aggregate_existing_rows_terminal_output_with_runtime(
    runtime: ExistingRowsTerminalRuntime<'_>,
    kind: AggregateKind,
    direction: Direction,
) -> Result<(ScalarAggregateOutput, usize), InternalError> {
    let ExistingRowsTerminalRuntime {
        model,
        entity_tag,
        store,
        logical_plan,
        index_prefix_specs,
        index_range_specs,
    } = runtime;

    // Phase 1: compile predicate/runtime inputs over the prepared logical plan.
    let execution_preparation = ExecutionPreparation::from_plan(
        model,
        logical_plan,
        resolved_index_slots_for_access_path(
            model,
            logical_plan.access.resolve_strategy().executable(),
        ),
    );
    let index_predicate_execution =
        execution_preparation
            .strict_mode()
            .map(|program| IndexPredicateExecution {
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
    let runtime = StructuralTraversalRuntime::new(store, entity_tag);
    let mut key_stream = runtime.ordered_key_stream_from_structural_runtime_access(access)?;

    // Phase 3: fold through existing-row semantics and record scan metrics.
    let (aggregate_output, rows_scanned) = ExecutionKernel::run_streaming_aggregate_reducer(
        store,
        logical_plan,
        kind,
        direction,
        AggregateFoldMode::ExistingRows,
        key_stream.as_mut(),
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
        return Err(crate::db::error::query_executor_invariant(
            "pk cardinality COUNT fast path requires single-path access strategy",
        ));
    };

    // Phase 2: read candidate-row cardinality directly from primary storage.
    let available_rows = match path.payload() {
        ExecutionPathPayload::FullScan => store.with_data(|data| {
            let store_len = data.len();

            usize::try_from(store_len).unwrap_or(usize::MAX)
        }),
        ExecutionPathPayload::KeyRange { start, end } => {
            let start_raw = DataKey::try_from_structural_key(entity_tag, start)?.to_raw()?;
            let end_raw = DataKey::try_from_structural_key(entity_tag, end)?.to_raw()?;

            store.with_data(|data| {
                data.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                    .count()
            })
        }
        _ => {
            return Err(crate::db::error::query_executor_invariant(
                "pk cardinality COUNT fast path requires full-scan or key-range access",
            ));
        }
    };

    // Phase 3: apply canonical COUNT window semantics and emit scan metrics.
    let (count, rows_scanned) = count_window_result_from_page(page, available_rows);

    Ok((count, rows_scanned))
}

// Decode COUNT outputs while preserving call-site mismatch context.
fn expect_count_output(
    aggregate_output: ScalarAggregateOutput,
    mismatch_context: &'static str,
) -> Result<u32, InternalError> {
    match aggregate_output {
        ScalarAggregateOutput::Count(value) => Ok(value),
        _ => Err(crate::db::error::query_executor_invariant(mismatch_context)),
    }
}

// Decode EXISTS outputs while preserving call-site mismatch context.
fn expect_exists_output(
    aggregate_output: ScalarAggregateOutput,
    mismatch_context: &'static str,
) -> Result<bool, InternalError> {
    match aggregate_output {
        ScalarAggregateOutput::Exists(value) => Ok(value),
        _ => Err(crate::db::error::query_executor_invariant(mismatch_context)),
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one scalar aggregate terminal family request from the typed API
    // boundary, lower plan-derived policy into one prepared terminal contract,
    // and then execute that prepared contract.
    pub(in crate::db) fn execute_scalar_terminal_request(
        &self,
        plan: ExecutablePlan<E>,
        request: ScalarTerminalBoundaryRequest,
    ) -> Result<ScalarTerminalBoundaryOutput<E>, InternalError> {
        match request {
            ScalarTerminalBoundaryRequest::Count
            | ScalarTerminalBoundaryRequest::Exists
            | ScalarTerminalBoundaryRequest::IdTerminal { .. }
            | ScalarTerminalBoundaryRequest::IdBySlot { .. } => {
                let prepared = self.prepare_scalar_terminal_boundary(plan, request)?;

                self.execute_prepared_scalar_terminal_boundary(prepared)
            }
            ScalarTerminalBoundaryRequest::NthBySlot { target_field, nth } => {
                let field_slot =
                    resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                        .map_err(Self::map_aggregate_field_value_error)?;
                let prepared = self.prepare_scalar_aggregate_boundary(plan)?;

                self.execute_nth_field_aggregate_with_slot(
                    prepared,
                    target_field.field(),
                    field_slot,
                    nth,
                )
                .map(ScalarTerminalBoundaryOutput::Id)
            }
            ScalarTerminalBoundaryRequest::MedianBySlot { target_field } => {
                let field_slot =
                    resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                        .map_err(Self::map_aggregate_field_value_error)?;
                let prepared = self.prepare_scalar_aggregate_boundary(plan)?;

                self.execute_median_field_aggregate_with_slot(
                    prepared,
                    target_field.field(),
                    field_slot,
                )
                .map(ScalarTerminalBoundaryOutput::Id)
            }
            ScalarTerminalBoundaryRequest::MinMaxBySlot { target_field } => {
                let field_slot =
                    resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                        .map_err(Self::map_aggregate_field_value_error)?;
                let prepared = self.prepare_scalar_aggregate_boundary(plan)?;

                self.execute_min_max_field_aggregate_with_slot(
                    prepared,
                    target_field.field(),
                    field_slot,
                )
                .map(ScalarTerminalBoundaryOutput::IdPair)
            }
        }
    }

    // Lower one public scalar terminal request into one prepared terminal
    // boundary so execution no longer derives fast-path policy from the plan.
    fn prepare_scalar_terminal_boundary(
        &self,
        plan: ExecutablePlan<E>,
        request: ScalarTerminalBoundaryRequest,
    ) -> Result<PreparedScalarTerminalExecutionState<'_>, InternalError> {
        let boundary = match request {
            ScalarTerminalBoundaryRequest::Count => PreparedScalarTerminalBoundary {
                op: PreparedScalarTerminalOp::Count,
                strategy: Self::prepare_scalar_count_terminal_strategy(&plan),
            },
            ScalarTerminalBoundaryRequest::Exists => PreparedScalarTerminalBoundary {
                op: PreparedScalarTerminalOp::Exists,
                strategy: Self::prepare_scalar_exists_terminal_strategy(&plan),
            },
            ScalarTerminalBoundaryRequest::IdTerminal { kind } => PreparedScalarTerminalBoundary {
                op: PreparedScalarTerminalOp::IdTerminal { kind },
                strategy: PreparedScalarTerminalStrategy::KernelAggregate,
            },
            ScalarTerminalBoundaryRequest::IdBySlot { kind, target_field } => {
                resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                    .map_err(Self::map_aggregate_field_value_error)?;

                PreparedScalarTerminalBoundary {
                    op: PreparedScalarTerminalOp::IdBySlot {
                        kind,
                        target_field_name: target_field.field().to_string(),
                    },
                    strategy: PreparedScalarTerminalStrategy::KernelAggregate,
                }
            }
            ScalarTerminalBoundaryRequest::NthBySlot { .. }
            | ScalarTerminalBoundaryRequest::MedianBySlot { .. }
            | ScalarTerminalBoundaryRequest::MinMaxBySlot { .. } => {
                return Err(crate::db::error::query_executor_invariant(
                    "prepared scalar terminal boundary only supports COUNT/EXISTS/id terminals",
                ));
            }
        };
        let prepared = self.prepare_scalar_aggregate_boundary(plan)?;

        Ok(PreparedScalarTerminalExecutionState { boundary, prepared })
    }

    // Execute one prepared scalar terminal contract without consulting plan-owned
    // fast-path policy.
    fn execute_prepared_scalar_terminal_boundary(
        &self,
        prepared: PreparedScalarTerminalExecutionState<'_>,
    ) -> Result<ScalarTerminalBoundaryOutput<E>, InternalError> {
        let boundary = prepared.boundary.clone();
        let aggregate_output = run_prepared_scalar_terminal_boundary(self, prepared)?;

        match boundary.op {
            PreparedScalarTerminalOp::Count => {
                expect_count_output(aggregate_output, "aggregate COUNT result kind mismatch")
                    .map(ScalarTerminalBoundaryOutput::Count)
            }
            PreparedScalarTerminalOp::Exists => {
                expect_exists_output(aggregate_output, "aggregate EXISTS result kind mismatch")
                    .map(ScalarTerminalBoundaryOutput::Exists)
            }
            PreparedScalarTerminalOp::IdTerminal { kind }
            | PreparedScalarTerminalOp::IdBySlot { kind, .. } => {
                Self::expect_optional_id_terminal_output(
                    aggregate_output,
                    kind,
                    "aggregate id-terminal result kind mismatch",
                )
                .map(ScalarTerminalBoundaryOutput::Id)
            }
        }
    }

    // Resolve one prepared COUNT strategy from one typed executable plan.
    fn prepare_scalar_count_terminal_strategy(
        plan: &ExecutablePlan<E>,
    ) -> PreparedScalarTerminalStrategy {
        crate::db::executor::route::derive_count_terminal_fast_path_contract_for_model(
            E::MODEL,
            plan.logical_plan(),
            plan.execution_preparation().strict_mode().is_some(),
        )
        .map_or(
            PreparedScalarTerminalStrategy::KernelAggregate,
            |contract| match contract {
                CountTerminalFastPathContract::PrimaryKeyCardinality => {
                    PreparedScalarTerminalStrategy::CountPrimaryKeyCardinality
                }
                CountTerminalFastPathContract::PrimaryKeyExistingRows(direction) => {
                    PreparedScalarTerminalStrategy::CountExistingRows {
                        direction,
                        covering: false,
                    }
                }
                CountTerminalFastPathContract::IndexCoveringExistingRows(direction) => {
                    PreparedScalarTerminalStrategy::CountExistingRows {
                        direction,
                        covering: true,
                    }
                }
            },
        )
    }

    // Resolve one prepared EXISTS strategy from one typed executable plan.
    fn prepare_scalar_exists_terminal_strategy(
        plan: &ExecutablePlan<E>,
    ) -> PreparedScalarTerminalStrategy {
        crate::db::executor::route::derive_exists_terminal_fast_path_contract_for_model(
            plan.logical_plan(),
            plan.execution_preparation().strict_mode().is_some(),
        )
        .map_or(
            PreparedScalarTerminalStrategy::KernelAggregate,
            |contract| match contract {
                ExistsTerminalFastPathContract::IndexCoveringExistingRows(direction) => {
                    PreparedScalarTerminalStrategy::ExistsExistingRows { direction }
                }
            },
        )
    }

    // Decode id-returning aggregate outputs for MIN/MAX/FIRST/LAST terminals.
    fn expect_optional_id_terminal_output(
        aggregate_output: ScalarAggregateOutput,
        kind: AggregateKind,
        mismatch_context: &'static str,
    ) -> Result<Option<Id<E>>, InternalError> {
        match (kind, aggregate_output) {
            (AggregateKind::Min, ScalarAggregateOutput::Min(value))
            | (AggregateKind::Max, ScalarAggregateOutput::Max(value))
            | (AggregateKind::First, ScalarAggregateOutput::First(value))
            | (AggregateKind::Last, ScalarAggregateOutput::Last(value)) => {
                value.map(Self::decode_storage_key_to_id).transpose()
            }
            _ => Err(crate::db::error::query_executor_invariant(mismatch_context)),
        }
    }

    // Re-enter typed identity only at the terminal API boundary.
    fn decode_storage_key_to_id(key: StorageKey) -> Result<Id<E>, InternalError> {
        let value = key.as_value();
        let decoded = <E::Key as FieldValue>::from_value(&value).ok_or_else(|| {
            InternalError::store_corruption(format!(
                "scalar aggregate output primary key decode failed: {value:?}"
            ))
        })?;

        Ok(Id::from_key(decoded))
    }
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
