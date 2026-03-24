//! Module: executor::aggregate::projection
//! Responsibility: field-value projection terminals over materialized responses.
//! Does not own: grouped key canonicalization internals or route planning logic.
//! Boundary: projection terminal helpers (`values`, `distinct_values`, `first/last value`).
//!
//! `distinct_values_by(field)` here is a non-grouped effective-window helper.
//! Grouped Class B DISTINCT accounting is enforced only through grouped
//! execution context boundaries.

mod covering;
mod decode;

use crate::{
    db::{
        cursor::IndexScanContinuationInput,
        data::{DataKey, DataRow},
        direction::Direction,
        executor::{
            ExecutablePlan, ExecutionKernel, PreparedAggregatePlan,
            aggregate::{
                AggregateKind, PreparedAggregateStreamingInputs, PreparedCoveringDistinctStrategy,
                PreparedScalarProjectionExecutionState, PreparedScalarProjectionOp,
                PreparedScalarProjectionStrategy, ScalarAggregateOutput, ScalarProjectionWindow,
                field::{
                    AggregateFieldValueError, FieldSlot,
                    extract_orderable_field_value_with_slot_reader,
                    resolve_any_aggregate_target_slot_from_planner_slot_with_model,
                },
                materialized_distinct::insert_materialized_distinct_value,
                projection::{
                    covering::{
                        CoveringProjectionValues, covering_index_adjacent_distinct_eligible,
                        covering_index_projection_context, dedup_adjacent_values,
                        dedup_values_preserving_first, scalar_window_for_covering_projection,
                    },
                    decode::decode_covering_projection_component,
                },
            },
            group::GroupKeySet,
            pipeline::contracts::LoadExecutor,
            read_row_with_consistency_from_store,
            terminal::{RowDecoder, RowLayout},
        },
        predicate::MissingRowPolicy,
        query::{
            builder::aggregate::terminal_expr_for_kind,
            plan::{
                CoveringProjectionContext, CoveringProjectionOrder, FieldSlot as PlannedFieldSlot,
                constant_covering_projection_value_from_access,
            },
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
    value::Value,
};

type StructuralValueProjection = Vec<(DataKey, Value)>;
type CoveringProjectionPairRows = Vec<(DataKey, Value)>;
type CoveringProjectionPairsResolution = Result<Option<CoveringProjectionPairRows>, InternalError>;
type CoveringProjectionComponentRows = Vec<(DataKey, Vec<u8>)>;

// Typed boundary request for one scalar field-projection terminal family call.
pub(in crate::db) enum ScalarProjectionBoundaryRequest {
    Values,
    DistinctValues,
    CountDistinct,
    ValuesWithIds,
    TerminalValue { terminal_kind: AggregateKind },
}

// Typed boundary output for one scalar field-projection terminal family call.
pub(in crate::db) enum ScalarProjectionBoundaryOutput {
    Count(u32),
    Values(Vec<Value>),
    ValuesWithDataKeys(StructuralValueProjection),
    TerminalValue(Option<Value>),
}

impl ScalarProjectionBoundaryOutput {
    // Decode one plain-value projection boundary output.
    pub(in crate::db) fn into_values(self) -> Result<Vec<Value>, InternalError> {
        match self {
            Self::Values(values) => Ok(values),
            _ => Err(InternalError::query_executor_invariant(
                "scalar projection boundary values output kind mismatch",
            )),
        }
    }

    // Decode one count-distinct projection boundary output.
    pub(in crate::db) fn into_count(self) -> Result<u32, InternalError> {
        match self {
            Self::Count(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "scalar projection boundary count output kind mismatch",
            )),
        }
    }

    // Decode one `(id, value)` projection boundary output.
    pub(in crate::db) fn into_values_with_ids<E>(self) -> Result<Vec<(Id<E>, Value)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match self {
            Self::ValuesWithDataKeys(values) => values
                .into_iter()
                .map(|(data_key, value)| Ok((Id::from_key(data_key.try_key::<E>()?), value)))
                .collect(),
            _ => Err(InternalError::query_executor_invariant(
                "scalar projection boundary values-with-ids output kind mismatch",
            )),
        }
    }

    // Decode one terminal-value projection boundary output.
    pub(in crate::db) fn into_terminal_value(self) -> Result<Option<Value>, InternalError> {
        match self {
            Self::TerminalValue(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "scalar projection boundary terminal-value output kind mismatch",
            )),
        }
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one scalar field-projection terminal family request from the
    // typed API boundary, lower plan-derived policy into one prepared
    // projection contract, and then execute that contract.
    pub(in crate::db) fn execute_scalar_projection_boundary(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        request: ScalarProjectionBoundaryRequest,
    ) -> Result<ScalarProjectionBoundaryOutput, InternalError> {
        let prepared = self.prepare_scalar_projection_boundary(
            plan.into_prepared_aggregate_plan(),
            target_field,
            request,
        )?;

        self.execute_prepared_scalar_projection_boundary(prepared)
    }

    // Lower one public scalar field-projection request into one prepared
    // projection contract that no longer retains `ExecutablePlan<E>`.
    fn prepare_scalar_projection_boundary(
        &self,
        plan: PreparedAggregatePlan,
        target_field: PlannedFieldSlot,
        request: ScalarProjectionBoundaryRequest,
    ) -> Result<PreparedScalarProjectionExecutionState<'_>, InternalError> {
        let target_field_name = target_field.field().to_string();
        let authority = plan.authority();
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot_with_model(
            authority.model(),
            &target_field,
        )
        .map_err(AggregateFieldValueError::into_internal_error)?;
        let prepared = self.prepare_scalar_aggregate_boundary(plan)?;

        let op = PreparedScalarProjectionOp::from_request(request);
        if let PreparedScalarProjectionOp::TerminalValue { terminal_kind } = op
            && !matches!(terminal_kind, AggregateKind::First | AggregateKind::Last)
        {
            return Err(InternalError::query_executor_invariant(
                "terminal value projection requires FIRST/LAST aggregate kind",
            ));
        }

        let strategy = Self::prepare_scalar_projection_strategy(&prepared, &target_field_name, op);

        Ok(PreparedScalarProjectionExecutionState {
            boundary: crate::db::executor::aggregate::PreparedScalarProjectionBoundary {
                target_field_name,
                field_slot,
                op,
                strategy,
            },
            prepared,
        })
    }

    // Execute one prepared field-projection contract without re-reading
    // access-path, covering, or distinct policy from the original plan.
    fn execute_prepared_scalar_projection_boundary(
        &self,
        prepared_state: PreparedScalarProjectionExecutionState<'_>,
    ) -> Result<ScalarProjectionBoundaryOutput, InternalError> {
        let PreparedScalarProjectionExecutionState { boundary, prepared } = prepared_state;
        let row_layout = RowLayout::from_model(prepared.authority.model());

        match boundary.strategy.clone() {
            PreparedScalarProjectionStrategy::Materialized => self
                .execute_materialized_scalar_projection_boundary(boundary, prepared, &row_layout),
            PreparedScalarProjectionStrategy::CoveringIndex {
                context,
                window,
                distinct,
            } => self.execute_covering_scalar_projection_boundary(
                boundary, prepared, context, window, distinct,
            ),
            PreparedScalarProjectionStrategy::CoveringConstant { value } => {
                self.execute_constant_scalar_projection_boundary(boundary, prepared, value)
            }
        }
    }

    // Resolve one non-generic execution strategy for the prepared projection
    // contract before runtime execution begins.
    fn prepare_scalar_projection_strategy(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
        op: PreparedScalarProjectionOp,
    ) -> PreparedScalarProjectionStrategy {
        if !prepared.has_predicate()
            && let Some(context) = covering_index_projection_context(
                &prepared.logical_plan.access,
                prepared.order_spec(),
                target_field,
                prepared.authority.model().primary_key.name,
            )
        {
            let window = ScalarProjectionWindow {
                offset: scalar_window_for_covering_projection(prepared.page_spec()).0,
                limit: scalar_window_for_covering_projection(prepared.page_spec()).1,
            };
            let distinct = match op {
                PreparedScalarProjectionOp::DistinctValues
                | PreparedScalarProjectionOp::CountDistinct => {
                    Some(if covering_index_adjacent_distinct_eligible(context) {
                        PreparedCoveringDistinctStrategy::Adjacent
                    } else {
                        PreparedCoveringDistinctStrategy::PreserveFirst
                    })
                }
                _ => None,
            };

            return PreparedScalarProjectionStrategy::CoveringIndex {
                context,
                window,
                distinct,
            };
        }

        match op {
            PreparedScalarProjectionOp::Values
            | PreparedScalarProjectionOp::DistinctValues
            | PreparedScalarProjectionOp::CountDistinct
            | PreparedScalarProjectionOp::TerminalValue { .. } => {
                if let Some(value) =
                    Self::constant_covering_projection_value_if_eligible(prepared, target_field)
                {
                    return PreparedScalarProjectionStrategy::CoveringConstant { value };
                }
            }
            PreparedScalarProjectionOp::ValuesWithIds => {}
        }

        PreparedScalarProjectionStrategy::Materialized
    }

    // Execute one prepared covering-index projection contract. Decode failures
    // that prove the covering payload is unusable fall back to the canonical
    // materialized boundary without re-deriving strategy from the plan.
    fn execute_covering_scalar_projection_boundary(
        &self,
        boundary: crate::db::executor::aggregate::PreparedScalarProjectionBoundary,
        prepared: PreparedAggregateStreamingInputs<'_>,
        context: CoveringProjectionContext,
        window: ScalarProjectionWindow,
        distinct: Option<PreparedCoveringDistinctStrategy>,
    ) -> Result<ScalarProjectionBoundaryOutput, InternalError> {
        match boundary.op {
            PreparedScalarProjectionOp::Values => {
                if let Some(covering_projection) =
                    Self::covering_index_projection_values_with_context_from_prepared(
                        &prepared, context, window,
                    )?
                {
                    return Ok(ScalarProjectionBoundaryOutput::Values(
                        covering_projection.values,
                    ));
                }
            }
            PreparedScalarProjectionOp::DistinctValues => {
                if let Some(covering_projection) =
                    Self::covering_index_projection_values_with_context_from_prepared(
                        &prepared, context, window,
                    )?
                {
                    let values = match distinct {
                        Some(PreparedCoveringDistinctStrategy::Adjacent) => {
                            dedup_adjacent_values(covering_projection.values)
                        }
                        Some(PreparedCoveringDistinctStrategy::PreserveFirst) => {
                            dedup_values_preserving_first(covering_projection.values)?
                        }
                        None => {
                            return Err(InternalError::query_executor_invariant(
                                "covering DISTINCT projection requires prepared distinct strategy",
                            ));
                        }
                    };

                    return Ok(ScalarProjectionBoundaryOutput::Values(values));
                }
            }
            PreparedScalarProjectionOp::CountDistinct => {
                if let Some(covering_projection) =
                    Self::covering_index_projection_values_with_context_from_prepared(
                        &prepared, context, window,
                    )?
                {
                    let values = match distinct {
                        Some(PreparedCoveringDistinctStrategy::Adjacent) => {
                            dedup_adjacent_values(covering_projection.values)
                        }
                        Some(PreparedCoveringDistinctStrategy::PreserveFirst) => {
                            dedup_values_preserving_first(covering_projection.values)?
                        }
                        None => {
                            return Err(InternalError::query_executor_invariant(
                                "covering COUNT DISTINCT projection requires prepared distinct strategy",
                            ));
                        }
                    };

                    return Ok(ScalarProjectionBoundaryOutput::Count(
                        u32::try_from(values.len()).unwrap_or(u32::MAX),
                    ));
                }
            }
            PreparedScalarProjectionOp::ValuesWithIds => {
                if let Some(values) =
                    Self::covering_index_projection_values_from_context_structural(
                        &prepared, context, window,
                    )?
                {
                    return Ok(ScalarProjectionBoundaryOutput::ValuesWithDataKeys(values));
                }
            }
            PreparedScalarProjectionOp::TerminalValue { terminal_kind } => {
                if let Some(covering_projection) =
                    Self::covering_index_projection_values_with_context_from_prepared(
                        &prepared, context, window,
                    )?
                {
                    let value = match terminal_kind {
                        AggregateKind::First => covering_projection.values.first().cloned(),
                        AggregateKind::Last => covering_projection.values.last().cloned(),
                        _ => {
                            return Err(InternalError::query_executor_invariant(
                                "covering terminal value projection requires FIRST/LAST aggregate kind",
                            ));
                        }
                    };

                    return Ok(ScalarProjectionBoundaryOutput::TerminalValue(value));
                }
            }
        }

        let row_layout = RowLayout::from_model(prepared.authority.model());

        self.execute_materialized_scalar_projection_boundary(boundary, prepared, &row_layout)
    }

    // Execute one prepared constant projection contract without revisiting
    // covering eligibility checks.
    fn execute_constant_scalar_projection_boundary(
        &self,
        boundary: crate::db::executor::aggregate::PreparedScalarProjectionBoundary,
        prepared: PreparedAggregateStreamingInputs<'_>,
        value: Value,
    ) -> Result<ScalarProjectionBoundaryOutput, InternalError> {
        match boundary.op {
            PreparedScalarProjectionOp::Values => {
                let row_count = self.aggregate_count_from_prepared(prepared)?;
                let output_len = usize::try_from(row_count).unwrap_or(usize::MAX);

                Ok(ScalarProjectionBoundaryOutput::Values(vec![
                    value;
                    output_len
                ]))
            }
            PreparedScalarProjectionOp::DistinctValues => {
                let has_rows = self.aggregate_exists_from_prepared(prepared)?;
                Ok(ScalarProjectionBoundaryOutput::Values(if has_rows {
                    vec![value]
                } else {
                    Vec::new()
                }))
            }
            PreparedScalarProjectionOp::CountDistinct => {
                let has_rows = self.aggregate_exists_from_prepared(prepared)?;
                Ok(ScalarProjectionBoundaryOutput::Count(u32::from(has_rows)))
            }
            PreparedScalarProjectionOp::TerminalValue { .. } => {
                let has_rows = self.aggregate_exists_from_prepared(prepared)?;
                Ok(ScalarProjectionBoundaryOutput::TerminalValue(
                    has_rows.then_some(value),
                ))
            }
            PreparedScalarProjectionOp::ValuesWithIds => {
                Err(InternalError::query_executor_invariant(
                    "values-with-ids projection cannot execute constant covering strategy",
                ))
            }
        }
    }

    // Execute one prepared materialized projection contract.
    fn execute_materialized_scalar_projection_boundary(
        &self,
        boundary: crate::db::executor::aggregate::PreparedScalarProjectionBoundary,
        prepared: PreparedAggregateStreamingInputs<'_>,
        row_layout: &RowLayout,
    ) -> Result<ScalarProjectionBoundaryOutput, InternalError> {
        if let PreparedScalarProjectionOp::TerminalValue { terminal_kind } = boundary.op {
            return self
                .execute_terminal_value_field_projection_with_slot(
                    prepared,
                    &boundary.target_field_name,
                    boundary.field_slot,
                    terminal_kind,
                )
                .map(ScalarProjectionBoundaryOutput::TerminalValue);
        }

        let page = self.execute_scalar_materialized_page_stage(prepared)?;
        let (rows, _) = page.into_parts();

        match boundary.op {
            PreparedScalarProjectionOp::Values => {
                let projected_values = Self::project_field_values_from_materialized_structural(
                    rows,
                    row_layout,
                    &boundary.target_field_name,
                    boundary.field_slot,
                )?;

                Ok(ScalarProjectionBoundaryOutput::Values(
                    Self::field_values_from_projection(projected_values),
                ))
            }
            PreparedScalarProjectionOp::DistinctValues => {
                Self::project_field_values_from_materialized_structural(
                    rows,
                    row_layout,
                    &boundary.target_field_name,
                    boundary.field_slot,
                )
                .and_then(Self::project_distinct_field_values_from_materialized)
                .map(ScalarProjectionBoundaryOutput::Values)
            }
            PreparedScalarProjectionOp::CountDistinct => {
                Self::project_field_values_from_materialized_structural(
                    rows,
                    row_layout,
                    &boundary.target_field_name,
                    boundary.field_slot,
                )
                .and_then(Self::project_distinct_field_values_from_materialized)
                .map(|values| {
                    ScalarProjectionBoundaryOutput::Count(
                        u32::try_from(values.len()).unwrap_or(u32::MAX),
                    )
                })
            }
            PreparedScalarProjectionOp::ValuesWithIds => {
                Self::project_field_values_from_materialized_structural(
                    rows,
                    row_layout,
                    &boundary.target_field_name,
                    boundary.field_slot,
                )
                .map(ScalarProjectionBoundaryOutput::ValuesWithDataKeys)
            }
            PreparedScalarProjectionOp::TerminalValue { .. } => {
                Err(InternalError::query_executor_invariant(
                    "terminal value projection materialized branch must execute before row materialization",
                ))
            }
        }
    }

    // Execute one field-target scalar terminal projection (`first_value_by` /
    // `last_value_by`) using a planner-validated slot and route-owned
    // first/last row selection semantics.
    fn execute_terminal_value_field_projection_with_slot(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
        field_slot: FieldSlot,
        terminal_kind: AggregateKind,
    ) -> Result<Option<Value>, InternalError> {
        let consistency = prepared.consistency();
        let store = prepared.store;
        let entity_tag = prepared.authority.entity_tag();
        let row_layout = RowLayout::from_model(prepared.authority.model());
        let state = ExecutionKernel::prepare_aggregate_execution_state_from_prepared(
            prepared,
            terminal_expr_for_kind(terminal_kind),
        );
        let (ScalarAggregateOutput::First(selected_key)
        | ScalarAggregateOutput::Last(selected_key)) =
            ExecutionKernel::execute_prepared_aggregate_state(self, state)?
        else {
            return Err(InternalError::query_executor_invariant(
                "terminal value projection result kind mismatch",
            ));
        };
        let Some(selected_key) = selected_key else {
            return Ok(None);
        };

        let key = DataKey::new(entity_tag, selected_key);
        let Some(value) = Self::read_field_value_for_aggregate(
            store,
            &row_layout,
            consistency,
            &key,
            target_field,
            field_slot,
        )?
        else {
            return Ok(None);
        };

        Ok(Some(value))
    }

    // Project one materialized `(id, value)` vector into one field value vector while
    // preserving the effective response row order.
    fn field_values_from_projection(projected_values: StructuralValueProjection) -> Vec<Value> {
        projected_values
            .into_iter()
            .map(|(_, value)| value)
            .collect()
    }

    // Project one materialized `(id, value)` vector into distinct field values while
    // preserving first-observed order within the effective response window.
    // This is value DISTINCT semantics via canonical `GroupKey` equality.
    fn project_distinct_field_values_from_materialized(
        projected_values: StructuralValueProjection,
    ) -> Result<Vec<Value>, InternalError> {
        project_distinct_field_values_from_structural_projection(projected_values)
    }

    // Project materialized structural rows into structural `(data_key, value)`
    // pairs while preserving the effective response row order.
    fn project_field_values_from_materialized_structural(
        rows: Vec<DataRow>,
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<StructuralValueProjection, InternalError> {
        let row_decoder = RowDecoder::structural();

        rows.into_iter()
            .map(|(data_key, raw_row)| {
                let row = row_decoder.decode(row_layout, (data_key.clone(), raw_row))?;
                extract_orderable_field_value_with_slot_reader(
                    target_field,
                    field_slot,
                    &mut |index| row.slot(index),
                )
                .map(|value| (data_key, value))
                .map_err(AggregateFieldValueError::into_internal_error)
            })
            .collect()
    }

    // Resolve one constant field projection value when access shape guarantees
    // that target-field value is fixed by index-prefix equality bindings.
    //
    // Guard rails:
    // - only enabled for `MissingRowPolicy::Ignore` to preserve strict
    //   missing-row corruption surfacing behavior.
    // - only applies when the target field is bound by index-prefix equality.
    fn constant_covering_projection_value_if_eligible(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
    ) -> Option<Value> {
        if !matches!(prepared.consistency(), MissingRowPolicy::Ignore) {
            return None;
        }

        constant_covering_projection_value_from_access(&prepared.logical_plan.access, target_field)
    }

    // Resolve one index-covered projection value vector from already-prepared
    // covering strategy metadata.
    fn covering_index_projection_values_with_context_from_prepared(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        context: CoveringProjectionContext,
        window: ScalarProjectionWindow,
    ) -> Result<Option<CoveringProjectionValues>, InternalError> {
        let Some(projected_pairs) =
            Self::covering_index_projection_pairs_from_context(prepared, context, window)?
        else {
            return Ok(None);
        };

        let values = projected_pairs
            .into_iter()
            .map(|(_, value)| value)
            .collect();

        Ok(Some(CoveringProjectionValues { values }))
    }

    // Resolve one index-covered structural `(data_key, value)` projection
    // vector from already prepared covering strategy metadata.
    fn covering_index_projection_values_from_context_structural(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        context: CoveringProjectionContext,
        window: ScalarProjectionWindow,
    ) -> Result<Option<StructuralValueProjection>, InternalError> {
        Self::covering_index_projection_pairs_from_context(prepared, context, window)
    }

    // Resolve one covering projection pair vector from already prepared
    // covering-index strategy metadata.
    fn covering_index_projection_pairs_from_context(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        context: CoveringProjectionContext,
        window: ScalarProjectionWindow,
    ) -> CoveringProjectionPairsResolution {
        // Phase 1: read component pairs in the order implied by the covering contract.
        let scan_direction = match context.order_contract {
            CoveringProjectionOrder::IndexOrder(direction) => direction,
            CoveringProjectionOrder::PrimaryKeyOrder(_) => Direction::Asc,
        };
        let raw_pairs = Self::read_covering_projection_component_pairs(
            prepared,
            context.component_index,
            scan_direction,
        )?;

        // Phase 2: enforce missing-row policy and decode projection components.
        let mut projected_pairs = Vec::with_capacity(raw_pairs.len());
        for (data_key, component_bytes) in raw_pairs {
            if read_row_with_consistency_from_store(
                prepared.store,
                &data_key,
                prepared.consistency(),
            )?
            .is_none()
            {
                continue;
            }

            let Some(value) = decode_covering_projection_component(&component_bytes)? else {
                return Ok(None);
            };
            projected_pairs.push((data_key, value));
        }

        // Phase 3: realign to post-access order and apply prepared effective window.
        match context.order_contract {
            CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc) => {
                projected_pairs.sort_by(|left, right| left.0.cmp(&right.0));
            }
            CoveringProjectionOrder::PrimaryKeyOrder(Direction::Desc) => {
                projected_pairs.sort_by(|left, right| right.0.cmp(&left.0));
            }
            CoveringProjectionOrder::IndexOrder(Direction::Asc | Direction::Desc) => {}
        }

        let windowed_pairs = match window.limit {
            Some(limit) => projected_pairs
                .into_iter()
                .skip(window.offset)
                .take(limit)
                .collect(),
            None => projected_pairs.into_iter().skip(window.offset).collect(),
        };

        Ok(Some(windowed_pairs))
    }

    // Read one index-backed `(data_key, encoded_component)` stream for covering
    // projection decoding.
    fn read_covering_projection_component_pairs(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        component_index: usize,
        direction: Direction,
    ) -> Result<CoveringProjectionComponentRows, InternalError> {
        let continuation = IndexScanContinuationInput::new(None, direction);

        let prefix_specs = prepared.index_prefix_specs.as_slice();
        if let [spec] = prefix_specs {
            return Self::read_covering_projection_component_pairs_for_index_bounds(
                prepared.store_resolver,
                prepared.authority.entity_tag(),
                spec.index(),
                (spec.lower(), spec.upper()),
                continuation,
                component_index,
            );
        }
        if !prefix_specs.is_empty() {
            return Err(InternalError::query_executor_invariant(
                "covering projection index-prefix path requires one lowered prefix spec",
            ));
        }

        let range_specs = prepared.index_range_specs.as_slice();
        if let [spec] = range_specs {
            return Self::read_covering_projection_component_pairs_for_index_bounds(
                prepared.store_resolver,
                prepared.authority.entity_tag(),
                spec.index(),
                (spec.lower(), spec.upper()),
                continuation,
                component_index,
            );
        }
        if !range_specs.is_empty() {
            return Err(InternalError::query_executor_invariant(
                "covering projection index-range path requires one lowered range spec",
            ));
        }

        Err(InternalError::query_executor_invariant(
            "covering projection component scans require index-backed access paths",
        ))
    }

    // Execute COUNT from one prepared aggregate stage so constant projection
    // fast paths do not re-enter the plan-owned terminal wrapper surface.
    fn aggregate_count_from_prepared(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
    ) -> Result<u32, InternalError> {
        let state = ExecutionKernel::prepare_aggregate_execution_state_from_prepared(
            prepared,
            terminal_expr_for_kind(AggregateKind::Count),
        );
        match ExecutionKernel::execute_prepared_aggregate_state(self, state)? {
            ScalarAggregateOutput::Count(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "projection COUNT helper result kind mismatch",
            )),
        }
    }

    // Execute EXISTS from one prepared aggregate stage so constant projection
    // fast paths do not re-enter the plan-owned terminal wrapper surface.
    fn aggregate_exists_from_prepared(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
    ) -> Result<bool, InternalError> {
        let state = ExecutionKernel::prepare_aggregate_execution_state_from_prepared(
            prepared,
            terminal_expr_for_kind(AggregateKind::Exists),
        );
        match ExecutionKernel::execute_prepared_aggregate_state(self, state)? {
            ScalarAggregateOutput::Exists(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "projection EXISTS helper result kind mismatch",
            )),
        }
    }

    // Resolve one covering projection component stream for one lowered
    // index-prefix/index-range bounds contract.
    fn read_covering_projection_component_pairs_for_index_bounds(
        store_resolver: crate::db::executor::StructuralStoreResolver<'_>,
        entity_tag: crate::types::EntityTag,
        index: &crate::model::index::IndexModel,
        bounds: (
            &std::ops::Bound<crate::db::index::RawIndexKey>,
            &std::ops::Bound<crate::db::index::RawIndexKey>,
        ),
        continuation: IndexScanContinuationInput,
        component_index: usize,
    ) -> Result<CoveringProjectionComponentRows, InternalError> {
        let store = store_resolver.try_get_store(index.store())?;
        store.with_index(|index_store| {
            index_store.resolve_data_values_with_component_in_raw_range_limited(
                entity_tag,
                index,
                bounds,
                continuation,
                usize::MAX,
                component_index,
                None,
            )
        })
    }
}

fn project_distinct_field_values_from_structural_projection(
    projected_values: StructuralValueProjection,
) -> Result<Vec<Value>, InternalError> {
    let mut distinct_values = GroupKeySet::default();
    let mut distinct_projected_values = Vec::new();

    // Phase 1: preserve first-observed order while deduplicating on canonical
    // group-key equality over structural projection values.
    for (_, value) in projected_values {
        if !insert_materialized_distinct_value(&mut distinct_values, &value)? {
            continue;
        }
        distinct_projected_values.push(value);
    }

    Ok(distinct_projected_values)
}
