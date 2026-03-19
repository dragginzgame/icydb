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
        data::DataKey,
        direction::Direction,
        executor::{
            ExecutablePlan, ExecutionKernel, ExecutionOptimizationCounter,
            aggregate::{
                AggregateKind, PreparedAggregateStreamingInputs, PreparedCoveringDistinctStrategy,
                PreparedScalarProjectionExecutionState, PreparedScalarProjectionOp,
                PreparedScalarProjectionStrategy, ScalarAggregateOutput, ScalarProjectionWindow,
                field::{
                    FieldSlot, extract_orderable_field_value,
                    resolve_any_aggregate_target_slot_from_planner_slot,
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
        },
        predicate::MissingRowPolicy,
        query::{
            builder::aggregate::terminal_expr_for_kind,
            plan::{
                CoveringProjectionContext, CoveringProjectionOrder, FieldSlot as PlannedFieldSlot,
                constant_covering_projection_value_from_access,
            },
        },
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
    value::Value,
};

type IdValueProjection<E> = Vec<(Id<E>, Value)>;
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
pub(in crate::db) enum ScalarProjectionBoundaryOutput<E: EntityKind + EntityValue> {
    Count(u32),
    Values(Vec<Value>),
    ValuesWithIds(IdValueProjection<E>),
    TerminalValue(Option<Value>),
}

impl<E> ScalarProjectionBoundaryOutput<E>
where
    E: EntityKind + EntityValue,
{
    // Decode one plain-value projection boundary output.
    pub(in crate::db) fn into_values(self) -> Result<Vec<Value>, InternalError> {
        match self {
            Self::Values(values) => Ok(values),
            _ => Err(crate::db::error::query_executor_invariant(
                "scalar projection boundary values output kind mismatch",
            )),
        }
    }

    // Decode one count-distinct projection boundary output.
    pub(in crate::db) fn into_count(self) -> Result<u32, InternalError> {
        match self {
            Self::Count(value) => Ok(value),
            _ => Err(crate::db::error::query_executor_invariant(
                "scalar projection boundary count output kind mismatch",
            )),
        }
    }

    // Decode one `(id, value)` projection boundary output.
    pub(in crate::db) fn into_values_with_ids(self) -> Result<IdValueProjection<E>, InternalError> {
        match self {
            Self::ValuesWithIds(values) => Ok(values),
            _ => Err(crate::db::error::query_executor_invariant(
                "scalar projection boundary values-with-ids output kind mismatch",
            )),
        }
    }

    // Decode one terminal-value projection boundary output.
    pub(in crate::db) fn into_terminal_value(self) -> Result<Option<Value>, InternalError> {
        match self {
            Self::TerminalValue(value) => Ok(value),
            _ => Err(crate::db::error::query_executor_invariant(
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
    ) -> Result<ScalarProjectionBoundaryOutput<E>, InternalError> {
        let prepared = self.prepare_scalar_projection_boundary(plan, target_field, request)?;

        self.execute_prepared_scalar_projection_boundary(prepared)
    }

    // Lower one public scalar field-projection request into one prepared
    // projection contract that no longer retains `ExecutablePlan<E>`.
    fn prepare_scalar_projection_boundary(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        request: ScalarProjectionBoundaryRequest,
    ) -> Result<PreparedScalarProjectionExecutionState<'_, E>, InternalError> {
        let target_field_name = target_field.field().to_string();
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        let prepared = self.prepare_scalar_aggregate_boundary(plan)?;

        let op = PreparedScalarProjectionOp::from_request(request);
        if let PreparedScalarProjectionOp::TerminalValue { terminal_kind } = op
            && !matches!(terminal_kind, AggregateKind::First | AggregateKind::Last)
        {
            return Err(crate::db::error::query_executor_invariant(
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
        prepared_state: PreparedScalarProjectionExecutionState<'_, E>,
    ) -> Result<ScalarProjectionBoundaryOutput<E>, InternalError> {
        let PreparedScalarProjectionExecutionState { boundary, prepared } = prepared_state;

        match boundary.strategy.clone() {
            PreparedScalarProjectionStrategy::Materialized => {
                self.execute_materialized_scalar_projection_boundary(boundary, prepared)
            }
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
        prepared: &PreparedAggregateStreamingInputs<'_, E>,
        target_field: &str,
        op: PreparedScalarProjectionOp,
    ) -> PreparedScalarProjectionStrategy {
        if !prepared.has_predicate()
            && let Some(context) = covering_index_projection_context(
                &prepared.typed_access,
                prepared.order_spec(),
                target_field,
                E::MODEL.primary_key.name,
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
        prepared: PreparedAggregateStreamingInputs<'_, E>,
        context: CoveringProjectionContext,
        window: ScalarProjectionWindow,
        distinct: Option<PreparedCoveringDistinctStrategy>,
    ) -> Result<ScalarProjectionBoundaryOutput<E>, InternalError> {
        match boundary.op {
            PreparedScalarProjectionOp::Values => {
                if let Some(covering_projection) =
                    Self::covering_index_projection_values_with_context_from_prepared(
                        &prepared, context, window,
                    )?
                {
                    Self::record_covering_index_projection_fast_path_hit_for_tests();
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
                    Self::record_covering_index_projection_fast_path_hit_for_tests();
                    let values = match distinct {
                        Some(PreparedCoveringDistinctStrategy::Adjacent) => {
                            dedup_adjacent_values(covering_projection.values)
                        }
                        Some(PreparedCoveringDistinctStrategy::PreserveFirst) => {
                            dedup_values_preserving_first(covering_projection.values)?
                        }
                        None => {
                            return Err(crate::db::error::query_executor_invariant(
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
                    Self::record_covering_index_projection_fast_path_hit_for_tests();
                    let values = match distinct {
                        Some(PreparedCoveringDistinctStrategy::Adjacent) => {
                            dedup_adjacent_values(covering_projection.values)
                        }
                        Some(PreparedCoveringDistinctStrategy::PreserveFirst) => {
                            dedup_values_preserving_first(covering_projection.values)?
                        }
                        None => {
                            return Err(crate::db::error::query_executor_invariant(
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
                if let Some(values) = Self::covering_index_projection_values_with_ids_from_context(
                    &prepared, context, window,
                )? {
                    Self::record_covering_index_projection_fast_path_hit_for_tests();
                    return Ok(ScalarProjectionBoundaryOutput::ValuesWithIds(values));
                }
            }
            PreparedScalarProjectionOp::TerminalValue { terminal_kind } => {
                if let Some(covering_projection) =
                    Self::covering_index_projection_values_with_context_from_prepared(
                        &prepared, context, window,
                    )?
                {
                    Self::record_covering_index_projection_fast_path_hit_for_tests();
                    let value = match terminal_kind {
                        AggregateKind::First => covering_projection.values.first().cloned(),
                        AggregateKind::Last => covering_projection.values.last().cloned(),
                        _ => {
                            return Err(crate::db::error::query_executor_invariant(
                                "covering terminal value projection requires FIRST/LAST aggregate kind",
                            ));
                        }
                    };

                    return Ok(ScalarProjectionBoundaryOutput::TerminalValue(value));
                }
            }
        }

        self.execute_materialized_scalar_projection_boundary(boundary, prepared)
    }

    // Execute one prepared constant projection contract without revisiting
    // covering eligibility checks.
    fn execute_constant_scalar_projection_boundary(
        &self,
        boundary: crate::db::executor::aggregate::PreparedScalarProjectionBoundary,
        prepared: PreparedAggregateStreamingInputs<'_, E>,
        value: Value,
    ) -> Result<ScalarProjectionBoundaryOutput<E>, InternalError> {
        Self::record_covering_constant_projection_fast_path_hit_for_tests();

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
                Err(crate::db::error::query_executor_invariant(
                    "values-with-ids projection cannot execute constant covering strategy",
                ))
            }
        }
    }

    // Record one covering index projection fast-path hit in tests.
    #[allow(clippy::missing_const_for_fn)]
    fn record_covering_index_projection_fast_path_hit_for_tests() {
        Self::record_execution_optimization_hit_for_tests(
            ExecutionOptimizationCounter::CoveringIndexProjectionFastPath,
        );
    }

    // Record one constant covering projection fast-path hit in tests.
    #[allow(clippy::missing_const_for_fn)]
    fn record_covering_constant_projection_fast_path_hit_for_tests() {
        Self::record_execution_optimization_hit_for_tests(
            ExecutionOptimizationCounter::CoveringConstantProjectionFastPath,
        );
    }

    // Execute one prepared materialized projection contract.
    fn execute_materialized_scalar_projection_boundary(
        &self,
        boundary: crate::db::executor::aggregate::PreparedScalarProjectionBoundary,
        prepared: PreparedAggregateStreamingInputs<'_, E>,
    ) -> Result<ScalarProjectionBoundaryOutput<E>, InternalError> {
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

        let response = self.execute_scalar_materialized_rows_stage(prepared)?;

        match boundary.op {
            PreparedScalarProjectionOp::Values => Self::project_field_values_from_materialized(
                response,
                &boundary.target_field_name,
                boundary.field_slot,
            )
            .map(ScalarProjectionBoundaryOutput::Values),
            PreparedScalarProjectionOp::DistinctValues => {
                Self::project_distinct_field_values_from_materialized(
                    response,
                    &boundary.target_field_name,
                    boundary.field_slot,
                )
                .map(ScalarProjectionBoundaryOutput::Values)
            }
            PreparedScalarProjectionOp::CountDistinct => {
                Self::project_distinct_field_values_from_materialized(
                    response,
                    &boundary.target_field_name,
                    boundary.field_slot,
                )
                .map(|values| {
                    ScalarProjectionBoundaryOutput::Count(
                        u32::try_from(values.len()).unwrap_or(u32::MAX),
                    )
                })
            }
            PreparedScalarProjectionOp::ValuesWithIds => {
                Self::project_field_values_with_ids_from_materialized(
                    response,
                    &boundary.target_field_name,
                    boundary.field_slot,
                )
                .map(ScalarProjectionBoundaryOutput::ValuesWithIds)
            }
            PreparedScalarProjectionOp::TerminalValue { .. } => {
                Err(crate::db::error::query_executor_invariant(
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
        prepared: PreparedAggregateStreamingInputs<'_, E>,
        target_field: &str,
        field_slot: FieldSlot,
        terminal_kind: AggregateKind,
    ) -> Result<Option<Value>, InternalError> {
        let consistency = prepared.consistency();
        let state = ExecutionKernel::prepare_aggregate_execution_state_from_prepared(
            prepared,
            terminal_expr_for_kind(terminal_kind),
        );
        let (ScalarAggregateOutput::First(selected_key)
        | ScalarAggregateOutput::Last(selected_key)) =
            ExecutionKernel::execute_prepared_aggregate_state(self, state)?
        else {
            return Err(crate::db::error::query_executor_invariant(
                "terminal value projection result kind mismatch",
            ));
        };
        let Some(selected_key) = selected_key else {
            return Ok(None);
        };

        let ctx = self.recovered_context()?;
        let key = DataKey::new(E::ENTITY_TAG, selected_key);
        let Some(entity) = Self::read_entity_for_field_extrema(&ctx, consistency, &key)? else {
            return Ok(None);
        };
        extract_orderable_field_value(&entity, target_field, field_slot)
            .map_err(Self::map_aggregate_field_value_error)
            .map(Some)
    }

    // Project one materialized response into one field value vector while
    // preserving the effective response row order.
    fn project_field_values_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<Value>, InternalError> {
        response
            .into_iter()
            .map(|row| {
                extract_orderable_field_value(row.entity_ref(), target_field, field_slot)
                    .map_err(Self::map_aggregate_field_value_error)
            })
            .collect()
    }

    // Project one materialized response into distinct field values while
    // preserving first-observed order within the effective response window.
    // This is value DISTINCT semantics via canonical `GroupKey` equality.
    fn project_distinct_field_values_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<Value>, InternalError> {
        let mut distinct_values = GroupKeySet::default();
        let mut projected_values = Vec::new();
        for row in response {
            let value = extract_orderable_field_value(row.entity_ref(), target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            if !insert_materialized_distinct_value(&mut distinct_values, &value)? {
                continue;
            }
            projected_values.push(value);
        }

        Ok(projected_values)
    }

    // Project one materialized response into id/value pairs while preserving
    // the effective response row order.
    fn project_field_values_with_ids_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<IdValueProjection<E>, InternalError> {
        response
            .into_iter()
            .map(|row| {
                let (id, entity) = row.into_parts();
                extract_orderable_field_value(&entity, target_field, field_slot)
                    .map(|value| (id, value))
                    .map_err(Self::map_aggregate_field_value_error)
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
        prepared: &PreparedAggregateStreamingInputs<'_, E>,
        target_field: &str,
    ) -> Option<Value> {
        if !matches!(prepared.consistency(), MissingRowPolicy::Ignore) {
            return None;
        }

        constant_covering_projection_value_from_access(&prepared.typed_access, target_field)
    }

    // Resolve one index-covered projection value vector from already-prepared
    // covering strategy metadata.
    fn covering_index_projection_values_with_context_from_prepared(
        prepared: &PreparedAggregateStreamingInputs<'_, E>,
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

    // Resolve one index-covered `(id, value)` projection vector from already
    // prepared covering strategy metadata.
    fn covering_index_projection_values_with_ids_from_context(
        prepared: &PreparedAggregateStreamingInputs<'_, E>,
        context: CoveringProjectionContext,
        window: ScalarProjectionWindow,
    ) -> Result<Option<IdValueProjection<E>>, InternalError> {
        let Some(projected_pairs) =
            Self::covering_index_projection_pairs_from_context(prepared, context, window)?
        else {
            return Ok(None);
        };

        let projected_values = projected_pairs
            .into_iter()
            .map(|(data_key, value)| {
                data_key
                    .try_key::<E>()
                    .map(Id::from_key)
                    .map(|id| (id, value))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Some(projected_values))
    }

    // Resolve one covering projection pair vector from already prepared
    // covering-index strategy metadata.
    fn covering_index_projection_pairs_from_context(
        prepared: &PreparedAggregateStreamingInputs<'_, E>,
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
        let ctx = &prepared.ctx;
        for (data_key, component_bytes) in raw_pairs {
            match prepared.consistency() {
                MissingRowPolicy::Ignore => match ctx.read(&data_key) {
                    Ok(_) => {}
                    Err(err) if err.is_not_found() => continue,
                    Err(err) => return Err(err),
                },
                MissingRowPolicy::Error => {
                    ctx.read_strict(&data_key)?;
                }
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
        prepared: &PreparedAggregateStreamingInputs<'_, E>,
        component_index: usize,
        direction: Direction,
    ) -> Result<CoveringProjectionComponentRows, InternalError> {
        let ctx = &prepared.ctx;
        let continuation = IndexScanContinuationInput::new(None, direction);

        let prefix_specs = prepared.index_prefix_specs.as_slice();
        if let [spec] = prefix_specs {
            return Self::read_covering_projection_component_pairs_for_index_bounds(
                ctx,
                spec.index(),
                (spec.lower(), spec.upper()),
                continuation,
                component_index,
            );
        }
        if !prefix_specs.is_empty() {
            return Err(crate::db::error::query_executor_invariant(
                "covering projection index-prefix path requires one lowered prefix spec",
            ));
        }

        let range_specs = prepared.index_range_specs.as_slice();
        if let [spec] = range_specs {
            return Self::read_covering_projection_component_pairs_for_index_bounds(
                ctx,
                spec.index(),
                (spec.lower(), spec.upper()),
                continuation,
                component_index,
            );
        }
        if !range_specs.is_empty() {
            return Err(crate::db::error::query_executor_invariant(
                "covering projection index-range path requires one lowered range spec",
            ));
        }

        Err(crate::db::error::query_executor_invariant(
            "covering projection component scans require index-backed access paths",
        ))
    }

    // Execute COUNT from one prepared aggregate stage so constant projection
    // fast paths do not re-enter the plan-owned terminal wrapper surface.
    fn aggregate_count_from_prepared(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_, E>,
    ) -> Result<u32, InternalError> {
        let state = ExecutionKernel::prepare_aggregate_execution_state_from_prepared(
            prepared,
            terminal_expr_for_kind(AggregateKind::Count),
        );
        match ExecutionKernel::execute_prepared_aggregate_state(self, state)? {
            ScalarAggregateOutput::Count(value) => Ok(value),
            _ => Err(crate::db::error::query_executor_invariant(
                "projection COUNT helper result kind mismatch",
            )),
        }
    }

    // Execute EXISTS from one prepared aggregate stage so constant projection
    // fast paths do not re-enter the plan-owned terminal wrapper surface.
    fn aggregate_exists_from_prepared(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_, E>,
    ) -> Result<bool, InternalError> {
        let state = ExecutionKernel::prepare_aggregate_execution_state_from_prepared(
            prepared,
            terminal_expr_for_kind(AggregateKind::Exists),
        );
        match ExecutionKernel::execute_prepared_aggregate_state(self, state)? {
            ScalarAggregateOutput::Exists(value) => Ok(value),
            _ => Err(crate::db::error::query_executor_invariant(
                "projection EXISTS helper result kind mismatch",
            )),
        }
    }

    // Resolve one covering projection component stream for one lowered
    // index-prefix/index-range bounds contract.
    fn read_covering_projection_component_pairs_for_index_bounds(
        ctx: &crate::db::executor::Context<'_, E>,
        index: &crate::model::index::IndexModel,
        bounds: (
            &std::ops::Bound<crate::db::index::RawIndexKey>,
            &std::ops::Bound<crate::db::index::RawIndexKey>,
        ),
        continuation: IndexScanContinuationInput,
        component_index: usize,
    ) -> Result<CoveringProjectionComponentRows, InternalError> {
        let store = ctx
            .db
            .with_store_registry(|registry| registry.try_get_store(index.store()))?;
        store.with_index(|index_store| {
            index_store.resolve_data_values_with_component_in_raw_range_limited(
                E::ENTITY_TAG,
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
