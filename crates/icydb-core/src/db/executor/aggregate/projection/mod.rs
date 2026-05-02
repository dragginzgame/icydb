//! Module: executor::aggregate::projection
//! Responsibility: field-value projection terminals over materialized responses.
//! Does not own: grouped key canonicalization internals or route planning logic.
//! Boundary: projection terminal helpers (`values`, `distinct_values`, `first/last value`).
//!
//! `distinct_values_by(field)` here is a non-grouped effective-window helper.
//! Grouped Class B DISTINCT accounting is enforced only through grouped
//! execution context boundaries.

mod covering;

use crate::{
    db::{
        data::{DataKey, DataRow},
        executor::{
            CoveringProjectionComponentRows, ExecutionKernel, PreparedAggregatePlan,
            PreparedExecutionPlan,
            aggregate::{
                AggregateKind, PreparedAggregateSpec, PreparedAggregateStreamingInputs,
                PreparedAggregateTargetField, PreparedCoveringDistinctStrategy,
                PreparedScalarProjectionOp, PreparedScalarProjectionStrategy,
                ScalarProjectionWindow,
                field::{
                    AggregateFieldValueError, FieldSlot,
                    extract_orderable_field_value_from_decoded_slot,
                    resolve_any_aggregate_target_slot_from_planner_slot,
                },
                materialized_distinct::insert_materialized_distinct_value,
                projection::covering::{
                    count_adjacent_values, count_values_preserving_first,
                    covering_index_adjacent_distinct_eligible, covering_index_projection_context,
                    dedup_adjacent_values, dedup_values_preserving_first,
                },
            },
            covering_projection_scan_direction, covering_requires_row_presence_check,
            decode_single_covering_projection_pairs,
            group::GroupKeySet,
            page_window_state,
            pipeline::contracts::LoadExecutor,
            reorder_covering_projection_pairs,
            resolve_covering_projection_components_from_lowered_specs, saturating_u32_len,
            terminal::{RowDecoder, RowLayout},
        },
        predicate::MissingRowPolicy,
        query::builder::aggregate::{
            ScalarProjectionBoundaryOutput, ScalarProjectionBoundaryRequest,
        },
        query::plan::{
            CoveringProjectionContext, FieldSlot as PlannedFieldSlot,
            constant_covering_projection_value_from_access,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

type ValueProjection = Vec<(DataKey, Value)>;
type CoveringProjectionPairRows = Vec<(DataKey, Value)>;
type CoveringProjectionPairsResolution = Result<Option<CoveringProjectionPairRows>, InternalError>;

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one scalar field-projection terminal family request from the
    // typed API boundary, lower plan-derived policy into one prepared
    // projection contract, and then execute that contract.
    pub(in crate::db) fn execute_scalar_projection_boundary(
        &self,
        plan: PreparedExecutionPlan<E>,
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
    // projection contract that no longer retains `PreparedExecutionPlan<E>`.
    fn prepare_scalar_projection_boundary(
        &self,
        plan: PreparedAggregatePlan,
        target_field: PlannedFieldSlot,
        request: ScalarProjectionBoundaryRequest,
    ) -> Result<crate::db::executor::aggregate::PreparedScalarProjectionBoundary<'_>, InternalError>
    {
        let target_field_name = target_field.field().to_string();
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot(&target_field)
            .map_err(AggregateFieldValueError::into_internal_error)?;
        let prepared = self.prepare_scalar_aggregate_boundary(plan)?;

        let op = PreparedScalarProjectionOp::from_request(request);
        op.validate_terminal_value_kind()?;

        let strategy = Self::prepare_scalar_projection_strategy(&prepared, &target_field_name, op);

        Ok(
            crate::db::executor::aggregate::PreparedScalarProjectionBoundary {
                target_field_name,
                field_slot,
                op,
                strategy,
                prepared,
            },
        )
    }

    // Execute one prepared field-projection contract without re-reading
    // access-path, covering, or distinct policy from the original plan.
    fn execute_prepared_scalar_projection_boundary(
        &self,
        prepared_boundary: crate::db::executor::aggregate::PreparedScalarProjectionBoundary<'_>,
    ) -> Result<ScalarProjectionBoundaryOutput, InternalError> {
        let crate::db::executor::aggregate::PreparedScalarProjectionBoundary {
            target_field_name,
            field_slot,
            op,
            strategy,
            prepared,
        } = prepared_boundary;

        match strategy {
            PreparedScalarProjectionStrategy::Materialized => self
                .execute_materialized_scalar_projection_boundary(
                    prepared,
                    &target_field_name,
                    field_slot,
                    op,
                ),
            PreparedScalarProjectionStrategy::CoveringIndex {
                context,
                window,
                distinct,
            } => self.execute_covering_scalar_projection_boundary(
                prepared,
                &target_field_name,
                field_slot,
                op,
                context,
                window,
                distinct,
            ),
            PreparedScalarProjectionStrategy::CoveringConstant { value } => {
                self.execute_constant_scalar_projection_boundary(op, prepared, value)
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
                prepared.authority.primary_key_name(),
            )
        {
            let (offset, limit) = page_window_state(prepared.page_spec());
            let window = ScalarProjectionWindow { offset, limit };
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
    #[expect(
        clippy::too_many_arguments,
        reason = "covering scalar projection execution still threads pre-resolved boundary context, window state, and DISTINCT policy explicitly"
    )]
    fn execute_covering_scalar_projection_boundary(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
        target_field_name: &str,
        field_slot: FieldSlot,
        op: PreparedScalarProjectionOp,
        context: CoveringProjectionContext,
        window: ScalarProjectionWindow,
        distinct: Option<PreparedCoveringDistinctStrategy>,
    ) -> Result<ScalarProjectionBoundaryOutput, InternalError> {
        match op {
            PreparedScalarProjectionOp::Values => {
                if let Some(values) =
                    Self::covering_index_projection_values_with_context_from_prepared(
                        &prepared, context, window,
                    )?
                {
                    return Ok(ScalarProjectionBoundaryOutput::Values(values));
                }
            }
            PreparedScalarProjectionOp::DistinctValues => {
                if let Some(values) =
                    Self::covering_index_projection_values_with_context_from_prepared(
                        &prepared, context, window,
                    )?
                {
                    let values = apply_covering_distinct_projection_values(values, distinct, op)?;

                    return Ok(ScalarProjectionBoundaryOutput::Values(values));
                }
            }
            PreparedScalarProjectionOp::CountDistinct => {
                if let Some(values) =
                    Self::covering_index_projection_values_with_context_from_prepared(
                        &prepared, context, window,
                    )?
                {
                    let count = count_covering_distinct_projection_values(values, distinct, op)?;

                    return Ok(ScalarProjectionBoundaryOutput::Count(count));
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
                if let Some(values) =
                    Self::covering_index_projection_values_with_context_from_prepared(
                        &prepared, context, window,
                    )?
                {
                    PreparedScalarProjectionOp::TerminalValue { terminal_kind }
                        .validate_terminal_value_kind()?;
                    let value = match terminal_kind {
                        AggregateKind::First => values.first().cloned(),
                        AggregateKind::Last => values.last().cloned(),
                        _ => unreachable!(),
                    };

                    return Ok(ScalarProjectionBoundaryOutput::TerminalValue(value));
                }
            }
        }

        self.execute_materialized_scalar_projection_boundary(
            prepared,
            target_field_name,
            field_slot,
            op,
        )
    }

    // Execute one prepared constant projection contract without revisiting
    // covering eligibility checks.
    fn execute_constant_scalar_projection_boundary(
        &self,
        op: PreparedScalarProjectionOp,
        prepared: PreparedAggregateStreamingInputs<'_>,
        value: Value,
    ) -> Result<ScalarProjectionBoundaryOutput, InternalError> {
        match op {
            PreparedScalarProjectionOp::Values => {
                let row_count = ExecutionKernel::execute_prepared_aggregate_state(
                    self,
                    ExecutionKernel::prepare_aggregate_execution_state_from_prepared(
                        prepared,
                        PreparedAggregateSpec::terminal(AggregateKind::Count),
                    ),
                )?
                .into_count("projection COUNT helper result kind mismatch")?;
                let output_len = usize::try_from(row_count).unwrap_or(usize::MAX);

                Ok(ScalarProjectionBoundaryOutput::Values(vec![
                    value;
                    output_len
                ]))
            }
            PreparedScalarProjectionOp::DistinctValues => {
                let has_rows = self.constant_projection_has_rows(prepared)?;
                Ok(ScalarProjectionBoundaryOutput::Values(if has_rows {
                    vec![value]
                } else {
                    Vec::new()
                }))
            }
            PreparedScalarProjectionOp::CountDistinct => {
                let has_rows = self.constant_projection_has_rows(prepared)?;
                Ok(ScalarProjectionBoundaryOutput::Count(u32::from(has_rows)))
            }
            PreparedScalarProjectionOp::TerminalValue { .. } => {
                let has_rows = self.constant_projection_has_rows(prepared)?;
                Ok(ScalarProjectionBoundaryOutput::TerminalValue(
                    has_rows.then_some(value),
                ))
            }
            PreparedScalarProjectionOp::ValuesWithIds => {
                Err(op.constant_covering_strategy_unsupported())
            }
        }
    }

    // Execute one prepared materialized projection contract.
    fn execute_materialized_scalar_projection_boundary(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
        target_field_name: &str,
        field_slot: FieldSlot,
        op: PreparedScalarProjectionOp,
    ) -> Result<ScalarProjectionBoundaryOutput, InternalError> {
        if let PreparedScalarProjectionOp::TerminalValue { terminal_kind } = op {
            return self
                .execute_selected_value_field_projection_with_slot(
                    prepared,
                    target_field_name,
                    field_slot,
                    terminal_kind,
                )
                .map(ScalarProjectionBoundaryOutput::TerminalValue);
        }

        let (rows, row_layout) = self.load_materialized_aggregate_rows(prepared)?;

        match op {
            PreparedScalarProjectionOp::Values => {
                let projected_values = Self::project_values_from_materialized_structural(
                    rows,
                    &row_layout,
                    target_field_name,
                    field_slot,
                )?;

                Ok(ScalarProjectionBoundaryOutput::Values(projected_values))
            }
            PreparedScalarProjectionOp::DistinctValues => {
                Self::project_values_from_materialized_structural(
                    rows,
                    &row_layout,
                    target_field_name,
                    field_slot,
                )
                .and_then(project_distinct_values_from_materialized_values)
                .map(ScalarProjectionBoundaryOutput::Values)
            }
            PreparedScalarProjectionOp::CountDistinct => {
                Self::project_values_from_materialized_structural(
                    rows,
                    &row_layout,
                    target_field_name,
                    field_slot,
                )
                .and_then(count_distinct_values_from_materialized_values)
                .map(ScalarProjectionBoundaryOutput::Count)
            }
            PreparedScalarProjectionOp::ValuesWithIds => {
                Self::project_field_values_from_materialized_structural(
                    rows,
                    &row_layout,
                    target_field_name,
                    field_slot,
                )
                .map(ScalarProjectionBoundaryOutput::ValuesWithDataKeys)
            }
            PreparedScalarProjectionOp::TerminalValue { .. } => {
                Err(op.materialized_branch_unreachable())
            }
        }
    }

    // Execute one field-target selected-value projection (`first_value_by` /
    // `last_value_by` / surface `MIN/MAX(field)`) using a planner-validated slot
    // and route-owned selected-row semantics.
    fn execute_selected_value_field_projection_with_slot(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
        field_slot: FieldSlot,
        terminal_kind: AggregateKind,
    ) -> Result<Option<Value>, InternalError> {
        let consistency = prepared.consistency();
        let store = prepared.store;
        let entity_tag = prepared.authority.entity_tag();
        let row_layout = prepared.authority.row_layout();
        let aggregate = if terminal_kind.is_extrema() {
            PreparedAggregateSpec::field_target(
                terminal_kind,
                PreparedAggregateTargetField::new(
                    target_field.to_string(),
                    field_slot,
                    true,
                    true,
                    target_field == prepared.authority.primary_key_name(),
                ),
            )
        } else {
            PreparedAggregateSpec::terminal(terminal_kind)
        };
        let state =
            ExecutionKernel::prepare_aggregate_execution_state_from_prepared(prepared, aggregate);
        let selected_key = ExecutionKernel::execute_prepared_aggregate_state(self, state)?
            .into_optional_id_terminal(
                terminal_kind,
                "terminal value projection result kind mismatch",
            )?;
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

    // Reuse one canonical EXISTS aggregate probe for constant covering
    // projection branches that only need to know whether the effective window
    // is empty before shaping output.
    fn constant_projection_has_rows(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
    ) -> Result<bool, InternalError> {
        ExecutionKernel::execute_prepared_aggregate_state(
            self,
            ExecutionKernel::prepare_aggregate_execution_state_from_prepared(
                prepared,
                PreparedAggregateSpec::terminal(AggregateKind::Exists),
            ),
        )?
        .into_exists("projection EXISTS helper result kind mismatch")
    }

    // Project materialized structural rows into structural `(data_key, value)`
    // pairs while preserving the effective response row order.
    fn project_field_values_from_materialized_structural(
        rows: Vec<DataRow>,
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<ValueProjection, InternalError> {
        rows.into_iter()
            .map(|(data_key, raw_row)| {
                let value = RowDecoder::decode_required_slot_value(
                    row_layout,
                    data_key.storage_key(),
                    &raw_row,
                    field_slot.index,
                )?;

                extract_orderable_field_value_from_decoded_slot(target_field, field_slot, value)
                    .map(|value| (data_key, value))
                    .map_err(AggregateFieldValueError::into_internal_error)
            })
            .collect()
    }

    // Project materialized structural rows into field values only for terminal
    // families that do not need to expose row ids in their public output.
    fn project_values_from_materialized_structural(
        rows: Vec<DataRow>,
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<Value>, InternalError> {
        rows.into_iter()
            .map(|(data_key, raw_row)| {
                let value = RowDecoder::decode_required_slot_value(
                    row_layout,
                    data_key.storage_key(),
                    &raw_row,
                    field_slot.index,
                )?;

                extract_orderable_field_value_from_decoded_slot(target_field, field_slot, value)
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
    ) -> Result<Option<Vec<Value>>, InternalError> {
        let Some(projected_pairs) =
            Self::covering_index_projection_pairs_from_context(prepared, context, window)?
        else {
            return Ok(None);
        };

        Ok(Some(
            projected_pairs
                .into_iter()
                .map(|(_, value)| value)
                .collect(),
        ))
    }

    // Resolve one index-covered structural `(data_key, value)` projection
    // vector from already prepared covering strategy metadata.
    fn covering_index_projection_values_from_context_structural(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        context: CoveringProjectionContext,
        window: ScalarProjectionWindow,
    ) -> Result<Option<ValueProjection>, InternalError> {
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
        let scan_direction = covering_projection_scan_direction(context.order_contract);
        let raw_pairs = Self::read_covering_projection_component_pairs(
            prepared,
            context.component_index,
            scan_direction,
        )?;

        // Phase 2: enforce missing-row policy and decode projection components.
        let Some(mut projected_pairs) = decode_single_covering_projection_pairs(
            raw_pairs,
            prepared.store,
            prepared.consistency(),
            covering_requires_row_presence_check(),
            "aggregate covering projection expected one decoded component",
            Ok,
        )?
        else {
            return Ok(None);
        };

        // Phase 3: realign to post-access order and apply prepared effective window.
        reorder_covering_projection_pairs(context.order_contract, projected_pairs.as_mut_slice());
        apply_scalar_projection_window_in_place(&mut projected_pairs, window);

        Ok(Some(projected_pairs))
    }

    // Read one index-backed `(data_key, encoded_component)` stream for covering
    // projection decoding.
    fn read_covering_projection_component_pairs(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        component_index: usize,
        direction: crate::db::direction::Direction,
    ) -> Result<CoveringProjectionComponentRows, InternalError> {
        resolve_covering_projection_components_from_lowered_specs(
            prepared.authority.entity_tag(),
            prepared.index_prefix_specs.as_ref(),
            prepared.index_range_specs.as_ref(),
            direction,
            usize::MAX,
            &[component_index],
            |index| prepared.store_resolver.try_get_store(index.store()),
        )
    }
}

fn project_distinct_values_from_materialized_values(
    projected_values: Vec<Value>,
) -> Result<Vec<Value>, InternalError> {
    let mut distinct_values = GroupKeySet::default();
    let mut distinct_projected_values = Vec::with_capacity(projected_values.len());

    // Phase 1: preserve first-observed order while deduplicating on canonical
    // group-key equality over structural projection values.
    for value in projected_values {
        if !insert_materialized_distinct_value(&mut distinct_values, &value)? {
            continue;
        }
        distinct_projected_values.push(value);
    }

    Ok(distinct_projected_values)
}

fn count_distinct_values_from_materialized_values(
    projected_values: Vec<Value>,
) -> Result<u32, InternalError> {
    let mut distinct_values = GroupKeySet::default();
    let mut distinct_count = 0usize;

    // Phase 1: count canonical DISTINCT admissions without retaining an output
    // vector because `COUNT(DISTINCT field)` only needs the accepted cardinality.
    for value in projected_values {
        if !insert_materialized_distinct_value(&mut distinct_values, &value)? {
            continue;
        }
        distinct_count = distinct_count.saturating_add(1);
    }

    Ok(saturating_u32_len(distinct_count))
}

// Apply the prepared covering DISTINCT strategy to one already-windowed
// projection vector so covering projection terminals share one dedup contract.
fn apply_covering_distinct_projection_values(
    values: Vec<Value>,
    distinct: Option<PreparedCoveringDistinctStrategy>,
    op: PreparedScalarProjectionOp,
) -> Result<Vec<Value>, InternalError> {
    match distinct {
        Some(PreparedCoveringDistinctStrategy::Adjacent) => Ok(dedup_adjacent_values(values)),
        Some(PreparedCoveringDistinctStrategy::PreserveFirst) => {
            dedup_values_preserving_first(values)
        }
        None => Err(op.covering_distinct_strategy_required()),
    }
}

// Count the prepared covering DISTINCT strategy without retaining the accepted
// output vector for `COUNT(DISTINCT field)` projection terminals.
fn count_covering_distinct_projection_values(
    values: Vec<Value>,
    distinct: Option<PreparedCoveringDistinctStrategy>,
    op: PreparedScalarProjectionOp,
) -> Result<u32, InternalError> {
    let count = match distinct {
        Some(PreparedCoveringDistinctStrategy::Adjacent) => count_adjacent_values(values),
        Some(PreparedCoveringDistinctStrategy::PreserveFirst) => {
            count_values_preserving_first(values)?
        }
        None => return Err(op.covering_distinct_strategy_required()),
    };

    Ok(saturating_u32_len(count))
}

// Apply one prepared scalar projection page window in place so covering
// projection paths do not allocate a second pair vector after reordering.
fn apply_scalar_projection_window_in_place<T>(
    projected_pairs: &mut Vec<(DataKey, T)>,
    window: ScalarProjectionWindow,
) {
    let keep_start = window.offset.min(projected_pairs.len());
    if keep_start > 0 {
        projected_pairs.drain(..keep_start);
    }

    if let Some(limit) = window.limit
        && projected_pairs.len() > limit
    {
        projected_pairs.truncate(limit);
    }
}
