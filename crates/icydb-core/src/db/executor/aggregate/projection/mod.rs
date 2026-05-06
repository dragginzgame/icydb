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
                    covering_index_adjacent_distinct_eligible, covering_index_projection_context,
                },
            },
            covering_projection_scan_direction, covering_requires_row_presence_check,
            decode_single_covering_projection_pairs, decode_single_covering_projection_value,
            group::GroupKeySet,
            page_window_state,
            pipeline::contracts::LoadExecutor,
            read_row_presence_with_consistency_from_data_store,
            record_row_check_covering_candidate_seen, record_row_check_row_emitted,
            reorder_covering_projection_pairs,
            resolve_covering_projection_components_from_lowered_specs, saturating_u32_len,
            terminal::{RowDecoder, RowLayout},
        },
        predicate::MissingRowPolicy,
        query::builder::aggregate::{
            ScalarProjectionBoundaryOutput, ScalarProjectionBoundaryRequest,
        },
        query::plan::{
            CoveringProjectionContext, CoveringProjectionOrder, FieldSlot as PlannedFieldSlot,
            constant_covering_projection_value_from_access,
            expr::{Expr, eval_builder_expr_for_value_preview},
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

        let strategy = Self::prepare_scalar_projection_strategy(&prepared, &target_field_name, &op);

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
            PreparedScalarProjectionStrategy::CoveringConstant { value } => self
                .execute_constant_scalar_projection_boundary(
                    op,
                    prepared,
                    &target_field_name,
                    value,
                ),
        }
    }

    // Resolve one non-generic execution strategy for the prepared projection
    // contract before runtime execution begins.
    fn prepare_scalar_projection_strategy(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
        op: &PreparedScalarProjectionOp,
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
            PreparedScalarProjectionOp::Values { .. }
            | PreparedScalarProjectionOp::DistinctValues
            | PreparedScalarProjectionOp::CountDistinct
            | PreparedScalarProjectionOp::TerminalValue { .. } => {
                if let Some(value) =
                    Self::constant_covering_projection_value_if_eligible(prepared, target_field)
                {
                    return PreparedScalarProjectionStrategy::CoveringConstant { value };
                }
            }
            PreparedScalarProjectionOp::ValuesWithIds { .. } => {}
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
            PreparedScalarProjectionOp::Values { .. } => {
                if let Some(values) =
                    Self::covering_index_projection_values_with_context_from_prepared(
                        &prepared, context, window,
                    )?
                {
                    let values = project_scalar_values(values, target_field_name, op.projection())?;

                    return Ok(ScalarProjectionBoundaryOutput::Values(values));
                }
            }
            PreparedScalarProjectionOp::DistinctValues => {
                if let Some(projected_pairs) =
                    Self::covering_index_projection_values_from_context_structural(
                        &prepared, context, window,
                    )?
                {
                    let values = distinct_values_from_covering_projection_pairs(
                        projected_pairs,
                        distinct,
                        op,
                    )?;

                    return Ok(ScalarProjectionBoundaryOutput::Values(values));
                }
            }
            PreparedScalarProjectionOp::CountDistinct => {
                if let Some(projected_pairs) =
                    Self::covering_index_projection_values_from_context_structural(
                        &prepared, context, window,
                    )?
                {
                    let count =
                        count_covering_distinct_projection_pairs(projected_pairs, distinct, op)?;

                    return Ok(ScalarProjectionBoundaryOutput::Count(count));
                }
            }
            PreparedScalarProjectionOp::ValuesWithIds { .. } => {
                if let Some(values) =
                    Self::covering_index_projection_values_from_context_structural(
                        &prepared, context, window,
                    )?
                {
                    let values =
                        project_scalar_value_pairs(values, target_field_name, op.projection())?;

                    return Ok(ScalarProjectionBoundaryOutput::ValuesWithDataKeys(values));
                }
            }
            PreparedScalarProjectionOp::TerminalValue { terminal_kind, .. } => {
                if let Some(projected_pairs) =
                    Self::covering_index_projection_values_from_context_structural(
                        &prepared, context, window,
                    )?
                {
                    op.validate_terminal_value_kind()?;
                    let value = terminal_value_from_covering_projection_pairs(
                        projected_pairs,
                        terminal_kind,
                    )
                    .map(|value| project_scalar_value(target_field_name, op.projection(), value))
                    .transpose()?;

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
        target_field_name: &str,
        value: Value,
    ) -> Result<ScalarProjectionBoundaryOutput, InternalError> {
        match op {
            PreparedScalarProjectionOp::Values { .. } => {
                let value = project_scalar_value(target_field_name, op.projection(), value)?;
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
                let value = project_scalar_value(target_field_name, op.projection(), value)?;
                Ok(ScalarProjectionBoundaryOutput::TerminalValue(
                    has_rows.then_some(value),
                ))
            }
            PreparedScalarProjectionOp::ValuesWithIds { .. } => {
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
        if let PreparedScalarProjectionOp::TerminalValue { terminal_kind, .. } = &op {
            return self
                .execute_selected_value_field_projection_with_slot(
                    prepared,
                    target_field_name,
                    field_slot,
                    *terminal_kind,
                )
                .and_then(|value| {
                    value
                        .map(|value| {
                            project_scalar_value(target_field_name, op.projection(), value)
                        })
                        .transpose()
                })
                .map(ScalarProjectionBoundaryOutput::TerminalValue);
        }

        let (rows, row_layout) = self.load_materialized_aggregate_rows(prepared)?;

        match op {
            PreparedScalarProjectionOp::Values { .. } => {
                let projected_values = Self::project_values_from_materialized_structural(
                    rows,
                    &row_layout,
                    target_field_name,
                    field_slot,
                    op.projection(),
                )?;

                Ok(ScalarProjectionBoundaryOutput::Values(projected_values))
            }
            PreparedScalarProjectionOp::DistinctValues => {
                Self::project_distinct_values_from_materialized_structural(
                    rows,
                    &row_layout,
                    target_field_name,
                    field_slot,
                )
                .map(ScalarProjectionBoundaryOutput::Values)
            }
            PreparedScalarProjectionOp::CountDistinct => {
                Self::count_distinct_values_from_materialized_structural(
                    rows,
                    &row_layout,
                    target_field_name,
                    field_slot,
                )
                .map(ScalarProjectionBoundaryOutput::Count)
            }
            PreparedScalarProjectionOp::ValuesWithIds { .. } => {
                Self::project_field_values_from_materialized_structural(
                    rows,
                    &row_layout,
                    target_field_name,
                    field_slot,
                    op.projection(),
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
            key,
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
        projection: Option<&Expr>,
    ) -> Result<ValueProjection, InternalError> {
        rows.into_iter()
            .map(|(data_key, raw_row)| {
                let value = RowDecoder::decode_required_slot_value(
                    row_layout,
                    data_key.storage_key(),
                    &raw_row,
                    field_slot.index,
                )?;

                let value = extract_orderable_field_value_from_decoded_slot(
                    target_field,
                    field_slot,
                    value,
                )
                .map_err(AggregateFieldValueError::into_internal_error)?;
                let value = project_scalar_value(target_field, projection, value)?;

                Ok((data_key, value))
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
        projection: Option<&Expr>,
    ) -> Result<Vec<Value>, InternalError> {
        rows.into_iter()
            .map(|(data_key, raw_row)| {
                let value = RowDecoder::decode_required_slot_value(
                    row_layout,
                    data_key.storage_key(),
                    &raw_row,
                    field_slot.index,
                )?;

                let value = extract_orderable_field_value_from_decoded_slot(
                    target_field,
                    field_slot,
                    value,
                )
                .map_err(AggregateFieldValueError::into_internal_error)?;

                project_scalar_value(target_field, projection, value)
            })
            .collect()
    }

    // Project DISTINCT materialized structural values in one decode/admission
    // pass. Accepted values are still owned because they are the public output,
    // but rejected candidates no longer pass through a full projected-values
    // staging vector first.
    fn project_distinct_values_from_materialized_structural(
        rows: Vec<DataRow>,
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<Value>, InternalError> {
        let mut distinct_values = GroupKeySet::default();
        let mut projected_values = Vec::with_capacity(rows.len());

        // Phase 1: decode each projected field value and retain only the
        // first canonical DISTINCT admission in response order.
        for (data_key, raw_row) in rows {
            let value = RowDecoder::decode_required_slot_value(
                row_layout,
                data_key.storage_key(),
                &raw_row,
                field_slot.index,
            )?;
            let value =
                extract_orderable_field_value_from_decoded_slot(target_field, field_slot, value)
                    .map_err(AggregateFieldValueError::into_internal_error)?;

            if !insert_materialized_distinct_value(&mut distinct_values, &value)? {
                continue;
            }
            projected_values.push(value);
        }

        Ok(projected_values)
    }

    // Count DISTINCT materialized structural values without first retaining
    // the decoded projection vector. `distinct_values(field)` keeps the
    // value-retaining path because those accepted values are its public output.
    fn count_distinct_values_from_materialized_structural(
        rows: Vec<DataRow>,
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<u32, InternalError> {
        let mut distinct_values = GroupKeySet::default();
        let mut distinct_count = 0usize;

        // Phase 1: decode each projected field value and count canonical
        // DISTINCT admissions in response order.
        for (data_key, raw_row) in rows {
            let value = RowDecoder::decode_required_slot_value(
                row_layout,
                data_key.storage_key(),
                &raw_row,
                field_slot.index,
            )?;
            let value =
                extract_orderable_field_value_from_decoded_slot(target_field, field_slot, value)
                    .map_err(AggregateFieldValueError::into_internal_error)?;

            if !insert_materialized_distinct_value(&mut distinct_values, &value)? {
                continue;
            }
            distinct_count = distinct_count.saturating_add(1);
        }

        Ok(saturating_u32_len(distinct_count))
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
        if matches!(
            context.order_contract,
            CoveringProjectionOrder::IndexOrder(_)
        ) && (window.offset != 0 || window.limit.is_some())
        {
            return Self::covering_index_projection_pairs_in_index_order_window(
                prepared, raw_pairs, window,
            );
        }

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

    // Decode only the effective page-window rows for covering projections whose
    // output order is already index traversal order. Reordered covering paths
    // stay on the materialized decoder because they need the full candidate set
    // before applying the scalar projection window.
    fn covering_index_projection_pairs_in_index_order_window(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        raw_pairs: CoveringProjectionComponentRows,
        window: ScalarProjectionWindow,
    ) -> CoveringProjectionPairsResolution {
        let mut projected_pairs = Vec::with_capacity(window.limit.unwrap_or(raw_pairs.len()));
        let mut present_rows = 0usize;
        let mut emitted_rows = 0usize;

        // Phase 1: preserve row-presence accounting over every covering
        // candidate, but decode only rows that survive the effective
        // OFFSET/LIMIT window.
        for (data_key, _existence_witness, components) in raw_pairs {
            record_row_check_covering_candidate_seen();
            let row_present = prepared.store.with_data(|data| {
                read_row_presence_with_consistency_from_data_store(
                    data,
                    &data_key,
                    prepared.consistency(),
                )
            })?;
            if !row_present {
                continue;
            }

            if present_rows < window.offset {
                present_rows = present_rows.saturating_add(1);
                record_row_check_row_emitted();
                continue;
            }
            if window.limit.is_some_and(|limit| emitted_rows >= limit) {
                present_rows = present_rows.saturating_add(1);
                record_row_check_row_emitted();
                continue;
            }

            let Some(value) = decode_single_covering_projection_value(
                components,
                "aggregate covering projection expected one decoded component",
            )?
            else {
                return Ok(None);
            };
            projected_pairs.push((data_key, value));
            present_rows = present_rows.saturating_add(1);
            emitted_rows = emitted_rows.saturating_add(1);
            record_row_check_row_emitted();
        }

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

// Apply an optional bounded fluent projection expression at the executor
// projection boundary. This keeps `project_values*` from re-projecting a
// materialized value page in the terminal layer.
fn project_scalar_value(
    target_field: &str,
    projection: Option<&Expr>,
    value: Value,
) -> Result<Value, InternalError> {
    let Some(projection) = projection else {
        return Ok(value);
    };

    eval_builder_expr_for_value_preview(projection, target_field, &value)
        .map_err(|err| InternalError::query_unsupported(err.to_string()))
}

// Project a value-only terminal page in the same pass that consumes executor
// projection output.
fn project_scalar_values(
    values: Vec<Value>,
    target_field: &str,
    projection: Option<&Expr>,
) -> Result<Vec<Value>, InternalError> {
    values
        .into_iter()
        .map(|value| project_scalar_value(target_field, projection, value))
        .collect()
}

// Project an id/value terminal page while preserving row identifiers and
// output order.
fn project_scalar_value_pairs(
    values: ValueProjection,
    target_field: &str,
    projection: Option<&Expr>,
) -> Result<ValueProjection, InternalError> {
    values
        .into_iter()
        .map(|(data_key, value)| {
            project_scalar_value(target_field, projection, value).map(|value| (data_key, value))
        })
        .collect()
}

// Apply the prepared covering DISTINCT strategy to already-windowed structural
// pairs, moving accepted values directly into the output vector instead of
// first materializing every value-only candidate.
fn distinct_values_from_covering_projection_pairs(
    projected_pairs: ValueProjection,
    distinct: Option<PreparedCoveringDistinctStrategy>,
    op: PreparedScalarProjectionOp,
) -> Result<Vec<Value>, InternalError> {
    match distinct {
        Some(PreparedCoveringDistinctStrategy::Adjacent) => Ok(
            adjacent_distinct_values_from_projection_pairs(projected_pairs),
        ),
        Some(PreparedCoveringDistinctStrategy::PreserveFirst) => {
            distinct_values_preserving_first_from_projection_pairs(projected_pairs)
        }
        None => Err(op.covering_distinct_strategy_required()),
    }
}

// Deduplicate adjacent covering values while discarding the ordering key at
// the same boundary. Adjacent mode is used only when the prepared covering
// order makes equal values contiguous.
fn adjacent_distinct_values_from_projection_pairs(projected_pairs: ValueProjection) -> Vec<Value> {
    let mut out = Vec::with_capacity(projected_pairs.len());
    for (_, value) in projected_pairs {
        if out.last().is_some_and(|previous| previous == &value) {
            continue;
        }
        out.push(value);
    }

    out
}

// Deduplicate covering values by canonical first-observed identity while
// discarding the ordering key at the same boundary.
fn distinct_values_preserving_first_from_projection_pairs(
    projected_pairs: ValueProjection,
) -> Result<Vec<Value>, InternalError> {
    let mut seen = GroupKeySet::default();
    let mut out = Vec::with_capacity(projected_pairs.len());
    for (_, value) in projected_pairs {
        if !insert_materialized_distinct_value(&mut seen, &value)? {
            continue;
        }
        out.push(value);
    }

    Ok(out)
}

// Count the prepared covering DISTINCT strategy from already-windowed
// structural pairs without materializing a second value-only vector.
fn count_covering_distinct_projection_pairs(
    projected_pairs: ValueProjection,
    distinct: Option<PreparedCoveringDistinctStrategy>,
    op: PreparedScalarProjectionOp,
) -> Result<u32, InternalError> {
    let count = match distinct {
        Some(PreparedCoveringDistinctStrategy::Adjacent) => {
            count_adjacent_projection_pairs(projected_pairs)
        }
        Some(PreparedCoveringDistinctStrategy::PreserveFirst) => {
            count_projection_pairs_preserving_first(projected_pairs)?
        }
        None => return Err(op.covering_distinct_strategy_required()),
    };

    Ok(saturating_u32_len(count))
}

// Count adjacent-deduplicable covering pairs while ignoring the ordering key
// once the prepared window has already been applied.
fn count_adjacent_projection_pairs(projected_pairs: ValueProjection) -> usize {
    let mut previous = None::<Value>;
    let mut count = 0usize;
    for (_, value) in projected_pairs {
        if previous.as_ref().is_some_and(|previous| previous == &value) {
            continue;
        }
        previous = Some(value);
        count = count.saturating_add(1);
    }

    count
}

// Count first-observed canonical DISTINCT covering values without retaining an
// accepted value vector for count-only projection terminals.
fn count_projection_pairs_preserving_first(
    projected_pairs: ValueProjection,
) -> Result<usize, InternalError> {
    let mut seen = GroupKeySet::default();
    let mut count = 0usize;
    for (_, value) in projected_pairs {
        if !insert_materialized_distinct_value(&mut seen, &value)? {
            continue;
        }
        count = count.saturating_add(1);
    }

    Ok(count)
}

// Select the first/last value directly from the already-windowed covering pair
// vector. The key component exists only for ordering and row-presence policy,
// so terminal-value projection can move the selected value without allocating a
// separate value vector or cloning from it.
fn terminal_value_from_covering_projection_pairs(
    projected_pairs: ValueProjection,
    terminal_kind: AggregateKind,
) -> Option<Value> {
    match terminal_kind {
        AggregateKind::First => projected_pairs.into_iter().next().map(|(_, value)| value),
        AggregateKind::Last => projected_pairs
            .into_iter()
            .next_back()
            .map(|(_, value)| value),
        _ => unreachable!(),
    }
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
