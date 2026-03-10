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
                AggregateKind, AggregateOutput,
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
            load::LoadExecutor,
        },
        predicate::MissingRowPolicy,
        query::{
            builder::{
                AggregateExpr,
                aggregate::{count, exists, first, last, max, min},
            },
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
type CoveringProjectionPairs = (CoveringProjectionContext, CoveringProjectionPairRows);

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute `values_by(field)` over the effective response window using one
    /// planner-resolved field slot.
    pub(in crate::db) fn values_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Vec<Value>, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        if let Some(projected_values) =
            self.covering_index_projection_values_if_eligible(&plan, &target_field)?
        {
            Self::record_execution_optimization_hit_for_tests(
                ExecutionOptimizationCounter::CoveringIndexProjectionFastPath,
            );
            return Ok(projected_values);
        }
        if let Some(constant_value) =
            Self::constant_covering_projection_value_if_eligible(&plan, target_field.field())
        {
            Self::record_execution_optimization_hit_for_tests(
                ExecutionOptimizationCounter::CoveringConstantProjectionFastPath,
            );
            let row_count = self.aggregate_count(plan)?;
            let output_len = usize::try_from(row_count).unwrap_or(usize::MAX);
            return Ok(vec![constant_value; output_len]);
        }

        let response = self.execute(plan)?;

        Self::project_field_values_from_materialized(response, target_field.field(), field_slot)
    }

    /// Execute `distinct_values_by(field)` over the effective response window
    /// using one planner-resolved field slot.
    pub(in crate::db) fn distinct_values_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Vec<Value>, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        if let Some(covering_projection) =
            self.covering_index_projection_values_with_context_if_eligible(&plan, &target_field)?
        {
            Self::record_execution_optimization_hit_for_tests(
                ExecutionOptimizationCounter::CoveringIndexProjectionFastPath,
            );
            if covering_index_adjacent_distinct_eligible(covering_projection.context) {
                return Ok(dedup_adjacent_values(covering_projection.values));
            }

            return dedup_values_preserving_first(covering_projection.values);
        }
        if let Some(constant_value) =
            Self::constant_covering_projection_value_if_eligible(&plan, target_field.field())
        {
            Self::record_execution_optimization_hit_for_tests(
                ExecutionOptimizationCounter::CoveringConstantProjectionFastPath,
            );
            let has_rows = self.aggregate_exists(plan)?;
            return Ok(if has_rows {
                vec![constant_value]
            } else {
                Vec::new()
            });
        }

        let response = self.execute(plan)?;

        Self::project_distinct_field_values_from_materialized(
            response,
            target_field.field(),
            field_slot,
        )
    }

    /// Execute `values_by_with_ids(field)` over the effective response window
    /// using one planner-resolved field slot.
    pub(in crate::db) fn values_by_with_ids_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<IdValueProjection<E>, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        if let Some(projected_values) =
            self.covering_index_projection_values_with_ids_if_eligible(&plan, &target_field)?
        {
            Self::record_execution_optimization_hit_for_tests(
                ExecutionOptimizationCounter::CoveringIndexProjectionFastPath,
            );
            return Ok(projected_values);
        }
        let response = self.execute(plan)?;

        Self::project_field_values_with_ids_from_materialized(
            response,
            target_field.field(),
            field_slot,
        )
    }

    /// Execute `first_value_by(field)` using one planner-resolved field slot.
    pub(in crate::db) fn first_value_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<Value>, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        if let Some(projected_values) =
            self.covering_index_projection_values_if_eligible(&plan, &target_field)?
        {
            Self::record_execution_optimization_hit_for_tests(
                ExecutionOptimizationCounter::CoveringIndexProjectionFastPath,
            );
            return Ok(projected_values.first().cloned());
        }
        if let Some(constant_value) =
            Self::constant_covering_projection_value_if_eligible(&plan, target_field.field())
        {
            Self::record_execution_optimization_hit_for_tests(
                ExecutionOptimizationCounter::CoveringConstantProjectionFastPath,
            );
            let has_rows = self.aggregate_exists(plan)?;
            return Ok(has_rows.then_some(constant_value));
        }

        self.execute_terminal_value_field_projection_with_slot(
            plan,
            target_field.field(),
            field_slot,
            AggregateKind::First,
        )
    }

    /// Execute `last_value_by(field)` using one planner-resolved field slot.
    pub(in crate::db) fn last_value_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<Value>, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        if let Some(projected_values) =
            self.covering_index_projection_values_if_eligible(&plan, &target_field)?
        {
            Self::record_execution_optimization_hit_for_tests(
                ExecutionOptimizationCounter::CoveringIndexProjectionFastPath,
            );
            return Ok(projected_values.last().cloned());
        }
        if let Some(constant_value) =
            Self::constant_covering_projection_value_if_eligible(&plan, target_field.field())
        {
            Self::record_execution_optimization_hit_for_tests(
                ExecutionOptimizationCounter::CoveringConstantProjectionFastPath,
            );
            let has_rows = self.aggregate_exists(plan)?;
            return Ok(has_rows.then_some(constant_value));
        }

        self.execute_terminal_value_field_projection_with_slot(
            plan,
            target_field.field(),
            field_slot,
            AggregateKind::Last,
        )
    }

    // Execute one field-target scalar terminal projection (`first_value_by` /
    // `last_value_by`) using a planner-validated slot and route-owned
    // first/last row selection semantics.
    fn execute_terminal_value_field_projection_with_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: &str,
        field_slot: FieldSlot,
        terminal_kind: AggregateKind,
    ) -> Result<Option<Value>, InternalError> {
        if !terminal_kind.supports_terminal_value_projection() {
            return Err(crate::db::error::executor_invariant(
                "terminal value projection requires FIRST/LAST aggregate kind",
            ));
        }

        let consistency = plan.consistency();
        let (AggregateOutput::First(selected_id) | AggregateOutput::Last(selected_id)) =
            ExecutionKernel::execute_aggregate_spec(
                self,
                plan,
                terminal_aggregate_expr(terminal_kind),
            )?
        else {
            return Err(crate::db::error::executor_invariant(
                "terminal value projection result kind mismatch",
            ));
        };
        let Some(selected_id) = selected_id else {
            return Ok(None);
        };

        let ctx = self.recovered_context()?;
        let key = DataKey::try_new::<E>(selected_id.key())?;
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
        let mut projected_values = Vec::new();
        for row in response {
            let value = extract_orderable_field_value(row.entity_ref(), target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            projected_values.push(value);
        }

        Ok(projected_values)
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
        let mut projected_values = Vec::new();
        for row in response {
            let (id, entity) = row.into_parts();
            let value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            projected_values.push((id, value));
        }

        Ok(projected_values)
    }

    // Resolve one constant field projection value when access shape guarantees
    // that target-field value is fixed by index-prefix equality bindings.
    //
    // Guard rails:
    // - only enabled for `MissingRowPolicy::Ignore` to preserve strict
    //   missing-row corruption surfacing behavior.
    // - only applies when the target field is bound by index-prefix equality.
    fn constant_covering_projection_value_if_eligible(
        plan: &ExecutablePlan<E>,
        target_field: &str,
    ) -> Option<Value> {
        if !matches!(plan.consistency(), MissingRowPolicy::Ignore) {
            return None;
        }

        constant_covering_projection_value_from_access(plan.access(), target_field)
    }

    // Resolve one index-covered projection value vector for field terminals when
    // planner/runtime shape contracts allow index-only value decoding.
    fn covering_index_projection_values_if_eligible(
        &self,
        plan: &ExecutablePlan<E>,
        target_field: &PlannedFieldSlot,
    ) -> Result<Option<Vec<Value>>, InternalError> {
        let Some(covering_projection) =
            self.covering_index_projection_values_with_context_if_eligible(plan, target_field)?
        else {
            return Ok(None);
        };

        Ok(Some(covering_projection.values))
    }

    // Resolve one index-covered projection value vector with routing metadata
    // so terminal-specific post-processing can choose safe distinct strategy.
    fn covering_index_projection_values_with_context_if_eligible(
        &self,
        plan: &ExecutablePlan<E>,
        target_field: &PlannedFieldSlot,
    ) -> Result<Option<CoveringProjectionValues>, InternalError> {
        let Some((context, projected_pairs)) =
            self.covering_index_projection_pairs_if_eligible(plan, target_field)?
        else {
            return Ok(None);
        };

        let values = projected_pairs
            .into_iter()
            .map(|(_, value)| value)
            .collect();

        Ok(Some(CoveringProjectionValues { values, context }))
    }

    // Resolve one index-covered `(id, value)` vector for `values_by_with_ids`
    // terminals when planner/runtime shape contracts allow index-only decode.
    fn covering_index_projection_values_with_ids_if_eligible(
        &self,
        plan: &ExecutablePlan<E>,
        target_field: &PlannedFieldSlot,
    ) -> Result<Option<IdValueProjection<E>>, InternalError> {
        let Some((_, projected_pairs)) =
            self.covering_index_projection_pairs_if_eligible(plan, target_field)?
        else {
            return Ok(None);
        };

        let mut projected_values = Vec::with_capacity(projected_pairs.len());
        for (data_key, value) in projected_pairs {
            let id = Id::from_key(data_key.try_key::<E>()?);
            projected_values.push((id, value));
        }

        Ok(Some(projected_values))
    }

    // Resolve one index-covered projection pair vector with routing metadata so
    // field-value terminals can share decode, policy, ordering, and window logic.
    fn covering_index_projection_pairs_if_eligible(
        &self,
        plan: &ExecutablePlan<E>,
        target_field: &PlannedFieldSlot,
    ) -> Result<Option<CoveringProjectionPairs>, InternalError> {
        if plan.has_predicate() {
            return Ok(None);
        }

        let Some(context) =
            covering_index_projection_context::<E>(plan.access(), plan, target_field.field())
        else {
            return Ok(None);
        };

        // Phase 1: read component pairs in the order implied by the covering contract.
        let scan_direction = match context.order_contract {
            CoveringProjectionOrder::IndexOrder(direction) => direction,
            CoveringProjectionOrder::PrimaryKeyOrder(_) => Direction::Asc,
        };
        let raw_pairs = self.read_covering_projection_component_pairs(
            plan,
            context.component_index,
            scan_direction,
        )?;

        // Phase 2: enforce missing-row policy and decode projection components.
        let mut projected_pairs = Vec::with_capacity(raw_pairs.len());
        let ctx = self.recovered_context()?;
        for (data_key, component_bytes) in raw_pairs {
            match plan.consistency() {
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

        // Phase 3: realign to post-access order and apply effective window.
        match context.order_contract {
            CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc) => {
                projected_pairs.sort_by(|left, right| left.0.cmp(&right.0));
            }
            CoveringProjectionOrder::PrimaryKeyOrder(Direction::Desc) => {
                projected_pairs.sort_by(|left, right| right.0.cmp(&left.0));
            }
            CoveringProjectionOrder::IndexOrder(Direction::Asc | Direction::Desc) => {}
        }

        let (offset, limit) = scalar_window_for_covering_projection(plan.page_spec());
        let mut windowed_pairs = Vec::new();
        for (data_key, value) in projected_pairs.into_iter().skip(offset) {
            if let Some(limit) = limit
                && windowed_pairs.len() == limit
            {
                break;
            }
            windowed_pairs.push((data_key, value));
        }

        Ok(Some((context, windowed_pairs)))
    }

    // Read one index-backed `(data_key, encoded_component)` stream for covering
    // projection decoding.
    fn read_covering_projection_component_pairs(
        &self,
        plan: &ExecutablePlan<E>,
        component_index: usize,
        direction: Direction,
    ) -> Result<Vec<(DataKey, Vec<u8>)>, InternalError> {
        let ctx = self.recovered_context()?;
        let continuation = IndexScanContinuationInput::new(None, direction);

        let prefix_specs = plan.index_prefix_specs()?;
        if let [spec] = prefix_specs {
            let store = ctx
                .db
                .with_store_registry(|registry| registry.try_get_store(spec.index().store()))?;
            return store.with_index(|index_store| {
                index_store.resolve_data_values_with_component_in_raw_range_limited::<E>(
                    spec.index(),
                    (spec.lower(), spec.upper()),
                    continuation,
                    usize::MAX,
                    component_index,
                    None,
                )
            });
        }
        if !prefix_specs.is_empty() {
            return Err(crate::db::error::executor_invariant(
                "covering projection index-prefix path requires one lowered prefix spec",
            ));
        }

        let range_specs = plan.index_range_specs()?;
        if let [spec] = range_specs {
            let store = ctx
                .db
                .with_store_registry(|registry| registry.try_get_store(spec.index().store()))?;
            return store.with_index(|index_store| {
                index_store.resolve_data_values_with_component_in_raw_range_limited::<E>(
                    spec.index(),
                    (spec.lower(), spec.upper()),
                    continuation,
                    usize::MAX,
                    component_index,
                    None,
                )
            });
        }
        if !range_specs.is_empty() {
            return Err(crate::db::error::executor_invariant(
                "covering projection index-range path requires one lowered range spec",
            ));
        }

        Err(crate::db::error::executor_invariant(
            "covering projection component scans require index-backed access paths",
        ))
    }
}

fn terminal_aggregate_expr(kind: AggregateKind) -> AggregateExpr {
    match kind {
        AggregateKind::Count => count(),
        AggregateKind::Sum => {
            unreachable!("terminal aggregate expression helper must not be used for SUM(field)")
        }
        AggregateKind::Exists => exists(),
        AggregateKind::Min => min(),
        AggregateKind::Max => max(),
        AggregateKind::First => first(),
        AggregateKind::Last => last(),
    }
}
