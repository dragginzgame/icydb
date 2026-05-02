//! Module: executor::aggregate::helpers
//! Responsibility: helper terminals for ranked and projected field aggregates.
//! Does not own: core aggregate route planning or key-stream folding contracts.
//! Boundary: materialized helper projections used by aggregate terminal APIs.

use crate::{
    db::{
        cursor::{ContinuationRuntime, LoopAction},
        data::{DataKey, DataRow, RawRow},
        direction::Direction,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutableAccess,
            KeyStreamLoopControl, TraversalRuntime,
            aggregate::PreparedAggregateStreamingInputs,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, compare_orderable_field_values,
                extract_orderable_field_value_from_decoded_slot,
            },
            pipeline::contracts::LoadExecutor,
            plan_metrics::record_rows_scanned_for_path,
            read_data_row_with_consistency_from_store,
            terminal::{RowDecoder, RowLayout, page::KernelRow},
        },
        index::predicate::IndexPredicateExecution,
        predicate::MissingRowPolicy,
        registry::StoreHandle,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::{StorageKey, Value},
};
use std::cmp::Ordering;

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Select one key from an already ordered `(storage_key, value)` projection
    // by caller-provided ordinal policy.
    fn select_ordered_field_projection_key(
        ordered_rows: Vec<(StorageKey, Value)>,
        select_index: impl FnOnce(usize) -> Option<usize>,
    ) -> Option<StorageKey> {
        let selected_index = select_index(ordered_rows.len())?;

        ordered_rows
            .into_iter()
            .nth(selected_index)
            .map(|(id, _)| id)
    }

    // Decode one retained slot and project it into the canonical aggregate
    // field value surface used by both materialized helper paths and single-row
    // aggregate lookups.
    fn decode_projected_field_value(
        row_layout: &RowLayout,
        storage_key: StorageKey,
        raw_row: &RawRow,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Value, InternalError> {
        let value = RowDecoder::decode_required_slot_value(
            row_layout,
            storage_key,
            raw_row,
            field_slot.index,
        )?;
        extract_orderable_field_value_from_decoded_slot(target_field, field_slot, value)
            .map_err(AggregateFieldValueError::into_internal_error)
    }

    // Canonical precedence predicate for field projections under deterministic
    // field ordering with primary-key ascending tie-break.
    fn field_projection_candidate_precedes(
        target_field: &str,
        candidate_key: &StorageKey,
        candidate_value: &Value,
        current_key: &StorageKey,
        current_value: &Value,
        field_preference: Ordering,
    ) -> Result<bool, InternalError> {
        let field_order =
            compare_orderable_field_values(target_field, candidate_value, current_value)
                .map_err(AggregateFieldValueError::into_internal_error)?;
        if field_order == field_preference {
            return Ok(true);
        }

        Ok(field_order == Ordering::Equal && candidate_key < current_key)
    }

    // Execute one field-target nth aggregate (`nth(field, n)`) via canonical
    // materialized fallback semantics using one planner-resolved field slot.
    pub(in crate::db::executor::aggregate) fn execute_nth_field_aggregate_with_slot(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
        field_slot: FieldSlot,
        nth: usize,
    ) -> Result<Option<StorageKey>, InternalError> {
        let (rows, row_layout) = self.load_materialized_aggregate_rows(prepared)?;

        Self::aggregate_nth_field_from_materialized(
            rows,
            &row_layout,
            target_field,
            field_slot,
            nth,
        )
    }

    // Execute one field-target median aggregate (`median(field)`) via
    // canonical materialized fallback semantics using one planner-resolved
    // field slot.
    pub(in crate::db::executor::aggregate) fn execute_median_field_aggregate_with_slot(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Option<StorageKey>, InternalError> {
        let (rows, row_layout) = self.load_materialized_aggregate_rows(prepared)?;

        Self::aggregate_median_field_from_materialized(rows, &row_layout, target_field, field_slot)
    }

    // Execute one field-target paired extrema aggregate (`min_max(field)`)
    // via canonical materialized fallback semantics using one
    // planner-resolved field slot.
    pub(in crate::db::executor::aggregate) fn execute_min_max_field_aggregate_with_slot(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Option<(StorageKey, StorageKey)>, InternalError> {
        let (rows, row_layout) = self.load_materialized_aggregate_rows(prepared)?;

        Self::aggregate_min_max_field_from_materialized(rows, &row_layout, target_field, field_slot)
    }

    // Reduce one materialized response into `nth(field, n)` using deterministic
    // ordering `(field_value_asc, primary_key_asc)`.
    fn aggregate_nth_field_from_materialized(
        rows: Vec<DataRow>,
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
        nth: usize,
    ) -> Result<Option<StorageKey>, InternalError> {
        let ordered_rows = Self::ordered_field_projection_from_materialized(
            rows,
            row_layout,
            target_field,
            field_slot,
        )?;
        // Phase 2: project the requested ordinal position.
        Ok(Self::select_ordered_field_projection_key(
            ordered_rows,
            |len| (nth < len).then_some(nth),
        ))
    }

    // Reduce one materialized response into `median(field)` using deterministic
    // ordering `(field_value_asc, primary_key_asc)`.
    // Even-length windows select the lower median for type-agnostic stability.
    fn aggregate_median_field_from_materialized(
        rows: Vec<DataRow>,
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Option<StorageKey>, InternalError> {
        let ordered_rows = Self::ordered_field_projection_from_materialized(
            rows,
            row_layout,
            target_field,
            field_slot,
        )?;

        Ok(Self::select_ordered_field_projection_key(
            ordered_rows,
            |len| match len {
                0 => None,
                len if len % 2 == 0 => Some(len / 2 - 1),
                len => Some(len / 2),
            },
        ))
    }

    // Reduce one materialized response into `(min_by(field), max_by(field))`
    // using one pass over the response window.
    fn aggregate_min_max_field_from_materialized(
        rows: Vec<DataRow>,
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Option<(StorageKey, StorageKey)>, InternalError> {
        let mut min_candidate: Option<(StorageKey, Value)> = None;
        let mut max_candidate: Option<(StorageKey, Value)> = None;
        Self::for_each_projected_field_from_materialized(
            rows,
            row_layout,
            target_field,
            field_slot,
            |key, value| {
                let replace_min = match min_candidate.as_ref() {
                    Some((current_key, current_value)) => {
                        Self::field_projection_candidate_precedes(
                            target_field,
                            &key,
                            &value,
                            current_key,
                            current_value,
                            Ordering::Less,
                        )?
                    }
                    None => true,
                };
                let replace_max = match max_candidate.as_ref() {
                    Some((current_key, current_value)) => {
                        Self::field_projection_candidate_precedes(
                            target_field,
                            &key,
                            &value,
                            current_key,
                            current_value,
                            Ordering::Greater,
                        )?
                    }
                    None => true,
                };

                match (replace_min, replace_max) {
                    (true, true) => {
                        min_candidate = Some((key, value.clone()));
                        max_candidate = Some((key, value));
                    }
                    (true, false) => {
                        min_candidate = Some((key, value));
                    }
                    (false, true) => {
                        max_candidate = Some((key, value));
                    }
                    (false, false) => {}
                }

                Ok(())
            },
        )?;

        let Some((min_key, _)) = min_candidate else {
            return Ok(None);
        };
        let Some((max_key, _)) = max_candidate else {
            return Err(InternalError::query_executor_invariant(
                "min_max(field) reduction produced a min id without a max id",
            ));
        };

        Ok(Some((min_key, max_key)))
    }

    // Project one response window into deterministic field ordering
    // `(field_value_asc, primary_key_asc)`.
    fn ordered_field_projection_from_materialized(
        rows: Vec<DataRow>,
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<(StorageKey, Value)>, InternalError> {
        let mut ordered_rows: Vec<(StorageKey, Value)> = Vec::new();

        // Decode each projected field value directly into the ordered
        // candidate set. `nth(field)` and `median(field)` still need the
        // ordered vector, but they do not need a separate projected-pair vector
        // before insertion.
        Self::for_each_projected_field_from_materialized(
            rows,
            row_layout,
            target_field,
            field_slot,
            |key, value| {
                let mut insert_index = ordered_rows.len();
                for (index, (current_key, current_value)) in ordered_rows.iter().enumerate() {
                    let candidate_precedes = Self::field_projection_candidate_precedes(
                        target_field,
                        &key,
                        &value,
                        current_key,
                        current_value,
                        Ordering::Less,
                    )?;
                    if candidate_precedes {
                        insert_index = index;
                        break;
                    }
                }

                ordered_rows.insert(insert_index, (key, value));
                Ok(())
            },
        )?;

        Ok(ordered_rows)
    }

    // Project materialized scalar rows into `(id, field_value)` pairs through
    // a caller-owned fold, so one-pass reducers do not need a second projected
    // vector after the materialized row boundary.
    fn for_each_projected_field_from_materialized(
        rows: Vec<DataRow>,
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
        mut visit_projected: impl FnMut(StorageKey, Value) -> Result<(), InternalError>,
    ) -> Result<(), InternalError> {
        for (data_key, raw_row) in rows {
            let storage_key = data_key.storage_key();
            let value = Self::decode_projected_field_value(
                row_layout,
                storage_key,
                &raw_row,
                target_field,
                field_slot,
            )?;
            visit_projected(storage_key, value)?;
        }

        Ok(())
    }

    // Load one structural row for field aggregates while preserving read
    // consistency classification behavior.
    pub(in crate::db::executor) fn read_kernel_row_for_field_aggregate(
        store: StoreHandle,
        row_layout: &RowLayout,
        row_decoder: RowDecoder,
        consistency: MissingRowPolicy,
        key: &DataKey,
    ) -> Result<Option<KernelRow>, InternalError> {
        let Some(row) = read_data_row_with_consistency_from_store(store, key, consistency)? else {
            return Ok(None);
        };

        row_decoder.decode(row_layout, row).map(Some)
    }

    // Load one projected field value from one persisted row while preserving
    // read consistency classification behavior at the outer aggregate edge.
    pub(in crate::db::executor) fn read_field_value_for_aggregate(
        store: StoreHandle,
        row_layout: &RowLayout,
        consistency: MissingRowPolicy,
        key: &DataKey,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Option<Value>, InternalError> {
        let Some(row) = read_data_row_with_consistency_from_store(store, key, consistency)? else {
            return Ok(None);
        };
        let value = Self::decode_projected_field_value(
            row_layout,
            key.storage_key(),
            &row.1,
            target_field,
            field_slot,
        )?;

        Ok(Some(value))
    }

    // Stream one ordered existing-row access path exactly once, handling
    // continuation gating, row reads, and scan metrics before invoking the
    // caller-owned fold closure for each admitted row.
    pub(in crate::db::executor::aggregate) fn for_each_existing_stream_row(
        prepared: PreparedAggregateStreamingInputs<'_>,
        direction: Direction,
        mut on_row: impl FnMut(KernelRow) -> Result<(), InternalError>,
    ) -> Result<(), InternalError> {
        // Phase 1: lower the prepared access/runtime inputs into one key stream.
        let PreparedAggregateStreamingInputs {
            store_resolver: _,
            authority,
            store,
            logical_plan,
            execution_preparation,
            index_prefix_specs,
            index_range_specs,
        } = prepared;
        let consistency =
            crate::db::executor::traversal::row_read_consistency_for_plan(&logical_plan);
        let mut continuation = ContinuationRuntime::from_window(
            crate::db::executor::ExecutionKernel::window_cursor_contract(&logical_plan, None),
        );
        let index_predicate_execution =
            execution_preparation
                .strict_mode()
                .map(|program| IndexPredicateExecution {
                    program,
                    rejected_keys_counter: None,
                });
        let access = ExecutableAccess::from_executable_plan(
            logical_plan.access.executable_contract(),
            AccessStreamBindings::new(
                index_prefix_specs.as_ref(),
                index_range_specs.as_ref(),
                AccessScanContinuationInput::new(None, direction),
            ),
            None,
            index_predicate_execution,
        );
        let runtime = TraversalRuntime::new(store, authority.entity_tag());
        let mut key_stream = runtime.ordered_key_stream_from_runtime_access(access)?;
        let row_layout = authority.row_layout();
        let row_decoder = RowDecoder::structural();

        // Phase 2: walk the key stream once and invoke the caller fold only
        // for rows admitted by the canonical continuation contract.
        let mut rows_scanned = 0usize;

        loop {
            match Self::loop_control_from_stream_continuation(continuation.pre_fetch()) {
                KeyStreamLoopControl::Skip => continue,
                KeyStreamLoopControl::Emit => {}
                KeyStreamLoopControl::Stop => break,
            }

            let Some(data_key) = key_stream.next_key()? else {
                break;
            };
            let Some(row) = Self::read_kernel_row_for_field_aggregate(
                store,
                &row_layout,
                row_decoder,
                consistency,
                &data_key,
            )?
            else {
                continue;
            };
            rows_scanned = rows_scanned.saturating_add(1);

            match continuation.accept_row() {
                LoopAction::Skip => continue,
                LoopAction::Emit => {}
                LoopAction::Stop => break,
            }

            on_row(row)?;
        }

        // Phase 3: preserve canonical aggregate scan accounting once per stream fold.
        record_rows_scanned_for_path(authority.entity_path(), rows_scanned);

        Ok(())
    }

    // Preserve the canonical continuation-to-loop-control mapping for
    // aggregate stream folds without re-matching the enum at every callsite.
    const fn loop_control_from_stream_continuation(action: LoopAction) -> KeyStreamLoopControl {
        match action {
            LoopAction::Skip => KeyStreamLoopControl::Skip,
            LoopAction::Emit => KeyStreamLoopControl::Emit,
            LoopAction::Stop => KeyStreamLoopControl::Stop,
        }
    }
}
