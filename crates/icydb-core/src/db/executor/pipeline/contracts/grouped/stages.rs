//! Module: db::executor::pipeline::contracts::grouped::stages
//! Defines grouped pipeline stage contracts from route selection through
//! grouped output.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        data::{DataKey, RawRow, StorageKey},
        executor::{
            ExecutionOptimization, ExecutionPreparation,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot,
                extract_orderable_field_value_with_slot_ref_reader,
            },
            pipeline::contracts::{GroupedCursorPage, ResolvedExecutionKeyStream},
            terminal::{RetainedSlotLayout, RowDecoder, RowLayout},
        },
        predicate::{MissingRowPolicy, PredicateProgram},
        query::plan::{
            FieldSlot as PlannedFieldSlot, GroupedAggregateExecutionSpec,
            GroupedDistinctExecutionStrategy, expr::extend_scalar_projection_referenced_slots,
        },
        registry::StoreHandle,
    },
    error::InternalError,
    model::field::FieldModel,
    value::Value,
};

///
/// RowView
///
/// Structural grouped row view used inside grouped runtime loops.
/// Rows carry slot-indexed values only, so grouped execution can remain
/// monomorphic after typed decode happens at the row-runtime boundary.
///

pub(in crate::db::executor) struct RowView {
    storage: RowViewStorage,
}

// Compile one grouped ingest slot layout from the planner-owned grouped
// runtime shape plus the already selected predicate program.
pub(in crate::db::executor) fn compile_grouped_row_slot_layout_from_parts(
    row_layout: RowLayout,
    group_fields: &[PlannedFieldSlot],
    grouped_aggregate_execution_specs: &[GroupedAggregateExecutionSpec],
    grouped_distinct_execution_strategy: &GroupedDistinctExecutionStrategy,
    compiled_predicate: Option<&PredicateProgram>,
) -> RetainedSlotLayout {
    let field_count = row_layout.field_count();
    let mut required_slots = vec![false; field_count];

    // Phase 1: every grouped path needs the group key slots themselves.
    for field in group_fields {
        if let Some(required_slot) = required_slots.get_mut(field.index()) {
            *required_slot = true;
        }
    }

    // Phase 2: residual predicate evaluation still runs on grouped row views.
    if let Some(compiled_predicate) = compiled_predicate {
        compiled_predicate.mark_referenced_slots(&mut required_slots);
    }

    // Phase 3: grouped reducer state needs every row slot referenced by either
    // one direct field-target aggregate or one widened aggregate-input scalar
    // expression carried into grouped fold runtime.
    for aggregate in grouped_aggregate_execution_specs {
        if let Some(target_field) = aggregate.target_field()
            && let Some(required_slot) = required_slots.get_mut(target_field.index())
        {
            *required_slot = true;
        }

        if let Some(compiled_input_expr) = aggregate.compiled_input_expr() {
            let mut referenced_slots = Vec::new();
            extend_scalar_projection_referenced_slots(compiled_input_expr, &mut referenced_slots);

            for slot in referenced_slots {
                if let Some(required_slot) = required_slots.get_mut(slot) {
                    *required_slot = true;
                }
            }
        }
    }

    // Phase 4: the dedicated grouped DISTINCT path still reads its target
    // field from the shared grouped row view when active.
    if let Some(target_field) = grouped_distinct_execution_strategy.global_distinct_target_slot()
        && let Some(required_slot) = required_slots.get_mut(target_field.index())
    {
        *required_slot = true;
    }

    RetainedSlotLayout::compile(
        field_count,
        required_slots
            .into_iter()
            .enumerate()
            .filter_map(|(slot, required)| required.then_some(slot))
            .collect(),
    )
}

// Grouped row views either keep one dense field-width slot image for tests and
// compatibility helpers or reuse one shared retained-slot layout plus compact
// retained values for production grouped ingest.
enum RowViewStorage {
    #[cfg(test)]
    Dense(Vec<Option<Value>>),
    Single {
        slot: usize,
        value: Value,
    },
    Indexed {
        layout: RetainedSlotLayout,
        values: Vec<Option<Value>>,
    },
}

impl RowView {
    /// Build one structural row view from slot-indexed values.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) const fn new(slots: Vec<Option<Value>>) -> Self {
        Self {
            storage: RowViewStorage::Dense(slots),
        }
    }

    /// Build one compact grouped row view from one shared retained-slot layout
    /// plus caller-declared retained values.
    #[must_use]
    pub(in crate::db::executor) fn from_indexed_values(
        layout: &RetainedSlotLayout,
        values: Vec<Option<Value>>,
    ) -> Self {
        debug_assert_eq!(values.len(), layout.retained_value_count());

        Self {
            storage: RowViewStorage::Indexed {
                layout: layout.clone(),
                values,
            },
        }
    }

    /// Build one compact grouped row view for the common single-slot grouped
    /// shape without cloning the shared retained-slot layout or allocating a
    /// one-element retained-values vector.
    #[must_use]
    pub(in crate::db::executor) const fn from_single_value(slot: usize, value: Value) -> Self {
        Self {
            storage: RowViewStorage::Single { slot, value },
        }
    }

    /// Borrow one slot by index without cloning the underlying value.
    #[must_use]
    pub(in crate::db::executor) fn borrow_slot(&self, index: usize) -> Option<&Value> {
        match &self.storage {
            #[cfg(test)]
            RowViewStorage::Dense(slots) => slots.get(index).and_then(Option::as_ref),
            RowViewStorage::Single { slot, value } => (*slot == index).then_some(value),
            RowViewStorage::Indexed { layout, values } => {
                let value_index = layout.value_index_for_slot(index)?;

                values.get(value_index).and_then(Option::as_ref)
            }
        }
    }

    /// Borrow one required slot and fail closed when it is missing.
    pub(in crate::db::executor) fn require_slot_ref(
        &self,
        index: usize,
    ) -> Result<&Value, InternalError> {
        self.borrow_slot(index).ok_or_else(|| {
            InternalError::query_executor_invariant(format!(
                "grouped row view missing required slot value: index={index}",
            ))
        })
    }

    /// Evaluate one compiled predicate program against this structural row.
    #[must_use]
    pub(in crate::db::executor) fn eval_predicate(
        &self,
        compiled_predicate: &PredicateProgram,
    ) -> bool {
        compiled_predicate.eval_with_slot_value_ref_reader(&mut |slot| self.borrow_slot(slot))
    }

    /// Extract one validated aggregate field value from this structural row.
    pub(in crate::db::executor) fn extract_orderable_field_value(
        &self,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Value, AggregateFieldValueError> {
        let mut read_slot = |index| self.borrow_slot(index);

        extract_orderable_field_value_with_slot_ref_reader(target_field, field_slot, &mut read_slot)
            .cloned()
    }

    /// Collect one grouped key payload from planned group field slots.
    pub(in crate::db::executor) fn group_values(
        &self,
        group_fields: &[PlannedFieldSlot],
    ) -> Result<Vec<Value>, InternalError> {
        let mut values = Vec::with_capacity(group_fields.len());

        for field in group_fields {
            let value = self.require_slot_ref(field.index())?.clone();
            values.push(value);
        }

        Ok(values)
    }
}

///
/// SingleGroupedSlotDecode
///
/// SingleGroupedSlotDecode freezes the field metadata needed by the common
/// one-slot grouped row path.
/// Grouped runtime uses this to avoid rediscovering both the selected-slot and
/// primary-key field contracts on every row decode.
///

struct SingleGroupedSlotDecode {
    slot: usize,
    field: &'static FieldModel,
    primary_key_field: &'static FieldModel,
}

///
/// GroupedRowDecodePath
///
/// GroupedRowDecodePath freezes how one grouped row should be decoded from
/// persisted row bytes at the structural grouped runtime boundary.
/// It keeps the common single-slot fast path and the indexed retained-layout
/// path under one local owner instead of reselecting that decode policy at
/// each grouped row read callsite.
///

enum GroupedRowDecodePath<'a> {
    Single(&'a SingleGroupedSlotDecode),
    Indexed,
}

///
/// StructuralGroupedRowRuntime
///
/// StructuralGroupedRowRuntime keeps grouped row reads on store-handle and
/// structural decode metadata only.
/// Grouped fold/runtime code receives slot-indexed `RowView` payloads without
/// carrying `Context<'_, E>` or any entity-typed row adapter in production.
///

pub(in crate::db::executor) struct StructuralGroupedRowRuntime {
    store: StoreHandle,
    row_layout: RowLayout,
    grouped_slot_layout: RetainedSlotLayout,
    single_grouped_slot_decode: Option<SingleGroupedSlotDecode>,
}

impl StructuralGroupedRowRuntime {
    /// Build one grouped row runtime from structural store authority and one
    /// precomputed row-decode layout.
    #[must_use]
    pub(in crate::db::executor) fn new(
        store: StoreHandle,
        row_layout: RowLayout,
        grouped_slot_layout: RetainedSlotLayout,
    ) -> Self {
        let single_grouped_slot_decode = match grouped_slot_layout.required_slots() {
            [required_slot] => {
                let contract = row_layout.contract();
                let field = contract
                    .fields()
                    .get(*required_slot)
                    .expect("grouped slot layout must reference one declared structural row field");
                let primary_key_field = contract
                    .fields()
                    .get(contract.primary_key_slot())
                    .expect("structural row contract must retain one declared primary-key field");

                Some(SingleGroupedSlotDecode {
                    slot: *required_slot,
                    field,
                    primary_key_field,
                })
            }
            _ => None,
        };

        Self {
            store,
            row_layout,
            grouped_slot_layout,
            single_grouped_slot_decode,
        }
    }

    // Decode one persisted data row straight into the structural slot view
    // consumed by grouped fold/runtime stages without building a full kernel row.
    fn row_view_from_data_row(&self, key: &DataKey, row: RawRow) -> Result<RowView, InternalError> {
        match self.row_decode_path() {
            GroupedRowDecodePath::Single(single_grouped_slot_decode) => self
                .single_slot_row_view_from_data_row(
                    key.storage_key(),
                    row,
                    single_grouped_slot_decode,
                ),
            GroupedRowDecodePath::Indexed => {
                let values = RowDecoder::decode_indexed_slot_values(
                    &self.row_layout,
                    key.storage_key(),
                    &row,
                    &self.grouped_slot_layout,
                )?;

                Ok(RowView::from_indexed_values(
                    &self.grouped_slot_layout,
                    values,
                ))
            }
        }
    }

    // Decode one grouped row view for the common single-slot shape without
    // allocating the shared indexed row-view wrapper.
    fn single_slot_row_view_from_data_row(
        &self,
        expected_key: StorageKey,
        row: RawRow,
        single_grouped_slot_decode: &SingleGroupedSlotDecode,
    ) -> Result<RowView, InternalError> {
        let value = self.decode_single_grouped_slot_value_from_raw_row(
            expected_key,
            &row,
            single_grouped_slot_decode,
        )?;

        let value = value.ok_or_else(|| {
            InternalError::query_executor_invariant(format!(
                "single-slot grouped row decode returned no value: slot={}",
                single_grouped_slot_decode.slot,
            ))
        })?;

        Ok(RowView::from_single_value(
            single_grouped_slot_decode.slot,
            value,
        ))
    }

    // Decode the caller-frozen single grouped slot directly from one raw row.
    // Both the single-slot row-view path and the direct grouped slot read path
    // share this decode contract, so the selected-slot and primary-key field
    // metadata only live in one place.
    fn decode_single_grouped_slot_value_from_raw_row(
        &self,
        expected_key: StorageKey,
        row: &RawRow,
        single_grouped_slot_decode: &SingleGroupedSlotDecode,
    ) -> Result<Option<Value>, InternalError> {
        RowDecoder::decode_required_slot_value_with_fields(
            &self.row_layout,
            expected_key,
            row,
            single_grouped_slot_decode.slot,
            single_grouped_slot_decode.field,
            single_grouped_slot_decode.primary_key_field,
        )
    }

    // Resolve the grouped row decode path once from the retained-slot runtime
    // metadata before row decode begins.
    fn row_decode_path(&self) -> GroupedRowDecodePath<'_> {
        self.single_grouped_slot_decode
            .as_ref()
            .map_or(GroupedRowDecodePath::Indexed, GroupedRowDecodePath::Single)
    }

    // Return the single-slot decode contract only when the caller-selected
    // required slot matches the runtime-frozen single grouped slot path.
    fn matching_single_grouped_slot_decode(
        &self,
        required_slot: usize,
    ) -> Option<&SingleGroupedSlotDecode> {
        self.single_grouped_slot_decode
            .as_ref()
            .filter(|single_grouped_slot_decode| single_grouped_slot_decode.slot == required_slot)
    }

    // Read one persisted row under the grouped consistency contract while
    // preserving fail-closed executor corruption handling.
    fn read_data_row(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
    ) -> Result<Option<RawRow>, InternalError> {
        let raw_key = key.to_raw()?;
        let row = self.store.with_data(|store| store.get(&raw_key));

        match (consistency, row) {
            (MissingRowPolicy::Ignore, None) => Ok(None),
            (MissingRowPolicy::Ignore | MissingRowPolicy::Error, Some(row)) => Ok(Some(row)),
            (MissingRowPolicy::Error, None) => {
                Err(crate::db::executor::ExecutorError::missing_row(key).into())
            }
        }
    }

    /// Read one data row and decode one caller-selected grouped slot value
    /// directly when the grouped runtime already carries the matching one-slot
    /// decode metadata.
    pub(in crate::db::executor) fn read_single_group_value(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
        required_slot: usize,
    ) -> Result<Option<Value>, InternalError> {
        let Some(row) = self.read_data_row(consistency, key)? else {
            return Ok(None);
        };

        if let Some(single_grouped_slot_decode) =
            self.matching_single_grouped_slot_decode(required_slot)
        {
            return self.decode_single_grouped_slot_value_from_raw_row(
                key.storage_key(),
                &row,
                single_grouped_slot_decode,
            );
        }

        let row_view = self.row_view_from_data_row(key, row)?;

        Ok(Some(row_view.require_slot_ref(required_slot)?.clone()))
    }

    /// Read one data row and project it into one structural grouped row view.
    pub(in crate::db::executor) fn read_row_view(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
    ) -> Result<Option<RowView>, InternalError> {
        self.read_data_row(consistency, key)?
            .map(|row| self.row_view_from_data_row(key, row))
            .transpose()
    }
}

///
/// GroupedStreamStage
///
/// Stream-construction stage payload for grouped execution.
/// Owns recovered context, execution preparation, and resolved grouped key
/// stream for fold-phase consumption.
///

pub(in crate::db::executor) struct GroupedStreamStage {
    row_runtime: StructuralGroupedRowRuntime,
    execution_preparation: ExecutionPreparation,
    resolved: ResolvedExecutionKeyStream,
}

impl GroupedStreamStage {
    // Build one grouped stream stage from recovered context, execution preparation,
    // and resolved grouped key stream payload.
    pub(in crate::db::executor) const fn new(
        row_runtime: StructuralGroupedRowRuntime,
        execution_preparation: ExecutionPreparation,
        resolved: ResolvedExecutionKeyStream,
    ) -> Self {
        Self {
            row_runtime,
            execution_preparation,
            resolved,
        }
    }

    // Borrow grouped runtime context, execution preparation, and mutable resolved
    // key stream together so callers can combine immutable/mutable borrows safely.
    pub(in crate::db::executor) const fn parts_mut(
        &mut self,
    ) -> (
        &StructuralGroupedRowRuntime,
        &ExecutionPreparation,
        &mut ResolvedExecutionKeyStream,
    ) {
        (
            &self.row_runtime,
            &self.execution_preparation,
            &mut self.resolved,
        )
    }
}

///
/// GroupedFoldStage
///
/// Fold-phase output payload for grouped execution.
/// Owns grouped page materialization plus observability counters consumed by
/// the final output stage.
///

pub(in crate::db::executor) struct GroupedFoldStage {
    page: GroupedCursorPage,
    filtered_rows: usize,
    check_filtered_rows_upper_bound: bool,
    rows_scanned: usize,
    optimization: Option<ExecutionOptimization>,
    index_predicate_applied: bool,
    index_predicate_keys_rejected: u64,
    distinct_keys_deduped: u64,
}

impl GroupedFoldStage {
    // Build one grouped fold-stage payload from grouped page output plus stream
    // observability metadata captured after grouped fold execution.
    pub(in crate::db::executor) fn from_grouped_stream(
        page: GroupedCursorPage,
        filtered_rows: usize,
        check_filtered_rows_upper_bound: bool,
        stream: &GroupedStreamStage,
        scanned_rows_fallback: usize,
    ) -> Self {
        Self {
            page,
            filtered_rows,
            check_filtered_rows_upper_bound,
            rows_scanned: stream
                .resolved
                .rows_scanned_override()
                .unwrap_or(scanned_rows_fallback),
            optimization: stream.resolved.optimization(),
            index_predicate_applied: stream.resolved.index_predicate_applied(),
            index_predicate_keys_rejected: stream.resolved.index_predicate_keys_rejected(),
            distinct_keys_deduped: stream.resolved.distinct_keys_deduped(),
        }
    }

    // Return grouped output row count for observability.
    pub(in crate::db::executor) const fn rows_returned(&self) -> usize {
        self.page.rows.len()
    }

    // Borrow grouped path optimization outcome metadata.
    pub(in crate::db::executor) const fn optimization(&self) -> Option<ExecutionOptimization> {
        self.optimization
    }

    // Borrow grouped path rows-scanned observability metric.
    pub(in crate::db::executor) const fn rows_scanned(&self) -> usize {
        self.rows_scanned
    }

    // Borrow grouped path index-predicate observability metadata.
    pub(in crate::db::executor) const fn index_predicate_applied(&self) -> bool {
        self.index_predicate_applied
    }

    // Borrow grouped path index-predicate rejection counter.
    pub(in crate::db::executor) const fn index_predicate_keys_rejected(&self) -> u64 {
        self.index_predicate_keys_rejected
    }

    // Borrow grouped path DISTINCT-key dedupe counter.
    pub(in crate::db::executor) const fn distinct_keys_deduped(&self) -> u64 {
        self.distinct_keys_deduped
    }

    // Return whether grouped finalization should assert filtered-row upper bound.
    pub(in crate::db::executor) const fn should_check_filtered_rows_upper_bound(&self) -> bool {
        self.check_filtered_rows_upper_bound
    }

    // Borrow grouped filtered-row count for pagination sanity checks.
    pub(in crate::db::executor) const fn filtered_rows(&self) -> usize {
        self.filtered_rows
    }

    // Consume folded stage and return final grouped page payload.
    pub(in crate::db::executor) fn into_page(self) -> GroupedCursorPage {
        self.page
    }
}
///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::executor::{RetainedSlotLayout, pipeline::contracts::RowView},
        value::Value,
    };

    #[test]
    fn indexed_row_view_resolves_sparse_slots_through_shared_layout() {
        let layout = RetainedSlotLayout::compile(6, vec![1, 4]);
        let row_view = RowView::from_indexed_values(
            &layout,
            vec![Some(Value::Uint(7)), Some(Value::Text("group".to_string()))],
        );

        assert_eq!(row_view.borrow_slot(1), Some(&Value::Uint(7)));
        assert_eq!(
            row_view.borrow_slot(4),
            Some(&Value::Text("group".to_string()))
        );
        assert_eq!(row_view.borrow_slot(0), None);
    }

    #[test]
    fn single_slot_row_view_resolves_only_its_declared_slot() {
        let row_view = RowView::from_single_value(4, Value::Text("group".to_string()));

        assert_eq!(
            row_view.borrow_slot(4),
            Some(&Value::Text("group".to_string()))
        );
        assert_eq!(row_view.borrow_slot(1), None);
    }
}
