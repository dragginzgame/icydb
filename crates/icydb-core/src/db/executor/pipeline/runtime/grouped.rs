//! Module: db::executor::pipeline::runtime::grouped
//! Defines grouped row runtime and fold-stage carriers.
//! Does not own: grouped route-stage DTOs or planner semantics.
//! Boundary: keeps grouped row decoding and fold-stage runtime state out of contracts.

use crate::{
    db::{
        data::{DataKey, RawRow, StorageKey, StructuralFieldDecodeContract},
        executor::{
            ExecutionOptimization, ExecutionPreparation,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, extract_orderable_field_value_with_slot_reader,
            },
            pipeline::contracts::{GroupedCursorPage, ResolvedExecutionKeyStream},
            projection::eval_effective_runtime_filter_program_with_value_cow_reader,
            terminal::{RetainedSlotLayout, RetainedSlotRow, RowDecoder, RowLayout},
        },
        predicate::MissingRowPolicy,
        query::plan::{
            EffectiveRuntimeFilterProgram, FieldSlot as PlannedFieldSlot,
            GroupedAggregateExecutionSpec, GroupedDistinctExecutionStrategy,
            expr::CompiledExprValueReader,
        },
        registry::StoreHandle,
    },
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;

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
    effective_runtime_filter_program: Option<&EffectiveRuntimeFilterProgram>,
) -> RetainedSlotLayout {
    let field_count = row_layout.field_count();
    let mut required_slots = vec![false; field_count];

    // Phase 1: every grouped path needs the group key slots themselves.
    for field in group_fields {
        if let Some(required_slot) = required_slots.get_mut(field.index()) {
            *required_slot = true;
        }
    }

    // Phase 2: residual filter semantics still run on grouped row views.
    if let Some(effective_runtime_filter_program) = effective_runtime_filter_program {
        effective_runtime_filter_program.mark_referenced_slots(&mut required_slots);
    }

    // Phase 3: grouped reducer state needs every row slot referenced by either
    // one direct field-target aggregate or one widened aggregate-input scalar
    // expression carried into grouped fold runtime.
    for aggregate in grouped_aggregate_execution_specs {
        if let Some(target_slot) = aggregate.target_slot()
            && let Some(required_slot) = required_slots.get_mut(target_slot.index())
        {
            *required_slot = true;
        }

        if let Some(compiled_input_expr) = aggregate.compiled_input_expr() {
            compiled_input_expr.mark_referenced_slots(&mut required_slots);
        }

        if let Some(compiled_filter_expr) = aggregate.compiled_filter_expr() {
            compiled_filter_expr.mark_referenced_slots(&mut required_slots);
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

// Grouped row views either keep one dense field-width slot image for tests,
// one compact single-slot value for the common grouped-count path, or one
// retained slot row for production grouped ingest. The retained row shape
// decodes each required field once at the row-runtime boundary so filter,
// grouping, and aggregate evaluation can reuse borrowed slot values.
enum RowViewStorage {
    #[cfg(test)]
    Dense(Vec<Option<Value>>),
    Single {
        slot: usize,
        value: Value,
    },
    Retained(RetainedSlotRow),
}

impl RowView {
    // Build the shared missing-slot invariant so borrowed and consuming slot
    // access paths preserve the same failure text.
    fn missing_required_slot_error(index: usize) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped row view missing required slot value: index={index}",
        ))
    }

    /// Build one structural row view from slot-indexed values.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) const fn new(slots: Vec<Option<Value>>) -> Self {
        Self {
            storage: RowViewStorage::Dense(slots),
        }
    }

    /// Build one grouped row view over already decoded retained slot values.
    #[must_use]
    pub(in crate::db::executor) const fn from_retained_slots(row: RetainedSlotRow) -> Self {
        Self {
            storage: RowViewStorage::Retained(row),
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

    /// Borrow one slot by index when the row view already owns decoded values.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::executor) fn borrow_slot_for_test(&self, index: usize) -> Option<&Value> {
        match &self.storage {
            RowViewStorage::Dense(slots) => slots.get(index).and_then(Option::as_ref),
            RowViewStorage::Single { slot, value } => (*slot == index).then_some(value),
            RowViewStorage::Retained(row) => row.slot_ref(index),
        }
    }

    /// Read one slot by index without cloning decoded grouped row values.
    pub(in crate::db::executor) fn slot_value(&self, index: usize) -> Option<Cow<'_, Value>> {
        match &self.storage {
            #[cfg(test)]
            RowViewStorage::Dense(slots) => {
                slots.get(index).and_then(Option::as_ref).map(Cow::Borrowed)
            }
            RowViewStorage::Single { slot, value } => {
                (*slot == index).then_some(Cow::Borrowed(value))
            }
            RowViewStorage::Retained(row) => row.slot_ref(index).map(Cow::Borrowed),
        }
    }

    /// Borrow one slot by index for value-only compiled expression readers.
    pub(in crate::db::executor) fn slot_value_ref(&self, index: usize) -> Option<&Value> {
        match &self.storage {
            #[cfg(test)]
            RowViewStorage::Dense(slots) => slots.get(index).and_then(Option::as_ref),
            RowViewStorage::Single { slot, value } => (*slot == index).then_some(value),
            RowViewStorage::Retained(row) => row.slot_ref(index),
        }
    }

    /// Read one required slot and fail closed when it is missing.
    pub(in crate::db::executor) fn require_slot_value(
        &self,
        index: usize,
    ) -> Result<Cow<'_, Value>, InternalError> {
        self.slot_value(index)
            .ok_or_else(|| Self::missing_required_slot_error(index))
    }

    /// Consume this row view and move out one required slot value without
    /// cloning. Use this only at callsites that no longer need any other row
    /// slots after extracting the selected value.
    pub(in crate::db::executor) fn into_required_slot_value(
        self,
        index: usize,
    ) -> Result<Value, InternalError> {
        match self.storage {
            #[cfg(test)]
            RowViewStorage::Dense(mut slots) => slots
                .get_mut(index)
                .and_then(Option::take)
                .ok_or_else(|| Self::missing_required_slot_error(index)),
            RowViewStorage::Single { slot, value } => {
                if slot == index {
                    return Ok(value);
                }

                Err(Self::missing_required_slot_error(index))
            }
            RowViewStorage::Retained(mut row) => row
                .take_slot(index)
                .ok_or_else(|| Self::missing_required_slot_error(index)),
        }
    }

    /// Decode one required slot into an owned value.
    pub(in crate::db::executor) fn require_slot_owned(
        &self,
        index: usize,
    ) -> Result<Value, InternalError> {
        match self.require_slot_value(index)? {
            Cow::Borrowed(value) => Ok(value.clone()),
            Cow::Owned(value) => Ok(value),
        }
    }

    /// Run one closure with a required slot value borrowed from either the
    /// decoded row-view storage or the stack-local lazy decode result.
    pub(in crate::db::executor) fn with_required_slot<R>(
        &self,
        index: usize,
        f: impl FnOnce(&Value) -> Result<R, InternalError>,
    ) -> Result<R, InternalError> {
        match self.require_slot_value(index)? {
            Cow::Borrowed(value) => f(value),
            Cow::Owned(value) => f(&value),
        }
    }

    /// Evaluate one compiled residual filter program against this structural row.
    pub(in crate::db::executor) fn eval_filter_program(
        &self,
        effective_runtime_filter_program: &EffectiveRuntimeFilterProgram,
    ) -> Result<bool, InternalError> {
        eval_effective_runtime_filter_program_with_value_cow_reader(
            effective_runtime_filter_program,
            &mut |slot| self.slot_value(slot),
            "grouped row filter expression could not read slot",
        )
    }

    /// Extract one validated aggregate field value from this structural row.
    pub(in crate::db::executor) fn extract_orderable_field_value(
        &self,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Value, InternalError> {
        let mut value = Some(self.require_slot_owned(field_slot.index)?);

        extract_orderable_field_value_with_slot_reader(target_field, field_slot, &mut |_| {
            value.take()
        })
        .map_err(AggregateFieldValueError::into_internal_error)
    }

    /// Collect one grouped key payload from planned group field slots.
    pub(in crate::db::executor) fn group_values(
        &self,
        group_fields: &[PlannedFieldSlot],
    ) -> Result<Vec<Value>, InternalError> {
        let mut values = Vec::with_capacity(group_fields.len());

        for field in group_fields {
            let value = self.require_slot_owned(field.index())?;
            values.push(value);
        }

        Ok(values)
    }
}

impl CompiledExprValueReader for RowView {
    fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>> {
        self.slot_value_ref(slot).map(Cow::Borrowed)
    }

    fn read_group_key(&self, _offset: usize) -> Option<Cow<'_, Value>> {
        None
    }

    fn read_aggregate(&self, _index: usize) -> Option<Cow<'_, Value>> {
        None
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
    field: StructuralFieldDecodeContract,
    primary_key_field: StructuralFieldDecodeContract,
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
                    .field_decode_contract(*required_slot)
                    .expect("grouped slot layout must reference one declared structural row field");
                let primary_key_field = contract
                    .field_decode_contract(contract.primary_key_slot())
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
                let retained_slots = RowDecoder::decode_retained_slots(
                    &self.row_layout,
                    key.storage_key(),
                    &row,
                    &self.grouped_slot_layout,
                )?;

                Ok(RowView::from_retained_slots(retained_slots))
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
        RowDecoder::decode_required_slot_value_with_contracts(
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

        row_view.into_required_slot_value(required_slot).map(Some)
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

    /// Return a cheap source-row candidate count when the resolved stream
    /// already owns one. Grouped fold uses this only for conservative table
    /// pre-sizing; unknown streams stay allocation-lazy.
    #[must_use]
    pub(in crate::db::executor) fn cheap_access_candidate_count_hint(&self) -> Option<usize> {
        self.resolved.cheap_access_candidate_count_hint()
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
        db::executor::{
            pipeline::runtime::RowView,
            terminal::{RetainedSlotLayout, RetainedSlotRow},
        },
        value::Value,
    };

    #[test]
    fn dense_test_row_view_resolves_sparse_slots() {
        let row_view = RowView::new(vec![
            None,
            Some(Value::Uint(7)),
            None,
            None,
            Some(Value::Text("group".to_string())),
            None,
        ]);

        assert_eq!(row_view.borrow_slot_for_test(1), Some(&Value::Uint(7)));
        assert_eq!(
            row_view.borrow_slot_for_test(4),
            Some(&Value::Text("group".to_string()))
        );
        assert_eq!(row_view.borrow_slot_for_test(0), None);
    }

    #[test]
    fn single_slot_row_view_resolves_only_its_declared_slot() {
        let row_view = RowView::from_single_value(4, Value::Text("group".to_string()));

        assert_eq!(
            row_view.borrow_slot_for_test(4),
            Some(&Value::Text("group".to_string()))
        );
        assert_eq!(row_view.borrow_slot_for_test(1), None);
    }

    #[test]
    fn retained_row_view_slot_reads_are_repeatable_borrows() {
        let layout = RetainedSlotLayout::compile(5, vec![1, 4]);
        let retained = RetainedSlotRow::from_indexed_values(
            &layout,
            vec![Some(Value::Uint(7)), Some(Value::Text("group".to_string()))],
        );
        let row_view = RowView::from_retained_slots(retained);

        assert_eq!(
            row_view.slot_value(1).as_deref(),
            Some(&Value::Uint(7)),
            "first retained slot read should borrow the decoded value",
        );
        assert_eq!(
            row_view.slot_value(1).as_deref(),
            Some(&Value::Uint(7)),
            "second retained slot read must see the same decoded value",
        );
        assert_eq!(
            row_view.slot_value(4).as_deref(),
            Some(&Value::Text("group".to_string())),
            "reading another retained slot must not invalidate earlier slots",
        );
    }
}
