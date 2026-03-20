//! Module: db::executor::pipeline::contracts::grouped::stages
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::contracts::grouped::stages.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        data::{DataKey, RawRow},
        executor::{
            ExecutionOptimization, ExecutionPreparation,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, extract_orderable_field_value_with_slot_reader,
            },
            pipeline::contracts::{GroupedCursorPage, ResolvedExecutionKeyStream},
            terminal::{RowDecoder, RowLayout},
        },
        predicate::{MissingRowPolicy, PredicateProgram},
        query::plan::FieldSlot as PlannedFieldSlot,
        registry::StoreHandle,
    },
    error::InternalError,
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
    slots: Vec<Option<Value>>,
}

impl RowView {
    /// Build one structural row view from slot-indexed values.
    #[must_use]
    pub(in crate::db::executor) const fn new(slots: Vec<Option<Value>>) -> Self {
        Self { slots }
    }

    /// Read one slot by index, cloning the value when present.
    #[must_use]
    pub(in crate::db::executor) fn read_slot(&self, index: usize) -> Option<Value> {
        self.slots.get(index).cloned().flatten()
    }

    /// Read one required slot and fail closed when it is missing.
    pub(in crate::db::executor) fn require_slot(
        &self,
        index: usize,
    ) -> Result<Value, InternalError> {
        self.read_slot(index).ok_or_else(|| {
            crate::db::error::query_executor_invariant(format!(
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
        compiled_predicate.eval_with_slot_reader(&mut |slot| self.read_slot(slot))
    }

    /// Extract one validated aggregate field value from this structural row.
    pub(in crate::db::executor) fn extract_orderable_field_value(
        &self,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Value, AggregateFieldValueError> {
        let mut read_slot = |index| self.read_slot(index);

        extract_orderable_field_value_with_slot_reader(target_field, field_slot, &mut read_slot)
    }

    /// Collect one grouped key payload from planned group field slots.
    pub(in crate::db::executor) fn group_values(
        &self,
        group_fields: &[PlannedFieldSlot],
    ) -> Result<Vec<Value>, InternalError> {
        group_fields
            .iter()
            .map(|field| self.require_slot(field.index()))
            .collect()
    }
}

///
/// GroupedRowRuntime
///
/// GroupedRowRuntime owns typed row decode at the grouped execution boundary.
/// Shared grouped fold logic consumes only structural `RowView` payloads after
/// this adapter has decoded one row for one entity type.
///

pub(in crate::db::executor) trait GroupedRowRuntime {
    /// Read one data row and project it into one structural grouped row view.
    fn read_row_view(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
    ) -> Result<Option<RowView>, InternalError>;
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
    row_decoder: RowDecoder,
}

impl StructuralGroupedRowRuntime {
    /// Build one grouped row runtime from structural store authority and model metadata.
    #[must_use]
    pub(in crate::db::executor) fn new(
        store: StoreHandle,
        model: &'static crate::model::entity::EntityModel,
    ) -> Self {
        Self {
            store,
            row_layout: RowLayout::from_model(model),
            row_decoder: RowDecoder::structural(),
        }
    }

    // Decode one persisted data row into the structural slot view consumed by
    // grouped fold/runtime stages.
    fn row_view_from_data_row(&self, row: (DataKey, RawRow)) -> Result<RowView, InternalError> {
        let kernel_row = self.row_decoder.decode(&self.row_layout, row)?;

        Ok(RowView::new(kernel_row.into_slots()))
    }

    // Read one persisted row under the grouped consistency contract while
    // preserving fail-closed executor corruption handling.
    fn read_data_row(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
    ) -> Result<Option<(DataKey, RawRow)>, InternalError> {
        let raw_key = key.to_raw()?;
        let row = self.store.with_data(|store| store.get(&raw_key));

        match (consistency, row) {
            (MissingRowPolicy::Ignore, None) => Ok(None),
            (MissingRowPolicy::Ignore | MissingRowPolicy::Error, Some(row)) => {
                Ok(Some((key.clone(), row)))
            }
            (MissingRowPolicy::Error, None) => Err(
                crate::db::executor::ExecutorError::store_corruption(format!("missing row: {key}"))
                    .into(),
            ),
        }
    }
}

impl GroupedRowRuntime for StructuralGroupedRowRuntime {
    fn read_row_view(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
    ) -> Result<Option<RowView>, InternalError> {
        self.read_data_row(consistency, key)?
            .map(|row| self.row_view_from_data_row(row))
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

pub(in crate::db::executor) struct GroupedStreamStage<'a> {
    row_runtime: Box<dyn GroupedRowRuntime + 'a>,
    execution_preparation: ExecutionPreparation,
    resolved: ResolvedExecutionKeyStream,
}

impl<'a> GroupedStreamStage<'a> {
    // Build one grouped stream stage from recovered context, execution preparation,
    // and resolved grouped key stream payload.
    pub(in crate::db::executor) fn new(
        row_runtime: Box<dyn GroupedRowRuntime + 'a>,
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
    pub(in crate::db::executor) fn parts_mut(
        &mut self,
    ) -> (
        &dyn GroupedRowRuntime,
        &ExecutionPreparation,
        &mut ResolvedExecutionKeyStream,
    ) {
        (
            self.row_runtime.as_ref(),
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
        stream: &GroupedStreamStage<'_>,
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
