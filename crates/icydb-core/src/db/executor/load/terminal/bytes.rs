use crate::{
    db::{
        access::{ExecutableAccessPathDispatch, dispatch_executable_access_path},
        data::DataKey,
        direction::Direction,
        executor::{
            AccessScanContinuationInput, ExecutablePlan, ExecutionOptimizationCounter,
            access_descriptor_from_plan_bindings,
            aggregate::field::{
                AggregateFieldValueError, extract_orderable_field_value,
                resolve_any_aggregate_target_slot_from_planner_slot,
            },
            load::LoadExecutor,
            route::BytesTerminalFastPathContract,
        },
        query::plan::FieldSlot as PlannedFieldSlot,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::load::terminal::{
    bytes_page_window_state, invariant, saturating_add_payload_len, serialized_value_len,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute one `bytes()` terminal over the canonical load response.
    pub(in crate::db) fn bytes(&self, plan: ExecutablePlan<E>) -> Result<u64, InternalError> {
        if let Some(contract) = Self::derive_bytes_terminal_fast_path_contract(&plan) {
            return match contract {
                BytesTerminalFastPathContract::PrimaryKeyWindow(direction) => {
                    Self::record_execution_optimization_hit_for_tests(
                        ExecutionOptimizationCounter::BytesPrimaryKeyFastPath,
                    );
                    self.bytes_from_pk_store_window(plan, direction)
                }
                BytesTerminalFastPathContract::OrderedKeyStreamWindow(direction) => {
                    Self::record_execution_optimization_hit_for_tests(
                        ExecutionOptimizationCounter::BytesStreamFastPath,
                    );
                    self.bytes_from_ordered_key_stream_window(plan, direction)
                }
            };
        }

        let response = self.execute(plan)?;
        let ctx = self.recovered_context()?;
        let mut total = 0u64;

        // Sum persisted row payload sizes for the effective response window.
        for id in response.ids() {
            let key = DataKey::try_new::<E>(id.key())?;
            let row = ctx.read(&key)?;
            total = saturating_add_payload_len(total, row.len());
        }

        Ok(total)
    }

    /// Execute one `bytes(field)` terminal over the canonical load response
    /// window using one planner-resolved field slot.
    pub(in crate::db) fn bytes_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<u64, InternalError> {
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot::<E>(&target_field)
            .map_err(AggregateFieldValueError::into_internal_error)?;
        let response = self.execute(plan)?;
        let mut total = 0u64;

        // Fold serialized field payload sizes over the effective response window.
        for row in response {
            let value =
                extract_orderable_field_value(row.entity_ref(), target_field.field(), field_slot)
                    .map_err(AggregateFieldValueError::into_internal_error)?;
            total = saturating_add_payload_len(total, serialized_value_len(&value)?);
        }

        Ok(total)
    }
    // Fold `bytes()` directly from persisted primary rows over the canonical
    // page window for safe PK full-scan/key-range shapes.
    fn bytes_from_pk_store_window(
        &self,
        plan: ExecutablePlan<E>,
        direction: Direction,
    ) -> Result<u64, InternalError> {
        // Phase 1: snapshot paging + executable payload before store traversal.
        let page = plan.page_spec().cloned();
        let access_strategy = plan.access().resolve_strategy();
        let Some(path) = access_strategy.as_path() else {
            return Err(invariant(
                "bytes PK fast path requires single-path access strategy",
            ));
        };
        let (offset, limit) = bytes_page_window_state(page.as_ref());
        let ctx = self.recovered_context()?;

        // Phase 2: fold payload bytes through context traversal adapters.
        match dispatch_executable_access_path(path) {
            ExecutableAccessPathDispatch::FullScan => {
                ctx.sum_row_payload_bytes_full_scan_window(direction, offset, limit)
            }
            ExecutableAccessPathDispatch::KeyRange { start, end } => {
                let start_key = DataKey::try_new::<E>(*start)?;
                let end_key = DataKey::try_new::<E>(*end)?;
                ctx.sum_row_payload_bytes_key_range_window(
                    &start_key, &end_key, direction, offset, limit,
                )
            }
            _ => Err(invariant(
                "bytes PK fast path requires full-scan or key-range access",
            )),
        }
    }

    // Fold `bytes()` from an ordered key stream over the canonical page window
    // for unordered scalar shapes where row materialization is unnecessary.
    fn bytes_from_ordered_key_stream_window(
        &self,
        plan: ExecutablePlan<E>,
        direction: Direction,
    ) -> Result<u64, InternalError> {
        // Phase 1: materialize immutable stream bindings before stream resolution.
        let page = plan.page_spec().cloned();
        let consistency = plan.consistency();
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let descriptor = access_descriptor_from_plan_bindings(
            plan.access(),
            index_prefix_specs.as_slice(),
            index_range_specs.as_slice(),
            AccessScanContinuationInput::new(None, direction),
            None,
            None,
        );
        let (offset, limit) = bytes_page_window_state(page.as_ref());

        // Phase 2: stream keys and sum persisted payload lengths over the page window.
        let ctx = self.recovered_context()?;
        let mut key_stream = ctx.ordered_key_stream_from_access_descriptor(descriptor)?;

        ctx.sum_row_payload_bytes_from_ordered_key_stream(
            key_stream.as_mut(),
            consistency,
            offset,
            limit,
        )
    }
}
