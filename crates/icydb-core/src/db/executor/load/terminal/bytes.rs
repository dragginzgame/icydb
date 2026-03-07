use crate::{
    db::{
        access::{ExecutionPathKind, ExecutionPathPayload},
        data::DataKey,
        direction::Direction,
        executor::{
            AccessExecutionDescriptor, AccessScanContinuationInput, AccessStreamBindings,
            ExecutablePlan,
            aggregate::field::{
                AggregateFieldValueError, extract_orderable_field_value,
                resolve_any_aggregate_target_slot_from_planner_slot,
            },
            load::LoadExecutor,
        },
        query::plan::{FieldSlot as PlannedFieldSlot, OrderDirection},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
#[cfg(test)]
use std::cell::Cell;

use crate::db::executor::load::terminal::{
    bytes_page_window_state, invariant, saturating_add_payload_len, serialized_value_len,
};

#[cfg(test)]
thread_local! {
    static BYTES_PK_FAST_PATH_HITS: Cell<u64> = const { Cell::new(0) };
    static BYTES_STREAM_FAST_PATH_HITS: Cell<u64> = const { Cell::new(0) };
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute one `bytes()` terminal over the canonical load response.
    pub(in crate::db) fn bytes(&self, plan: ExecutablePlan<E>) -> Result<u64, InternalError> {
        if let Some(direction) = Self::bytes_pk_window_fast_path_direction(&plan) {
            Self::record_bytes_pk_fast_path_hit_for_tests();
            return self.bytes_from_pk_store_window(plan, direction);
        }
        if let Some(direction) = Self::bytes_stream_window_fast_path_direction(&plan) {
            Self::record_bytes_stream_fast_path_hit_for_tests();
            return self.bytes_from_ordered_key_stream_window(plan, direction);
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

    // Return the safe PK-shape traversal direction for `bytes()` fast-path
    // execution when no residual semantics require materialized execution.
    fn bytes_pk_window_fast_path_direction(plan: &ExecutablePlan<E>) -> Option<Direction> {
        if plan.has_predicate() || plan.is_distinct() {
            return None;
        }

        let direction = match plan.order_spec() {
            None => Direction::Asc,
            Some(order) => {
                if order.fields.len() != 1 {
                    return None;
                }
                let (field, order_direction) = &order.fields[0];
                if field != E::MODEL.primary_key.name {
                    return None;
                }

                match order_direction {
                    OrderDirection::Asc => Direction::Asc,
                    OrderDirection::Desc => Direction::Desc,
                }
            }
        };

        let access_strategy = plan.access().resolve_strategy();
        let path = access_strategy.as_path()?;
        if !matches!(
            path.kind(),
            ExecutionPathKind::FullScan | ExecutionPathKind::KeyRange
        ) {
            return None;
        }

        Some(direction)
    }

    // Return the stream traversal direction when one scalar shape can fold
    // bytes directly from routed ordered key streams without materialization.
    fn bytes_stream_window_fast_path_direction(plan: &ExecutablePlan<E>) -> Option<Direction> {
        if plan.has_predicate() || plan.is_distinct() {
            return None;
        }
        let access_strategy = plan.access().resolve_strategy();
        let path = access_strategy.as_path()?;

        let Some(order) = plan.order_spec() else {
            return Some(Direction::Asc);
        };
        if order.fields.len() != 1 {
            return None;
        }
        let (field, order_direction) = &order.fields[0];
        if field != E::MODEL.primary_key.name {
            return None;
        }
        if !matches!(
            path.kind(),
            ExecutionPathKind::ByKey | ExecutionPathKind::ByKeys
        ) {
            return None;
        }

        Some(match order_direction {
            OrderDirection::Asc => Direction::Asc,
            OrderDirection::Desc => Direction::Desc,
        })
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
        match path.payload() {
            ExecutionPathPayload::FullScan => {
                ctx.sum_row_payload_bytes_full_scan_window(direction, offset, limit)
            }
            ExecutionPathPayload::KeyRange { start, end } => {
                let start_key = DataKey::try_new::<E>(**start)?;
                let end_key = DataKey::try_new::<E>(**end)?;
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
        let descriptor = AccessExecutionDescriptor::from_bindings(
            plan.access(),
            AccessStreamBindings::new(
                index_prefix_specs.as_slice(),
                index_range_specs.as_slice(),
                AccessScanContinuationInput::new(None, direction),
            ),
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

    #[cfg(test)]
    pub(in crate::db::executor) fn take_bytes_pk_fast_path_hits_for_tests() -> u64 {
        BYTES_PK_FAST_PATH_HITS.with(|counter| {
            let hits = counter.get();
            counter.set(0);
            hits
        })
    }

    #[cfg(test)]
    pub(in crate::db::executor) fn take_bytes_stream_fast_path_hits_for_tests() -> u64 {
        BYTES_STREAM_FAST_PATH_HITS.with(|counter| {
            let hits = counter.get();
            counter.set(0);
            hits
        })
    }

    #[cfg(test)]
    fn record_bytes_pk_fast_path_hit_for_tests() {
        BYTES_PK_FAST_PATH_HITS.with(|counter| {
            counter.set(counter.get().saturating_add(1));
        });
    }

    #[cfg(test)]
    fn record_bytes_stream_fast_path_hit_for_tests() {
        BYTES_STREAM_FAST_PATH_HITS.with(|counter| {
            counter.set(counter.get().saturating_add(1));
        });
    }

    #[cfg(not(test))]
    const fn record_bytes_pk_fast_path_hit_for_tests() {}

    #[cfg(not(test))]
    const fn record_bytes_stream_fast_path_hit_for_tests() {}
}
