//! Module: executor::load::terminal
//! Responsibility: load terminal adapters (`take`, top-k/bottom-k row/value projections).
//! Does not own: core load execution routing or predicate/index planning semantics.
//! Boundary: terminal-level post-processing over canonical materialized load responses.

use crate::{
    db::{
        access::{ExecutionPathKind, ExecutionPathPayload},
        data::DataKey,
        direction::Direction,
        executor::{
            AccessExecutionDescriptor, AccessScanContinuationInput, AccessStreamBindings,
            ExecutablePlan,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, compare_orderable_field_values,
                extract_orderable_field_value, resolve_any_aggregate_target_slot_from_planner_slot,
                resolve_orderable_aggregate_target_slot_from_planner_slot,
            },
            load::LoadExecutor,
            saturating_row_len,
        },
        query::plan::{FieldSlot as PlannedFieldSlot, OrderDirection, PageSpec},
        response::EntityResponse,
    },
    error::InternalError,
    serialize::serialized_len,
    traits::{EntityKind, EntityValue},
    types::Id,
    value::Value,
};
#[cfg(test)]
use std::cell::Cell;
use std::cmp::Ordering;

#[cfg(test)]
thread_local! {
    static BYTES_PK_FAST_PATH_HITS: Cell<u64> = const { Cell::new(0) };
    static BYTES_STREAM_FAST_PATH_HITS: Cell<u64> = const { Cell::new(0) };
}

// Field ranking direction for k-selection terminals.
#[derive(Clone, Copy)]
enum RankedFieldDirection {
    Descending,
    Ascending,
}

impl RankedFieldDirection {
    // Determine whether the candidate value outranks the current value under
    // the selected direction contract.
    const fn candidate_precedes(self, candidate_vs_current: Ordering) -> bool {
        match self {
            Self::Descending => matches!(candidate_vs_current, Ordering::Greater),
            Self::Ascending => matches!(candidate_vs_current, Ordering::Less),
        }
    }
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

    /// Execute one `take(k)` terminal over the canonical load response.
    pub(in crate::db) fn take(
        &self,
        plan: ExecutablePlan<E>,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        self.execute_take_terminal(plan, take_count)
    }

    /// Execute one `top_k_by(field, k)` terminal over materialized load rows
    /// using one planner-resolved field slot.
    pub(in crate::db) fn top_k_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(AggregateFieldValueError::into_internal_error)?;
        let response = self.execute(plan)?;

        Self::top_k_field_from_materialized(response, target_field.field(), field_slot, take_count)
    }

    /// Execute one `bottom_k_by(field, k)` terminal over materialized load rows
    /// using one planner-resolved field slot.
    pub(in crate::db) fn bottom_k_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(AggregateFieldValueError::into_internal_error)?;
        let response = self.execute(plan)?;

        Self::bottom_k_field_from_materialized(
            response,
            target_field.field(),
            field_slot,
            take_count,
        )
    }

    /// Execute one `top_k_by_values(field, k)` terminal and return ranked values
    /// using one planner-resolved field slot.
    pub(in crate::db) fn top_k_by_values_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<Vec<Value>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(AggregateFieldValueError::into_internal_error)?;
        let response = self.execute(plan)?;

        Self::top_k_field_values_from_materialized(
            response,
            target_field.field(),
            field_slot,
            take_count,
        )
    }

    /// Execute one `bottom_k_by_values(field, k)` terminal and return ranked
    /// values using one planner-resolved field slot.
    pub(in crate::db) fn bottom_k_by_values_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<Vec<Value>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(AggregateFieldValueError::into_internal_error)?;
        let response = self.execute(plan)?;

        Self::bottom_k_field_values_from_materialized(
            response,
            target_field.field(),
            field_slot,
            take_count,
        )
    }

    /// Execute one `top_k_by_with_ids(field, k)` terminal and return `(id, value)`
    /// rows using one planner-resolved field slot.
    pub(in crate::db) fn top_k_by_with_ids_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(AggregateFieldValueError::into_internal_error)?;
        let response = self.execute(plan)?;

        Self::top_k_field_values_with_ids_from_materialized(
            response,
            target_field.field(),
            field_slot,
            take_count,
        )
    }

    /// Execute one `bottom_k_by_with_ids(field, k)` terminal and return
    /// `(id, value)` rows using one planner-resolved field slot.
    pub(in crate::db) fn bottom_k_by_with_ids_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let field_slot =
            resolve_orderable_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(AggregateFieldValueError::into_internal_error)?;
        let response = self.execute(plan)?;

        Self::bottom_k_field_values_with_ids_from_materialized(
            response,
            target_field.field(),
            field_slot,
            take_count,
        )
    }

    // Execute one row-terminal take (`take(k)`) via canonical materialized
    // response semantics.
    fn execute_take_terminal(
        &self,
        plan: ExecutablePlan<E>,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let response = self.execute(plan)?;
        let mut rows = response.rows();
        let take_len = usize::try_from(take_count).unwrap_or(usize::MAX);
        if rows.len() > take_len {
            rows.truncate(take_len);
        }

        Ok(EntityResponse::new(rows))
    }

    // Reduce one materialized response into deterministic top-k ranked rows
    // ordered by `(field_value_desc, primary_key_asc)`.
    fn top_k_ranked_rows_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, E, Value)>, InternalError> {
        Self::rank_k_rows_from_materialized(
            response,
            target_field,
            field_slot,
            take_count,
            RankedFieldDirection::Descending,
        )
    }

    // Reduce one materialized response into deterministic bottom-k ranked rows
    // ordered by `(field_value_asc, primary_key_asc)`.
    fn bottom_k_ranked_rows_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, E, Value)>, InternalError> {
        Self::rank_k_rows_from_materialized(
            response,
            target_field,
            field_slot,
            take_count,
            RankedFieldDirection::Ascending,
        )
    }

    // Shared ranked-row helper for all top/bottom k terminal families.
    // Memory contract:
    // - Ranking is applied to the materialized effective response window only.
    // - Memory growth is bounded by the effective execute() response size.
    // - No streaming heap optimization is used in 0.29 by design.
    fn rank_k_rows_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
        direction: RankedFieldDirection,
    ) -> Result<Vec<(Id<E>, E, Value)>, InternalError> {
        let mut ordered_rows: Vec<(Id<E>, E, Value)> = Vec::new();
        for row in response {
            let (id, entity) = row.into_parts();
            let value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(AggregateFieldValueError::into_internal_error)?;
            let mut insert_index = ordered_rows.len();
            for (index, (current_id, _, current_value)) in ordered_rows.iter().enumerate() {
                let ordering = compare_orderable_field_values(target_field, &value, current_value)
                    .map_err(AggregateFieldValueError::into_internal_error)?;
                let outranks_current = direction.candidate_precedes(ordering);
                let tie_breaks_by_pk = ordering == Ordering::Equal && id.key() < current_id.key();
                if outranks_current || tie_breaks_by_pk {
                    insert_index = index;
                    break;
                }
            }
            ordered_rows.insert(insert_index, (id, entity, value));
        }
        let take_len = usize::try_from(take_count).unwrap_or(usize::MAX);
        if ordered_rows.len() > take_len {
            ordered_rows.truncate(take_len);
        }

        Ok(ordered_rows)
    }

    // Reduce one materialized response into a deterministic top-k response
    // ordered by `(field_value_desc, primary_key_asc)`.
    fn top_k_field_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let ordered_rows = Self::top_k_ranked_rows_from_materialized(
            response,
            target_field,
            field_slot,
            take_count,
        )?;
        let output_rows = ordered_rows
            .into_iter()
            .map(|(id, entity, _)| (id, entity))
            .collect();

        Ok(EntityResponse::from_rows(output_rows))
    }

    // Reduce one materialized response into top-k projected field values under
    // deterministic `(field_value_desc, primary_key_asc)` ranking.
    fn top_k_field_values_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<Value>, InternalError> {
        let ordered_rows = Self::top_k_ranked_rows_from_materialized(
            response,
            target_field,
            field_slot,
            take_count,
        )?;
        let projected_values = ordered_rows
            .into_iter()
            .map(|(_, _, value)| value)
            .collect();

        Ok(projected_values)
    }

    // Reduce one materialized response into top-k projected field values with
    // ids under deterministic `(field_value_desc, primary_key_asc)` ranking.
    fn top_k_field_values_with_ids_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let ordered_rows = Self::top_k_ranked_rows_from_materialized(
            response,
            target_field,
            field_slot,
            take_count,
        )?;
        let projected_values = ordered_rows
            .into_iter()
            .map(|(id, _, value)| (id, value))
            .collect();

        Ok(projected_values)
    }

    // Reduce one materialized response into a deterministic bottom-k response
    // ordered by `(field_value_asc, primary_key_asc)`.
    fn bottom_k_field_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<EntityResponse<E>, InternalError> {
        let ordered_rows = Self::bottom_k_ranked_rows_from_materialized(
            response,
            target_field,
            field_slot,
            take_count,
        )?;
        let output_rows = ordered_rows
            .into_iter()
            .map(|(id, entity, _)| (id, entity))
            .collect();

        Ok(EntityResponse::from_rows(output_rows))
    }

    // Reduce one materialized response into bottom-k projected field values
    // under deterministic `(field_value_asc, primary_key_asc)` ranking.
    fn bottom_k_field_values_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<Value>, InternalError> {
        let ordered_rows = Self::bottom_k_ranked_rows_from_materialized(
            response,
            target_field,
            field_slot,
            take_count,
        )?;
        let projected_values = ordered_rows
            .into_iter()
            .map(|(_, _, value)| value)
            .collect();

        Ok(projected_values)
    }

    // Reduce one materialized response into bottom-k projected field values
    // with ids under deterministic `(field_value_asc, primary_key_asc)` ranking.
    fn bottom_k_field_values_with_ids_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let ordered_rows = Self::bottom_k_ranked_rows_from_materialized(
            response,
            target_field,
            field_slot,
            take_count,
        )?;
        let projected_values = ordered_rows
            .into_iter()
            .map(|(id, _, value)| (id, value))
            .collect();

        Ok(projected_values)
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
    pub(crate) fn take_bytes_pk_fast_path_hits_for_tests() -> u64 {
        BYTES_PK_FAST_PATH_HITS.with(|counter| {
            let hits = counter.get();
            counter.set(0);
            hits
        })
    }

    #[cfg(test)]
    pub(crate) fn take_bytes_stream_fast_path_hits_for_tests() -> u64 {
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

// Centralize payload-byte saturation so terminal behavior stays explicit and
// testable without requiring oversized persisted rows.
const fn saturating_add_payload_len(total: u64, row_len: usize) -> u64 {
    total.saturating_add(saturating_row_len(row_len))
}

fn bytes_page_window_state(page: Option<&PageSpec>) -> (usize, Option<usize>) {
    let Some(page) = page else {
        return (0, None);
    };
    let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);
    let limit = page
        .limit
        .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

    (offset, limit)
}

#[cfg(test)]
const fn bytes_window_limit_exhausted(limit_remaining: Option<usize>) -> bool {
    matches!(limit_remaining, Some(0))
}

#[cfg(test)]
const fn bytes_window_accept_row(
    offset_remaining: &mut usize,
    limit_remaining: &mut Option<usize>,
) -> bool {
    if *offset_remaining > 0 {
        *offset_remaining = offset_remaining.saturating_sub(1);
        return false;
    }

    if let Some(remaining) = limit_remaining.as_mut() {
        if *remaining == 0 {
            return false;
        }
        *remaining = remaining.saturating_sub(1);
    }

    true
}

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}

// Serialize one value using the canonical runtime codec and return payload len.
fn serialized_value_len(value: &Value) -> Result<usize, InternalError> {
    serialized_len(value).map_err(|err| {
        InternalError::serialize_internal(format!("bytes(field) value encode failed: {err}"))
    })
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_len_sum_saturates_on_overflow() {
        let total = saturating_add_payload_len(u64::MAX - 2, 10);
        assert_eq!(total, u64::MAX);
    }

    #[test]
    fn payload_len_sum_accumulates_without_overflow() {
        let total = saturating_add_payload_len(11, 5);
        assert_eq!(total, 16);
    }

    #[test]
    fn bytes_window_accept_row_respects_offset_and_limit() {
        let mut offset_remaining = 2usize;
        let mut limit_remaining = Some(2usize);

        assert!(!bytes_window_accept_row(
            &mut offset_remaining,
            &mut limit_remaining
        ));
        assert!(!bytes_window_accept_row(
            &mut offset_remaining,
            &mut limit_remaining
        ));
        assert!(bytes_window_accept_row(
            &mut offset_remaining,
            &mut limit_remaining
        ));
        assert!(bytes_window_accept_row(
            &mut offset_remaining,
            &mut limit_remaining
        ));
        assert!(!bytes_window_accept_row(
            &mut offset_remaining,
            &mut limit_remaining
        ));
        assert!(bytes_window_limit_exhausted(limit_remaining));
    }

    #[test]
    fn serialized_value_len_encodes_scalar_payload() {
        let len = serialized_value_len(&Value::Uint(10)).expect("value encode should succeed");
        assert!(len > 0, "encoded scalar payload should be non-empty");
    }
}
