//! Module: db::executor::terminal::ranking::materialized
//! Responsibility: module-local ownership and contracts for db::executor::terminal::ranking::materialized.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod projections;

use crate::{
    db::{
        data::DataRow,
        executor::{
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, compare_orderable_field_values_with_slot,
                extract_orderable_field_value_from_decoded_slot,
            },
            pipeline::contracts::LoadExecutor,
            terminal::{RowDecoder, RowLayout},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::{StorageKey, Value},
};
use std::cmp::Ordering;

// Field ranking direction for k-selection terminals.
#[derive(Clone, Copy)]
enum RankedFieldDirection {
    Descending,
    Ascending,
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Reduce one materialized response into deterministic top-k ranked rows
    // ordered by `(field_value_desc, primary_key_asc)`.
    pub(super) fn top_k_ranked_rows_from_materialized(
        row_layout: RowLayout,
        rows: &[DataRow],
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(usize, Value)>, InternalError> {
        rank_k_rows_from_materialized_structural(
            row_layout,
            rows,
            target_field,
            field_slot,
            take_count,
            RankedFieldDirection::Descending,
        )
    }

    // Reduce one materialized response into deterministic bottom-k ranked rows
    // ordered by `(field_value_asc, primary_key_asc)`.
    pub(super) fn bottom_k_ranked_rows_from_materialized(
        row_layout: RowLayout,
        rows: &[DataRow],
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(usize, Value)>, InternalError> {
        rank_k_rows_from_materialized_structural(
            row_layout,
            rows,
            target_field,
            field_slot,
            take_count,
            RankedFieldDirection::Ascending,
        )
    }
}

// Shared ranked-row helper for all top/bottom k terminal families.
// Memory contract:
// - Ranking is applied to the materialized effective response window only.
// - Memory growth is bounded by the effective execute() response size.
// - Selection uses a bounded nth-style window so ranking terminals do not
//   maintain one fully sorted vector during candidate ingestion.
fn rank_k_rows_from_materialized_structural(
    row_layout: RowLayout,
    rows: &[DataRow],
    target_field: &str,
    field_slot: FieldSlot,
    take_count: u32,
    direction: RankedFieldDirection,
) -> Result<Vec<(usize, Value)>, InternalError> {
    let mut ranked_rows = Vec::with_capacity(rows.len());

    // Phase 1: decode only the ranked target slot from borrowed raw rows and
    // retain one `(primary_key, row_index)` locator instead of cloning full
    // `DataRow` payloads through the bounded ranking window.
    for (row_index, (data_key, raw_row)) in rows.iter().enumerate() {
        let value = decode_ranked_field_value_from_materialized_row(
            &row_layout,
            data_key.storage_key(),
            raw_row,
            target_field,
            field_slot,
        )?;
        ranked_rows.push(((data_key.storage_key(), row_index), value));
    }

    // Phase 2: validate the comparable value domain once, then retain only the
    // requested bounded ranking window before sorting that retained subset
    // into final deterministic `(value, pk)` order.
    apply_ranked_take_window(
        target_field,
        field_slot,
        &mut ranked_rows,
        take_count,
        direction,
    )
    .map_err(AggregateFieldValueError::into_internal_error)?;

    let mut output_rows = Vec::with_capacity(ranked_rows.len());
    for ((_, row_index), value) in ranked_rows {
        output_rows.push((row_index, value));
    }

    Ok(output_rows)
}

// Decode the ranked target field directly from the borrowed persisted row so
// materialized ranking does not clone whole `(data_key, raw_row)` payloads
// just to read one comparable field.
fn decode_ranked_field_value_from_materialized_row(
    row_layout: &RowLayout,
    expected_key: StorageKey,
    raw_row: &crate::db::data::RawRow,
    target_field: &str,
    field_slot: FieldSlot,
) -> Result<Value, InternalError> {
    extract_orderable_field_value_from_decoded_slot(
        target_field,
        field_slot,
        RowDecoder::decode_required_slot_value(
            row_layout,
            expected_key,
            raw_row,
            field_slot.index,
        )?,
    )
    .map_err(AggregateFieldValueError::into_internal_error)
}

// Compare two ranked candidate keys and values under the deterministic
// `(field_value_direction, primary_key_asc)` terminal contract.
fn compare_ranked_keys_and_values<K>(
    target_field: &str,
    field_slot: FieldSlot,
    left_key: &K,
    left_value: &Value,
    right_key: &K,
    right_value: &Value,
    direction: RankedFieldDirection,
) -> Result<Ordering, AggregateFieldValueError>
where
    K: Ord,
{
    let value_ordering = compare_orderable_field_values_with_slot(
        target_field,
        field_slot,
        left_value,
        right_value,
    )?;
    let ranking_ordering = match direction {
        RankedFieldDirection::Descending => value_ordering.reverse(),
        RankedFieldDirection::Ascending => value_ordering,
    };
    if ranking_ordering != Ordering::Equal {
        return Ok(ranking_ordering);
    }

    Ok(left_key.cmp(right_key))
}

// Compare two ranked row candidates after comparable-value validation has
// already admitted the candidate set into one shared ordering domain.
fn compare_ranked_rows_infallible<K, R>(
    target_field: &str,
    field_slot: FieldSlot,
    left: &((K, R), Value),
    right: &((K, R), Value),
    direction: RankedFieldDirection,
) -> Ordering
where
    K: Ord,
{
    compare_ranked_keys_and_values(
        target_field,
        field_slot,
        &left.0.0,
        &left.1,
        &right.0.0,
        &right.1,
        direction,
    )
    .expect("ranked candidates must be prevalidated before bounded selection and sort")
}

// Validate that every ranked value belongs to one comparable domain before
// the nth-style selection path enters infallible comparator APIs.
fn validate_ranked_value_domain<K, R>(
    target_field: &str,
    field_slot: FieldSlot,
    ranked_rows: &[((K, R), Value)],
) -> Result<(), AggregateFieldValueError>
where
    K: Ord,
{
    let Some(((_, _), first_value)) = ranked_rows.first() else {
        return Ok(());
    };

    for ((_, _), value) in ranked_rows.iter().skip(1) {
        compare_orderable_field_values_with_slot(target_field, field_slot, first_value, value)?;
    }

    Ok(())
}

// Retain only the requested top/bottom-k ranked rows using bounded selection,
// then sort the retained window into final deterministic order.
fn apply_ranked_take_window<K, R>(
    target_field: &str,
    field_slot: FieldSlot,
    ranked_rows: &mut Vec<((K, R), Value)>,
    take_count: u32,
    direction: RankedFieldDirection,
) -> Result<(), AggregateFieldValueError>
where
    K: Ord,
{
    validate_ranked_value_domain(target_field, field_slot, ranked_rows.as_slice())?;

    let take_len = usize::try_from(take_count).unwrap_or(usize::MAX);
    if ranked_rows.len() > take_len && take_len > 0 {
        ranked_rows.select_nth_unstable_by(take_len - 1, |left, right| {
            compare_ranked_rows_infallible(target_field, field_slot, left, right, direction)
        });
        ranked_rows.truncate(take_len);
    }
    if take_len == 0 {
        ranked_rows.clear();
        return Ok(());
    }

    ranked_rows.sort_by(|left, right| {
        compare_ranked_rows_infallible(target_field, field_slot, left, right, direction)
    });

    Ok(())
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{RankedFieldDirection, apply_ranked_take_window, compare_ranked_keys_and_values};
    use crate::db::executor::aggregate::field::FieldSlot;
    use crate::{model::field::FieldKind, value::Value};
    use std::cmp::Ordering;

    fn uint_field_slot() -> FieldSlot {
        FieldSlot {
            index: 0,
            kind: FieldKind::Uint,
        }
    }

    #[test]
    fn compare_ranked_keys_and_values_desc_uses_value_then_key_order() {
        let ordering = compare_ranked_keys_and_values(
            "score",
            uint_field_slot(),
            &2_u64,
            &Value::Uint(9),
            &1_u64,
            &Value::Uint(7),
            RankedFieldDirection::Descending,
        )
        .expect("comparison");
        assert_eq!(ordering, Ordering::Less);

        let tie_break_ordering = compare_ranked_keys_and_values(
            "score",
            uint_field_slot(),
            &1_u64,
            &Value::Uint(7),
            &2_u64,
            &Value::Uint(7),
            RankedFieldDirection::Descending,
        )
        .expect("comparison");
        assert_eq!(tie_break_ordering, Ordering::Less);
    }

    #[test]
    fn apply_ranked_take_window_keeps_smallest_bottom_k_in_final_order() {
        let mut ranked_rows = vec![
            ((4_u64, ()), Value::Uint(40)),
            ((2_u64, ()), Value::Uint(20)),
            ((3_u64, ()), Value::Uint(30)),
            ((1_u64, ()), Value::Uint(10)),
        ];

        apply_ranked_take_window(
            "score",
            uint_field_slot(),
            &mut ranked_rows,
            2,
            RankedFieldDirection::Ascending,
        )
        .expect("bounded ranking");

        assert_eq!(
            ranked_rows,
            vec![
                ((1_u64, ()), Value::Uint(10)),
                ((2_u64, ()), Value::Uint(20))
            ],
        );
    }
}
