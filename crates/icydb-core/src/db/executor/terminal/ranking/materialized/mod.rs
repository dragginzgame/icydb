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
                extract_orderable_field_value_with_slot_reader,
            },
            pipeline::contracts::LoadExecutor,
            terminal::{RowDecoder, RowLayout},
        },
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::{EntityKind, EntityValue},
    value::Value,
};
use std::cmp::Ordering;

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
    // Reduce one materialized response into deterministic top-k ranked rows
    // ordered by `(field_value_desc, primary_key_asc)`.
    pub(super) fn top_k_ranked_rows_from_materialized(
        model: &'static EntityModel,
        rows: &[DataRow],
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(DataRow, Value)>, InternalError> {
        rank_k_rows_from_materialized_structural(
            model,
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
        model: &'static EntityModel,
        rows: &[DataRow],
        target_field: &str,
        field_slot: FieldSlot,
        take_count: u32,
    ) -> Result<Vec<(DataRow, Value)>, InternalError> {
        rank_k_rows_from_materialized_structural(
            model,
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
// - No streaming heap optimization is used in 0.29 by design.
fn rank_k_rows_from_materialized_structural(
    model: &'static EntityModel,
    rows: &[DataRow],
    target_field: &str,
    field_slot: FieldSlot,
    take_count: u32,
    direction: RankedFieldDirection,
) -> Result<Vec<(DataRow, Value)>, InternalError> {
    let row_layout = RowLayout::from_model(model);
    let row_decoder = RowDecoder::structural();
    let mut ordered_rows: Vec<(DataRow, Value)> = Vec::new();

    // Phase 1: decode structural rows and compute one comparable target value
    // per candidate before ranking order is updated.
    for (data_key, raw_row) in rows {
        let row = row_decoder.decode(&row_layout, (data_key.clone(), raw_row.clone()))?;
        let value = extract_orderable_field_value_with_slot_reader(
            target_field,
            field_slot,
            &mut |index| row.slot(index),
        )
        .map_err(AggregateFieldValueError::into_internal_error)?;

        // Phase 2: insert the candidate into deterministic `(value, pk)` order.
        let mut insert_index = ordered_rows.len();
        for (index, ((current_key, _), current_value)) in ordered_rows.iter().enumerate() {
            let ordering = compare_orderable_field_values_with_slot(
                target_field,
                field_slot,
                &value,
                current_value,
            )
            .map_err(AggregateFieldValueError::into_internal_error)?;
            let outranks_current = direction.candidate_precedes(ordering);
            let tie_breaks_by_pk = ordering == Ordering::Equal && data_key < current_key;
            if outranks_current || tie_breaks_by_pk {
                insert_index = index;
                break;
            }
        }
        ordered_rows.insert(insert_index, ((data_key.clone(), raw_row.clone()), value));
    }

    // Phase 3: truncate to the requested top/bottom-k boundary.
    let take_len = usize::try_from(take_count).unwrap_or(usize::MAX);
    if ordered_rows.len() > take_len {
        ordered_rows.truncate(take_len);
    }

    Ok(ordered_rows)
}
