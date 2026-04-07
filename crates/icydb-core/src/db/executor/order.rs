//! Module: executor::order
//! Responsibility: shared structural ordering helpers for executor row paths.
//! Does not own: planner order semantics or cursor wire validation.
//! Boundary: resolves order slots once and applies canonical ordering over slot-readable rows.

use crate::{
    db::{
        contracts::canonical_value_compare,
        cursor::{CursorBoundary, CursorBoundarySlot, apply_order_direction},
        query::plan::{ExpressionOrderTerm, OrderDirection, OrderSpec},
        scalar_expr::derive_expression_order_value,
    },
    error::InternalError,
    model::entity::{EntityModel, resolve_field_slot},
    value::Value,
};
use std::{borrow::Cow, cmp::Ordering};

///
/// OrderReadableRow
///
/// Structural executor row contract used by shared ordering logic.
/// Implementors expose slot-indexed values without re-entering typed entity
/// comparators in sort and cursor-boundary hot loops.
///

pub(in crate::db::executor) trait OrderReadableRow {
    /// Read one slot value for structural ordering and predicate evaluation.
    /// Structural row paths may return borrowed values so shared order/cursor
    /// helpers do not clone already-decoded slots in comparator hot loops.
    fn read_order_slot_cow(&self, slot: usize) -> Option<Cow<'_, Value>>;

    /// Read one slot value as an owned payload when a caller still needs to
    /// leave the borrowed structural-ordering boundary.
    fn read_order_slot(&self, slot: usize) -> Option<Value> {
        self.read_order_slot_cow(slot).map(Cow::into_owned)
    }
}

///
/// ResolvedOrderField
///
/// One order slot resolved from field name to model slot index.
/// Shared structural ordering keeps this resolved shape outside comparator
/// loops so sorting does not repeat field-name lookups.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResolvedOrderValueSource {
    Missing,
    DirectField(usize),
    ExpressionLower(usize),
    ExpressionUpper(usize),
}

impl ResolvedOrderValueSource {
    // Resolve one canonical ORDER BY field reference into its structural row source.
    fn from_field_name(model: &EntityModel, field: &str) -> Self {
        if let Some(expression) = ExpressionOrderTerm::parse(field) {
            let Some(slot) = resolve_field_slot(model, expression.field()) else {
                return Self::Missing;
            };

            return match expression {
                ExpressionOrderTerm::Lower(_) => Self::ExpressionLower(slot),
                ExpressionOrderTerm::Upper(_) => Self::ExpressionUpper(slot),
            };
        }

        resolve_field_slot(model, field).map_or(Self::Missing, Self::DirectField)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResolvedOrderField {
    source: ResolvedOrderValueSource,
    direction: OrderDirection,
}

///
/// ResolvedOrder
///
/// Slot-resolved canonical ordering shape shared by executor row paths.
/// This keeps sorting and cursor-boundary comparisons structural once the
/// planner has already fixed the visible order contract.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ResolvedOrder {
    fields: Vec<ResolvedOrderField>,
}

impl ResolvedOrder {
    fn iter(&self) -> impl Iterator<Item = ResolvedOrderField> + '_ {
        self.fields.iter().copied()
    }
}

/// Resolve one order spec into slot indexes for structural executor use.
#[must_use]
pub(in crate::db::executor) fn resolve_structural_order(
    model: &EntityModel,
    order: &OrderSpec,
) -> ResolvedOrder {
    let fields = order
        .fields
        .iter()
        .map(|(field, direction)| ResolvedOrderField {
            source: ResolvedOrderValueSource::from_field_name(model, field),
            direction: *direction,
        })
        .collect();

    ResolvedOrder { fields }
}

/// Mark every structural slot that one ORDER BY contract needs at runtime.
pub(in crate::db::executor) fn mark_structural_order_slots(
    model: &EntityModel,
    order: &OrderSpec,
    required_slots: &mut [bool],
) -> Result<(), InternalError> {
    // Phase 1: resolve each order term onto the canonical structural slot
    // source and reject unknown field references up front.
    for (field, _) in &order.fields {
        match ResolvedOrderValueSource::from_field_name(model, field) {
            ResolvedOrderValueSource::Missing => {
                return Err(InternalError::query_invalid_logical_plan(format!(
                    "order expression references unknown field '{field}'",
                )));
            }
            ResolvedOrderValueSource::DirectField(slot)
            | ResolvedOrderValueSource::ExpressionLower(slot)
            | ResolvedOrderValueSource::ExpressionUpper(slot) => {
                if let Some(required) = required_slots.get_mut(slot) {
                    *required = true;
                }
            }
        }
    }

    Ok(())
}

/// Apply canonical in-memory ordering with an optional bounded top-k window.
pub(in crate::db::executor) fn apply_structural_order_window<R>(
    rows: &mut Vec<R>,
    resolved_order: &ResolvedOrder,
    keep_count: Option<usize>,
) where
    R: OrderReadableRow,
{
    if let Some(keep_count) = keep_count {
        if keep_count == 0 {
            rows.clear();
            return;
        }

        if rows.len() > keep_count {
            rows.select_nth_unstable_by(keep_count - 1, |left, right| {
                compare_orderable_rows(left, right, resolved_order)
            });
            rows.truncate(keep_count);
        }
    }

    sort_structural_rows(rows.as_mut_slice(), resolved_order);
}

/// Compare one structural row against one cursor boundary under the canonical order contract.
pub(in crate::db::executor) fn compare_orderable_row_with_boundary<R>(
    row: &R,
    resolved_order: &ResolvedOrder,
    boundary: &CursorBoundary,
) -> Ordering
where
    R: OrderReadableRow,
{
    compare_structural_order_slots(resolved_order, |slot_index, field_index, direction| {
        let row_slot = order_value_from_row(row, field_index);
        let boundary_slot = boundary
            .slots
            .get(slot_index)
            .expect("cursor boundary must align with resolved order");

        apply_order_direction(
            compare_order_value_with_boundary(row_slot, boundary_slot),
            direction,
        )
    })
}

// Compare two structural rows according to the resolved canonical order.
fn compare_orderable_rows(
    left: &dyn OrderReadableRow,
    right: &dyn OrderReadableRow,
    resolved_order: &ResolvedOrder,
) -> Ordering {
    compare_structural_order_slots(resolved_order, |_slot_index, field_index, direction| {
        let left_slot = order_value_from_row(left, field_index);
        let right_slot = order_value_from_row(right, field_index);

        apply_order_direction(compare_order_values(left_slot, right_slot), direction)
    })
}

// Apply the canonical shared in-memory sort contract over one structural row slice.
fn sort_structural_rows<R>(rows: &mut [R], resolved_order: &ResolvedOrder)
where
    R: OrderReadableRow,
{
    rows.sort_by(|left, right| compare_orderable_rows(left, right, resolved_order));
}

// Compare one structural ordering tuple by resolving slot pairs lazily in canonical field order.
fn compare_structural_order_slots<F>(
    resolved_order: &ResolvedOrder,
    mut compare_slot: F,
) -> Ordering
where
    F: FnMut(usize, ResolvedOrderValueSource, OrderDirection) -> Ordering,
{
    for (slot_index, slot) in resolved_order.iter().enumerate() {
        let ordering = compare_slot(slot_index, slot.source, slot.direction);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

// Borrow one slot-reader value through the shared ordering seam.
fn order_value_from_row(
    row: &dyn OrderReadableRow,
    source: ResolvedOrderValueSource,
) -> Option<Cow<'_, Value>> {
    match source {
        ResolvedOrderValueSource::Missing => None,
        ResolvedOrderValueSource::DirectField(slot) => row.read_order_slot_cow(slot),
        ResolvedOrderValueSource::ExpressionLower(slot) => {
            derive_expression_order_row_value(row, slot, ExpressionOrderTerm::Lower(""))
                .map(Cow::Owned)
        }
        ResolvedOrderValueSource::ExpressionUpper(slot) => {
            derive_expression_order_row_value(row, slot, ExpressionOrderTerm::Upper(""))
                .map(Cow::Owned)
        }
    }
}

// Derive one owned expression-order value from one structural row slot.
fn derive_expression_order_row_value(
    row: &dyn OrderReadableRow,
    slot: usize,
    term: ExpressionOrderTerm<'_>,
) -> Option<Value> {
    let value = row.read_order_slot_cow(slot)?;

    derive_expression_order_value(term, value.as_ref())
}

// Compare two optional structural ordering values under cursor boundary
// semantics without forcing row paths to materialize owned `Value`s first.
fn compare_order_values(left: Option<Cow<'_, Value>>, right: Option<Cow<'_, Value>>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => canonical_value_compare(left.as_ref(), right.as_ref()),
    }
}

// Compare one row-provided ordering value against one persisted cursor
// boundary slot without rebuilding the row side into an owned boundary slot.
fn compare_order_value_with_boundary(
    value: Option<Cow<'_, Value>>,
    boundary: &CursorBoundarySlot,
) -> Ordering {
    match (value, boundary) {
        (None, CursorBoundarySlot::Missing) => Ordering::Equal,
        (None, CursorBoundarySlot::Present(_)) => Ordering::Less,
        (Some(_), CursorBoundarySlot::Missing) => Ordering::Greater,
        (Some(value), CursorBoundarySlot::Present(boundary_value)) => {
            canonical_value_compare(value.as_ref(), boundary_value)
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    struct TestRow {
        slots: Vec<Option<Value>>,
    }

    impl TestRow {
        fn new(slots: Vec<Option<Value>>) -> Self {
            Self { slots }
        }
    }

    impl OrderReadableRow for TestRow {
        fn read_order_slot_cow(&self, slot: usize) -> Option<Cow<'_, Value>> {
            self.slots
                .get(slot)
                .and_then(Option::as_ref)
                .map(Cow::Borrowed)
        }
    }

    fn resolved_order(fields: &[(Option<usize>, OrderDirection)]) -> ResolvedOrder {
        ResolvedOrder {
            fields: fields
                .iter()
                .map(|(field_index, direction)| ResolvedOrderField {
                    source: field_index
                        .map(ResolvedOrderValueSource::DirectField)
                        .unwrap_or(ResolvedOrderValueSource::Missing),
                    direction: *direction,
                })
                .collect(),
        }
    }

    #[test]
    fn apply_structural_order_sorts_rows_by_resolved_slots() {
        let mut rows = vec![
            TestRow::new(vec![Some(Value::Uint(3))]),
            TestRow::new(vec![Some(Value::Uint(1))]),
            TestRow::new(vec![Some(Value::Uint(2))]),
        ];

        apply_structural_order_window(
            &mut rows,
            &resolved_order(&[(Some(0), OrderDirection::Asc)]),
            None,
        );

        let ordered = rows
            .into_iter()
            .map(|row| row.read_order_slot(0))
            .collect::<Vec<_>>();
        assert_eq!(
            ordered,
            vec![
                Some(Value::Uint(1)),
                Some(Value::Uint(2)),
                Some(Value::Uint(3))
            ]
        );
    }

    #[test]
    fn apply_structural_order_bounded_keeps_smallest_rows_in_canonical_order() {
        let mut rows = vec![
            TestRow::new(vec![Some(Value::Uint(4))]),
            TestRow::new(vec![Some(Value::Uint(2))]),
            TestRow::new(vec![Some(Value::Uint(3))]),
            TestRow::new(vec![Some(Value::Uint(1))]),
        ];

        apply_structural_order_window(
            &mut rows,
            &resolved_order(&[(Some(0), OrderDirection::Asc)]),
            Some(2),
        );

        let ordered = rows
            .into_iter()
            .map(|row| row.read_order_slot(0))
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec![Some(Value::Uint(1)), Some(Value::Uint(2))]);
    }

    #[test]
    fn compare_orderable_row_with_boundary_respects_desc_direction() {
        let row = TestRow::new(vec![Some(Value::Uint(7))]);
        let boundary = CursorBoundary {
            slots: vec![CursorBoundarySlot::Present(Value::Uint(5))],
        };

        let ordering = compare_orderable_row_with_boundary(
            &row,
            &resolved_order(&[(Some(0), OrderDirection::Desc)]),
            &boundary,
        );

        assert_eq!(ordering, Ordering::Less);
    }
}
