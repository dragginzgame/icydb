use super::*;
use crate::{
    db::data::{CanonicalRow, with_structural_read_metrics},
    db::query::plan::{ResolvedOrderField, expr::CompiledExpr},
    entity::EntityDeclaration,
    model::field::FieldKind,
    types::{Blob, Text, Ulid},
    value::Value,
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;
use std::{borrow::Cow, cell::Cell, rc::Rc};

struct TestRow {
    slots: Vec<Option<Value>>,
}

impl TestRow {
    fn new(slots: Vec<Option<Value>>) -> Self {
        Self { slots }
    }
}

impl OrderReadableRow for TestRow {
    fn read_order_slot_ref(&self, slot: usize) -> Option<&Value> {
        self.slots.get(slot).and_then(Option::as_ref)
    }

    fn read_order_slot_cow(&self, slot: usize) -> Option<Cow<'_, Value>> {
        self.slots
            .get(slot)
            .and_then(Option::as_ref)
            .map(Cow::Borrowed)
    }
}

struct CountingRow {
    reads: Rc<Cell<usize>>,
    borrowed: bool,
    slots: Vec<Option<Value>>,
}

impl CountingRow {
    fn new(reads: Rc<Cell<usize>>, slots: Vec<Option<Value>>) -> Self {
        Self {
            reads,
            borrowed: false,
            slots,
        }
    }

    fn borrowed(reads: Rc<Cell<usize>>, slots: Vec<Option<Value>>) -> Self {
        Self {
            reads,
            borrowed: true,
            slots,
        }
    }
}

impl OrderReadableRow for CountingRow {
    fn read_order_slot_ref(&self, slot: usize) -> Option<&Value> {
        self.reads.set(self.reads.get().saturating_add(1));
        self.slots.get(slot).and_then(Option::as_ref)
    }

    fn read_order_slot_cow(&self, slot: usize) -> Option<Cow<'_, Value>> {
        self.reads.set(self.reads.get().saturating_add(1));
        self.slots
            .get(slot)
            .and_then(Option::as_ref)
            .map(Cow::Borrowed)
    }

    fn order_slots_are_borrowed(&self) -> bool {
        self.borrowed
    }
}

fn resolved_order(fields: &[(usize, OrderDirection)]) -> ResolvedOrder {
    ResolvedOrder::new(
        fields
            .iter()
            .map(|(field_index, direction)| {
                ResolvedOrderField::new(
                    ResolvedOrderValueSource::direct_field(*field_index),
                    *direction,
                )
            })
            .collect(),
    )
}

fn add_expression_order(direction: OrderDirection) -> ResolvedOrder {
    ResolvedOrder::new(vec![ResolvedOrderField::new(
        ResolvedOrderValueSource::expression(CompiledExpr::Add {
            left_slot: 0,
            left_field: "age".to_string(),
            right_slot: 1,
            right_field: "rank".to_string(),
        }),
        direction,
    )])
}

#[test]
fn apply_structural_order_sorts_rows_by_resolved_slots() {
    let mut rows = vec![
        TestRow::new(vec![Some(Value::Nat64(3))]),
        TestRow::new(vec![Some(Value::Nat64(1))]),
        TestRow::new(vec![Some(Value::Nat64(2))]),
    ];

    apply_structural_order_window(
        &mut rows,
        &resolved_order(&[(0, OrderDirection::Asc)]),
        None,
    );

    let ordered = rows
        .into_iter()
        .map(|row| row.read_order_slot(0))
        .collect::<Vec<_>>();
    assert_eq!(
        ordered,
        vec![
            Some(Value::Nat64(1)),
            Some(Value::Nat64(2)),
            Some(Value::Nat64(3))
        ]
    );
}

#[test]
fn apply_structural_order_bounded_keeps_smallest_rows_in_canonical_order() {
    let mut rows = vec![
        TestRow::new(vec![Some(Value::Nat64(4))]),
        TestRow::new(vec![Some(Value::Nat64(2))]),
        TestRow::new(vec![Some(Value::Nat64(3))]),
        TestRow::new(vec![Some(Value::Nat64(1))]),
    ];

    apply_structural_order_window(
        &mut rows,
        &resolved_order(&[(0, OrderDirection::Asc)]),
        Some(2),
    );

    let ordered = rows
        .into_iter()
        .map(|row| row.read_order_slot(0))
        .collect::<Vec<_>>();
    assert_eq!(ordered, vec![Some(Value::Nat64(1)), Some(Value::Nat64(2))]);
}

#[test]
fn compare_orderable_row_with_boundary_respects_desc_direction() {
    let row = TestRow::new(vec![Some(Value::Nat64(7))]);
    let boundary = CursorBoundary {
        slots: vec![CursorBoundarySlot::Present(Value::Nat64(5))],
    };

    let ordering = compare_orderable_row_with_boundary(
        &row,
        &resolved_order(&[(0, OrderDirection::Desc)]),
        &boundary,
    )
    .expect("valid cursor boundary should compare");

    assert_eq!(ordering, Ordering::Less);
}

#[test]
fn compare_orderable_row_with_boundary_rejects_short_boundary() {
    let row = TestRow::new(vec![Some(Value::Nat64(7))]);
    let boundary = CursorBoundary { slots: Vec::new() };

    assert!(
        compare_orderable_row_with_boundary(
            &row,
            &resolved_order(&[(0, OrderDirection::Asc)]),
            &boundary,
        )
        .is_err()
    );
}

#[test]
fn apply_structural_order_window_caches_slot_reads_once_per_row() {
    let left_reads = Rc::new(Cell::new(0));
    let middle_reads = Rc::new(Cell::new(0));
    let right_reads = Rc::new(Cell::new(0));
    let mut rows = vec![
        CountingRow::new(left_reads.clone(), vec![Some(Value::Nat64(3))]),
        CountingRow::new(middle_reads.clone(), vec![Some(Value::Nat64(1))]),
        CountingRow::new(right_reads.clone(), vec![Some(Value::Nat64(2))]),
    ];

    apply_structural_order_window(
        &mut rows,
        &resolved_order(&[(0, OrderDirection::Asc)]),
        Some(2),
    );

    assert_eq!(left_reads.get(), 1);
    assert_eq!(middle_reads.get(), 1);
    assert_eq!(right_reads.get(), 1);
}

#[test]
fn apply_structural_order_window_uses_borrowed_direct_slot_fast_path() {
    let left_reads = Rc::new(Cell::new(0));
    let middle_reads = Rc::new(Cell::new(0));
    let right_reads = Rc::new(Cell::new(0));
    let mut rows = vec![
        CountingRow::borrowed(left_reads.clone(), vec![Some(Value::Nat64(3))]),
        CountingRow::borrowed(middle_reads.clone(), vec![Some(Value::Nat64(1))]),
        CountingRow::borrowed(right_reads.clone(), vec![Some(Value::Nat64(2))]),
    ];

    apply_structural_order_window(
        &mut rows,
        &resolved_order(&[(0, OrderDirection::Asc)]),
        Some(2),
    );

    let ordered = rows
        .iter()
        .map(|row| row.read_order_slot(0))
        .collect::<Vec<_>>();
    assert_eq!(ordered, vec![Some(Value::Nat64(1)), Some(Value::Nat64(2))]);
    assert!(
        left_reads.get() + middle_reads.get() + right_reads.get() > 3,
        "borrowed direct-slot fast path should compare row slots directly instead of using the one-read cache",
    );
}

#[test]
fn bounded_direct_order_window_keeps_best_rows_without_sorting() {
    let order = resolved_order(&[(0, OrderDirection::Asc)]);
    let mut window = BoundedDirectOrderWindow::new(2);
    for value in [4, 1, 3, 2] {
        window.push(TestRow::new(vec![Some(Value::Nat64(value))]), &order);
    }
    let mut rows = window.into_rows();

    apply_structural_order_window(&mut rows, &order, Some(2));

    let ordered = rows
        .into_iter()
        .map(|row| row.read_order_slot(0))
        .collect::<Vec<_>>();
    assert_eq!(ordered, vec![Some(Value::Nat64(1)), Some(Value::Nat64(2))]);
}

#[test]
fn bounded_order_window_caches_expression_keys_once_and_keeps_complete_order() {
    let order = ResolvedOrder::new(vec![
        ResolvedOrderField::new(
            ResolvedOrderValueSource::expression(CompiledExpr::Add {
                left_slot: 0,
                left_field: "age".to_string(),
                right_slot: 1,
                right_field: "rank".to_string(),
            }),
            OrderDirection::Asc,
        ),
        ResolvedOrderField::new(
            ResolvedOrderValueSource::direct_field(2),
            OrderDirection::Asc,
        ),
    ]);
    let reads = (0..4).map(|_| Rc::new(Cell::new(0))).collect::<Vec<_>>();
    let mut window = BoundedOrderWindow::new(3, &order);
    for (reads, slots) in reads.iter().zip([
        vec![
            Some(Value::Nat64(5)),
            Some(Value::Nat64(5)),
            Some(Value::Nat64(2)),
        ],
        vec![
            Some(Value::Nat64(3)),
            Some(Value::Nat64(3)),
            Some(Value::Nat64(4)),
        ],
        vec![
            Some(Value::Nat64(2)),
            Some(Value::Nat64(4)),
            Some(Value::Nat64(3)),
        ],
        vec![
            Some(Value::Nat64(9)),
            Some(Value::Nat64(9)),
            Some(Value::Nat64(1)),
        ],
    ]) {
        window.push(CountingRow::borrowed(reads.clone(), slots));
    }

    assert!(
        reads.iter().all(|reads| reads.get() == 3),
        "each candidate should evaluate two expression slots and one tie-break slot exactly once",
    );

    let rows = window
        .into_pending_rows()
        .apply_order(&order, Some(3))
        .expect("matching cached expression order should remain valid");
    assert!(
        reads.iter().all(|reads| reads.get() == 3),
        "canonical final ordering should reuse the scan-evaluated expression tuples",
    );
    let ids = rows
        .iter()
        .map(|row| row.read_order_slot(2))
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        vec![
            Some(Value::Nat64(3)),
            Some(Value::Nat64(4)),
            Some(Value::Nat64(2)),
        ],
    );
}

#[test]
fn cached_bounded_order_window_rejects_a_different_order_contract() {
    let order = add_expression_order(OrderDirection::Asc);
    let mut window = BoundedOrderWindow::new(2, &order);
    window.push(TestRow::new(vec![
        Some(Value::Nat64(3)),
        Some(Value::Nat64(2)),
    ]));
    window.push(TestRow::new(vec![
        Some(Value::Nat64(1)),
        Some(Value::Nat64(1)),
    ]));

    let descending_order = add_expression_order(OrderDirection::Desc);
    let Err(error) = window
        .into_pending_rows()
        .apply_order(&descending_order, Some(2))
    else {
        panic!("a different resolved order must not consume cached values");
    };

    assert_eq!(
        error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
    );
}

#[test]
fn cached_bounded_order_window_rejects_a_different_keep_count() {
    let order = add_expression_order(OrderDirection::Asc);
    let mut window = BoundedOrderWindow::new(2, &order);
    window.push(TestRow::new(vec![
        Some(Value::Nat64(3)),
        Some(Value::Nat64(2)),
    ]));

    let Err(error) = window.into_pending_rows().apply_order(&order, Some(1)) else {
        panic!("a different bounded keep count must not consume cached values");
    };

    assert_eq!(
        error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
    );
}

#[test]
fn cached_bounded_order_rows_reject_a_plain_row_terminal() {
    let order = add_expression_order(OrderDirection::Asc);
    let mut window = BoundedOrderWindow::new(1, &order);
    window.push(TestRow::new(vec![
        Some(Value::Nat64(3)),
        Some(Value::Nat64(2)),
    ]));

    let Err(error) = window.into_pending_rows().into_plain_rows() else {
        panic!("cached expression-order rows must not enter a plain-row terminal");
    };

    assert_eq!(
        error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
    );
}

#[test]
fn cached_bounded_order_window_handles_empty_and_singleton_results() {
    let order = add_expression_order(OrderDirection::Asc);
    let empty = BoundedOrderWindow::<TestRow>::new(2, &order)
        .into_pending_rows()
        .apply_order(&order, Some(2))
        .expect("an empty cached order window should remain valid");
    assert!(empty.is_empty());

    let mut singleton = BoundedOrderWindow::new(2, &order);
    singleton.push(TestRow::new(vec![
        Some(Value::Nat64(3)),
        Some(Value::Nat64(2)),
    ]));
    let singleton = singleton
        .into_pending_rows()
        .apply_order(&order, Some(2))
        .expect("a singleton cached order window should remain valid");

    assert_eq!(singleton.len(), 1);
    assert_eq!(singleton[0].read_order_slot(0), Some(Value::Nat64(3)));
}

crate::test_canister! {
    ident = OrderWindowCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = OrderWindowStore,
    canister = OrderWindowCanister,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct OrderWindowEntity {
    id: Ulid,
    title: Text,
    tags: Vec<Text>,
    portrait: Blob,
    x: u64,
    y: u64,
}

crate::test_entity! {
    ident = OrderWindowEntity,
    entity_name = "OrderWindowEntity",
    tag = crate::testing::PROBE_ENTITY_TAG,
    store = OrderWindowStore,
    canister = OrderWindowCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { title: Text => FieldKind::Text { max_len: None } },
        crate::test_field! { tags: Vec<Text> => FieldKind::List(&FieldKind::Text { max_len: None }) },
        crate::test_field! { portrait: Blob => FieldKind::Blob { max_len: None } },
        crate::test_field! { x: u64 => FieldKind::Nat64 },
        crate::test_field! { y: u64 => FieldKind::Nat64 },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

fn direct_data_row(entity: &OrderWindowEntity) -> DataRow {
    let key = crate::db::data::DecodedDataStoreKey::try_new::<OrderWindowEntity>(entity.id)
        .expect("test key construction should succeed");
    let row = CanonicalRow::from_entity_with_model_proposal_for_test(entity)
        .expect("test row serialization should succeed")
        .into_raw_row();

    (key, row)
}

#[test]
fn cursor_boundary_from_orderable_row_handles_heap_cached_values() {
    let row = TestRow::new(vec![
        Some(Value::Nat64(1)),
        Some(Value::Nat64(2)),
        Some(Value::Nat64(3)),
        Some(Value::Nat64(4)),
        Some(Value::Nat64(5)),
    ]);
    let boundary = cursor_boundary_from_orderable_row(
        &row,
        &resolved_order(&[
            (0, OrderDirection::Asc),
            (1, OrderDirection::Asc),
            (2, OrderDirection::Asc),
            (3, OrderDirection::Asc),
            (4, OrderDirection::Asc),
        ]),
    );

    assert_eq!(
        boundary.slots,
        vec![
            CursorBoundarySlot::Present(Value::Nat64(1)),
            CursorBoundarySlot::Present(Value::Nat64(2)),
            CursorBoundarySlot::Present(Value::Nat64(3)),
            CursorBoundarySlot::Present(Value::Nat64(4)),
            CursorBoundarySlot::Present(Value::Nat64(5)),
        ]
    );
}

#[test]
fn direct_data_row_order_window_uses_sparse_direct_field_decode() {
    let alpha = OrderWindowEntity {
        id: Ulid::from_u128(1),
        title: "alpha".to_string(),
        tags: vec!["one".to_string(), "two".to_string()],
        portrait: Blob::from(vec![0x10, 0x20, 0x30]),
        x: 0,
        y: 0,
    };
    let beta = OrderWindowEntity {
        id: Ulid::from_u128(2),
        title: "beta".to_string(),
        tags: vec!["three".to_string()],
        portrait: Blob::from(vec![0x40, 0x50, 0x60]),
        x: 0,
        y: 0,
    };
    let mut rows = vec![direct_data_row(&beta), direct_data_row(&alpha)];

    let (_result, metrics) = with_structural_read_metrics(|| {
        apply_structural_order_window_to_data_rows(
            &mut rows,
            RowLayout::from_model_proposal_for_test(OrderWindowEntity::MODEL),
            &resolved_order(&[(1, OrderDirection::Asc)]),
            None,
        )
    });

    assert_eq!(
        rows[0]
            .1
            .try_decode_with_model_proposal_for_test::<OrderWindowEntity>()
            .unwrap(),
        alpha
    );
    assert_eq!(
        rows[1]
            .1
            .try_decode_with_model_proposal_for_test::<OrderWindowEntity>()
            .unwrap(),
        beta
    );
    assert_eq!(metrics.rows_opened, 2);
    assert_eq!(
        metrics.declared_slots_validated, 2,
        "pure direct-field ordering should validate only the ordered slot per row",
    );
    assert_eq!(
        metrics.validated_non_scalar_slots, 0,
        "direct-field ordering should not validate untouched non-scalar slots",
    );
    assert_eq!(
        metrics.materialized_non_scalar_slots, 0,
        "direct-field ordering should leave untouched non-scalar slots unmaterialized",
    );
    assert_eq!(metrics.rows_without_lazy_non_scalar_materializations, 2);
}

#[test]
fn direct_data_row_order_window_respects_mixed_field_directions() {
    let low = OrderWindowEntity {
        id: Ulid::from_u128(11),
        title: "low".to_string(),
        tags: Vec::new(),
        portrait: Blob::from(Vec::new()),
        x: 0,
        y: 0,
    };
    let high = OrderWindowEntity {
        id: Ulid::from_u128(12),
        title: "high".to_string(),
        tags: Vec::new(),
        portrait: Blob::from(Vec::new()),
        x: 0,
        y: 2,
    };
    let next = OrderWindowEntity {
        id: Ulid::from_u128(13),
        title: "next".to_string(),
        tags: Vec::new(),
        portrait: Blob::from(Vec::new()),
        x: 1,
        y: 1,
    };
    let mut rows = vec![
        direct_data_row(&low),
        direct_data_row(&high),
        direct_data_row(&next),
    ];

    apply_structural_order_window_to_data_rows(
        &mut rows,
        RowLayout::from_model_proposal_for_test(OrderWindowEntity::MODEL),
        &resolved_order(&[(4, OrderDirection::Asc), (5, OrderDirection::Desc)]),
        Some(2),
    )
    .expect("mixed direct data-row order should sort");

    let ordered = rows
        .iter()
        .map(|(_, row)| {
            row.try_decode_with_model_proposal_for_test::<OrderWindowEntity>()
                .unwrap()
                .title
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ordered,
        vec!["high".to_string(), "low".to_string()],
        "ORDER BY x ASC, y DESC must not collapse to raw x/y ascending row-key order",
    );
}
