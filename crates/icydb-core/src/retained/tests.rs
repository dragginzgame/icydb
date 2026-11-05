use super::RetainedBytes;
use crate::value::Value;
use std::{rc::Rc, sync::OnceLock};

#[test]
fn retained_capacity_counts_spare_buffers_and_nested_copies() {
    let mut text = String::with_capacity(4096);
    text.push('x');
    let mut values = Vec::with_capacity(8);
    values.push(text);
    let expected =
        size_of_val(&values) + values.capacity() * size_of::<String>() + values[0].capacity();
    assert_eq!(RetainedBytes::measure(&values, expected), Some(expected));
    assert_eq!(RetainedBytes::measure(&values, expected - 1), None);
    let shared = Rc::new(values);
    let first = RetainedBytes::measure(&shared, usize::MAX).expect("known allocations");
    let copies = (shared.clone(), shared);
    assert_eq!(RetainedBytes::measure(&copies, usize::MAX), Some(first * 2));
}

#[test]
fn retained_cells_charge_only_initialized_payloads() {
    let cell = OnceLock::new();
    let before = RetainedBytes::measure(&cell, usize::MAX).expect("empty cell");
    cell.set(vec![0_u8; 1024]).expect("initialize once");
    assert_eq!(
        RetainedBytes::measure(&cell, usize::MAX),
        Some(before + 1024)
    );
}

#[test]
fn retained_excessively_deep_values_and_overflow_decline_accounting() {
    let mut nested = Value::Null;
    for _ in 0..130 {
        nested = Value::List(vec![nested]);
    }
    assert_eq!(RetainedBytes::measure(&nested, usize::MAX), None);
    let mut bytes = RetainedBytes::new(usize::MAX);
    assert_eq!(bytes.add(usize::MAX), Some(()));
    assert_eq!(bytes.add(1), None);
}
