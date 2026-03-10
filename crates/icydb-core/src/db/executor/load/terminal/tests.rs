//! Module: db::executor::load::terminal::tests
//! Responsibility: module-local ownership and contracts for db::executor::load::terminal::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::value::Value;

use crate::db::executor::load::terminal::{
    bytes_window_accept_row, bytes_window_limit_exhausted, saturating_add_payload_len,
    serialized_value_len,
};

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
