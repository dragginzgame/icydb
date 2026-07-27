//! Module: db::executor::terminal::tests
//! Covers terminal paging, shaping, and output invariants in executor
//! materialization paths.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::terminal::{bytes_window_accept_row, bytes_window_limit_exhausted};

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
