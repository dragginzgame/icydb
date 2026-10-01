//! Module: db::tests
//! Covers IC update-message guards and malformed persisted-format inputs.
//! Does not own: runtime scheduling, recovery, or storage encoding.
//! Boundary: protects cross-subsystem execution and decoding invariants.

mod ic_update_model;
mod persisted_format_corpus;
