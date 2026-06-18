//! Module: db::test_support
//! Responsibility: db-local test helper modules.
//! Does not own: runtime test fixtures or production support APIs.
//! Boundary: exposes helpers only inside db test code.

pub(in crate::db) mod source_guard;
