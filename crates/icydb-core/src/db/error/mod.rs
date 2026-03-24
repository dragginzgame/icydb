//! Module: db::error
//!
//! Responsibility: DB-domain internal-error constructor and conversion boundary.
//! Does not own: core error taxonomy definitions (`ErrorClass`, `ErrorOrigin`, `InternalError`).
//! Boundary: db runtime/planner/executor/cursor/access code maps into taxonomy helpers here.

mod access;
mod cursor;
#[cfg(test)]
mod test_support;

pub(crate) use access::from_executor_access_plan_error;
pub(crate) use cursor::from_cursor_plan_error;

#[cfg(test)]
pub(crate) use test_support::{from_group_plan_error, plan_invariant_violation};
