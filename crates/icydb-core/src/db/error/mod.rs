//! Module: db::error
//!
//! Responsibility: DB-domain internal-error constructor and conversion boundary.
//! Does not own: core error taxonomy definitions (`ErrorClass`, `ErrorOrigin`, `InternalError`).
//! Boundary: db runtime/planner/executor/cursor/access code maps into taxonomy helpers here.

#[cfg(test)]
mod test_support;

#[cfg(test)]
pub(crate) use test_support::{from_group_plan_error, plan_invariant_violation};
