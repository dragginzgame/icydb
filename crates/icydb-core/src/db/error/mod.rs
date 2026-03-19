//! Module: db::error
//!
//! Responsibility: DB-domain internal-error constructor and conversion boundary.
//! Does not own: core error taxonomy definitions (`ErrorClass`, `ErrorOrigin`, `InternalError`).
//! Boundary: db runtime/planner/executor/cursor/access code maps into taxonomy helpers here.

mod access;
mod cursor;
mod executor;
mod planner;
mod query;
#[cfg(test)]
mod test_support;

pub(crate) use access::from_executor_access_plan_error;
pub(crate) use cursor::{cursor_invariant, from_cursor_plan_error};
pub(crate) use executor::{
    executor_internal, executor_invariant, executor_invariant_message, executor_unsupported,
};
pub(crate) use planner::{planner_invariant, query_invalid_logical_plan};
pub(crate) use query::{query_executor_invariant, query_invariant};

#[cfg(test)]
pub(crate) use test_support::{from_group_plan_error, plan_invariant_violation};
