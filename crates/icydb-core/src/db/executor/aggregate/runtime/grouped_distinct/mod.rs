//! Module: executor::aggregate::runtime::grouped_distinct
//! Responsibility: grouped global DISTINCT field-target runtime handling.
//! Does not own: grouped planning policy or generic grouped fold mechanics.
//! Boundary: grouped DISTINCT special-case helpers used by grouped read execution.

mod aggregate;
mod paging;
mod strategy;

///
/// TESTS
///

#[cfg(test)]
mod tests;

pub(in crate::db::executor) use aggregate::execute_global_distinct_field_aggregate;
pub(in crate::db::executor) use paging::page_global_distinct_grouped_row;
pub(in crate::db::executor) use strategy::global_distinct_field_target_and_kind;
