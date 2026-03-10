//! Module: executor::load::grouped_distinct
//! Responsibility: grouped global DISTINCT field-target runtime handling.
//! Does not own: grouped planning policy or generic grouped fold mechanics.
//! Boundary: grouped DISTINCT special-case helpers used by load grouped execution.

mod aggregate;
mod paging;
