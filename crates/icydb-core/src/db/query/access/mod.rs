//! Module: query::access
//! Responsibility: shared query-side access traversal helpers.
//! Does not own: explain rendering policy or planner access selection.
//! Boundary: centralized visitors over explain access variants.

mod access_visitor;

pub(in crate::db::query) use access_visitor::{AccessPathVisitor, visit_explain_access_path};
