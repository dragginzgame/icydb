//! Module: db::direction
//! Responsibility: canonical traversal direction shared across db subsystems.
//! Does not own: order-by planning semantics or cursor policy.
//! Boundary: stable ascending/descending contract for planning and execution.

use serde::Deserialize;

///
/// Direction
///
/// Canonical traversal direction shared by query planning, executor runtime,
/// and index-range envelope handling.
///

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub(in crate::db) enum Direction {
    /// Ascending traversal.
    #[default]
    Asc,
    /// Descending traversal.
    Desc,
}
