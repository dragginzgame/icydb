//! Module: db::direction
//! Responsibility: module-local ownership and contracts for db::direction.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use serde::{Deserialize, Serialize};

///
/// Direction
///
/// Canonical traversal direction shared by query planning, executor runtime,
/// and index-range envelope handling.
///

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum Direction {
    #[default]
    Asc,
    Desc,
}
