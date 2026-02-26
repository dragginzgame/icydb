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
