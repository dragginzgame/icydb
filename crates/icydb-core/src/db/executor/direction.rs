use crate::db::{data::DataKey, query::plan::Direction};

/// Normalize key ordering for ordered executor paths.
#[expect(clippy::ptr_arg)]
pub(crate) fn normalize_ordered_keys(
    keys: &mut Vec<DataKey>,
    direction: Direction,
    already_sorted: bool,
) {
    if !already_sorted {
        keys.sort_unstable();
    }
    if matches!(direction, Direction::Desc) {
        keys.reverse();
    }
}
