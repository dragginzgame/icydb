///
/// PostAccessStats
///
/// Post-access execution statistics.
///
/// Runtime currently consumes only:
/// - `rows_after_cursor` for continuation decisions
/// - `delete_was_limited` for delete diagnostics
///

pub(in crate::db::executor) struct PostAccessStats {
    pub(in crate::db::executor) delete_was_limited: bool,
    pub(in crate::db::executor) rows_after_cursor: usize,
}
