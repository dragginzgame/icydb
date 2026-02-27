pub(in crate::db) use crate::db::cursor::planned::GroupedPlannedCursor;
pub(in crate::db) use crate::db::cursor::token::GroupedContinuationToken;
#[allow(unused_imports)]
pub(crate) use crate::db::cursor::token::GroupedContinuationTokenError;
///
/// GROUPED CURSOR SCAFFOLD
///
/// WIP ownership note:
/// GROUP BY is intentionally isolated behind this module for now.
/// Keep grouped scaffold code behind this boundary for the time being and do not remove it.
///
/// Explicit ownership boundary for grouped cursor token/state scaffold.
/// This module gathers grouped cursor contracts and grouped cursor helpers under
/// one import surface.
///

#[allow(unused_imports)]
pub(in crate::db) use crate::db::cursor::{
    prepare_grouped_cursor, revalidate_grouped_cursor, validate_grouped_cursor_order_plan,
};
