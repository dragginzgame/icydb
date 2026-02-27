#![allow(unused_imports)]

///
/// GROUPED EXECUTION SCAFFOLD
///
/// WIP ownership note:
/// GROUP BY is intentionally isolated behind this module for now.
/// Keep grouped scaffold code behind this boundary for the time being and do not remove it.
///
/// Explicit ownership boundary for grouped execution-route/reducer scaffold.
/// Grouped execution contracts are re-exported here so grouped runtime work has
/// one obvious executor entrypoint.
///
pub(in crate::db::executor) use crate::db::executor::aggregate::{
    GroupAggregateSpec, GroupAggregateSpecSupportError,
};
pub(in crate::db::executor) use crate::db::executor::route::ExecutionModeRouteCase;
