//! Module: executor::aggregate::contracts::state::control
//! Responsibility: shared aggregate fold-control enums.
//! Does not own: aggregate reducer storage or stream traversal.
//! Boundary: carries reducer continuation and extrema selection decisions.

///
/// FoldControl
///
/// FoldControl tells aggregate execution kernels whether the current reducer can
/// stop scanning after one accepted input or must continue consuming rows.
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db::executor) enum FoldControl {
    Continue,
    Break,
}

///
/// ExtremumKind
///
/// ExtremumKind identifies the MIN/MAX reducer being applied by shared extrema
/// terminal update helpers.
/// It keeps the comparison and update choice explicit at the call site.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor::aggregate::contracts::state) enum ExtremumKind {
    Min,
    Max,
}

///
/// AggregateFoldMode
///
/// AggregateFoldMode describes to EXPLAIN whether the selected aggregate route
/// inspects existing rows or can use decoded key streams only.
///

#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AggregateFoldMode {
    ExistingRows,
    KeysOnly,
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
#[cfg(feature = "sql")]
crate::retained::retained_copy!(AggregateFoldMode);
