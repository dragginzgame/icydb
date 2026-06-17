//! Module: executor::aggregate::contracts::state::control
//! Responsibility: shared aggregate fold-control enums.
//! Does not own: aggregate reducer storage or stream traversal.
//! Boundary: carries reducer continuation and extrema direction decisions.

use crate::db::direction::Direction;

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
/// It carries the labels needed to preserve existing invariant errors while
/// keeping the ordering decision explicit at the call site.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor::aggregate::contracts::state) enum ExtremumKind {
    Min,
    Max,
}

impl ExtremumKind {
    // Return the expression-input label used by invariant errors for this
    // extrema aggregate.
    pub(in crate::db::executor::aggregate::contracts::state) const fn expression_label(
        self,
    ) -> &'static str {
        match self {
            Self::Min => "MIN(expr)",
            Self::Max => "MAX(expr)",
        }
    }

    // Return the field-input label used by invariant errors for this extrema
    // aggregate.
    pub(in crate::db::executor::aggregate::contracts::state) const fn field_label(
        self,
    ) -> &'static str {
        match self {
            Self::Min => "MIN(field)",
            Self::Max => "MAX(field)",
        }
    }

    // Return the primary-key value label used by invariant errors for this
    // extrema aggregate.
    pub(in crate::db::executor::aggregate::contracts::state) const fn primary_key_value_label(
        self,
    ) -> &'static str {
        match self {
            Self::Min => "MIN",
            Self::Max => "MAX",
        }
    }

    // Return the extrema early-termination decision for one ordered input
    // direction.
    pub(in crate::db::executor::aggregate::contracts::state) const fn fold_control_for_direction(
        self,
        direction: Direction,
    ) -> FoldControl {
        match (self, direction) {
            (Self::Min, Direction::Asc) | (Self::Max, Direction::Desc) => FoldControl::Break,
            _ => FoldControl::Continue,
        }
    }
}

///
/// AggregateFoldMode
///
/// AggregateFoldMode selects whether aggregate execution must inspect existing
/// rows or can satisfy the request from decoded key streams only.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AggregateFoldMode {
    ExistingRows,
    KeysOnly,
}
