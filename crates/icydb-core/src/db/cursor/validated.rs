//! Module: cursor::validated
//! Responsibility: executor-facing validated cursor state after validation.
//! Does not own: cursor validation policy derivation or token wire encoding.
//! Boundary: carries validated cursor boundary/anchor/offset state into runtime execution.

use crate::value::Value;

///
/// ValidatedGroupedCursor
///
/// Executor-facing grouped continuation state produced after grouped cursor
/// validation for grouped pagination.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ValidatedGroupedCursor {
    last_group_key: Option<Vec<Value>>,
    initial_offset: u32,
}

impl ValidatedGroupedCursor {
    #[must_use]
    pub(in crate::db) const fn none() -> Self {
        Self {
            last_group_key: None,
            initial_offset: 0,
        }
    }

    /// Construct grouped executor cursor state after grouped cursor validation.
    ///
    /// Normal grouped cursor input flows through grouped cursor preparation
    /// before this state reaches the executor.
    #[must_use]
    pub(in crate::db) const fn new_validated(
        last_group_key: Vec<Value>,
        initial_offset: u32,
    ) -> Self {
        Self {
            last_group_key: Some(last_group_key),
            initial_offset,
        }
    }

    #[must_use]
    pub(in crate::db) fn last_group_key(&self) -> Option<&[Value]> {
        self.last_group_key.as_deref()
    }

    #[must_use]
    pub(in crate::db) const fn initial_offset(&self) -> u32 {
        self.initial_offset
    }

    #[must_use]
    pub(in crate::db) const fn is_empty(&self) -> bool {
        self.last_group_key.is_none() && self.initial_offset == 0
    }
}
