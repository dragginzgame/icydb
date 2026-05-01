use crate::db::{
    executor::ScalarTerminalBoundaryRequest,
    query::{
        builder::aggregate::AggregateExplain,
        plan::{AggregateKind, FieldSlot},
    },
};

#[cfg(test)]
use crate::db::query::builder::aggregate::AggregateExpr;

///
/// FirstIdTerminal
///
/// Concrete fluent `first()` id terminal descriptor.
/// The descriptor is zero-sized because response-order FIRST has one fixed
/// executor request and no field target.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct FirstIdTerminal;

impl FirstIdTerminal {
    /// Prepare one fluent `first()` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new() -> Self {
        Self
    }

    /// Build the explain-visible aggregate expression projected by this descriptor.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::query) fn explain_aggregate() -> AggregateExpr {
        AggregateExpr::terminal_for_kind(AggregateKind::First)
    }

    /// Move the executor scalar terminal request out of this descriptor.
    #[must_use]
    pub(in crate::db) const fn into_executor_request(self) -> ScalarTerminalBoundaryRequest {
        let _ = self;

        ScalarTerminalBoundaryRequest::IdTerminal {
            kind: AggregateKind::First,
        }
    }
}

impl AggregateExplain for FirstIdTerminal {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        Some(AggregateKind::First)
    }
}

///
/// LastIdTerminal
///
/// Concrete fluent `last()` id terminal descriptor.
/// The descriptor is zero-sized because response-order LAST has one fixed
/// executor request and no field target.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct LastIdTerminal;

impl LastIdTerminal {
    /// Prepare one fluent `last()` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new() -> Self {
        Self
    }

    /// Build the explain-visible aggregate expression projected by this descriptor.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::query) fn explain_aggregate() -> AggregateExpr {
        AggregateExpr::terminal_for_kind(AggregateKind::Last)
    }

    /// Move the executor scalar terminal request out of this descriptor.
    #[must_use]
    pub(in crate::db) const fn into_executor_request(self) -> ScalarTerminalBoundaryRequest {
        let _ = self;

        ScalarTerminalBoundaryRequest::IdTerminal {
            kind: AggregateKind::Last,
        }
    }
}

impl AggregateExplain for LastIdTerminal {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        Some(AggregateKind::Last)
    }
}

///
/// NthIdBySlotTerminal
///
/// Concrete fluent `nth_by(field, nth)` id terminal descriptor.
/// The descriptor owns the resolved field slot and ordinal so no later layer
/// needs to branch on order-sensitive terminal shape.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct NthIdBySlotTerminal {
    target_field: FieldSlot,
    nth: usize,
}

impl NthIdBySlotTerminal {
    /// Prepare one fluent `nth_by(field, nth)` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new(target_field: FieldSlot, nth: usize) -> Self {
        Self { target_field, nth }
    }

    /// Move the executor scalar terminal request out of this descriptor.
    #[must_use]
    pub(in crate::db) fn into_executor_request(self) -> ScalarTerminalBoundaryRequest {
        ScalarTerminalBoundaryRequest::NthBySlot {
            target_field: self.target_field,
            nth: self.nth,
        }
    }
}

impl AggregateExplain for NthIdBySlotTerminal {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        None
    }
}

///
/// MedianIdBySlotTerminal
///
/// Concrete fluent `median_by(field)` id terminal descriptor.
/// The descriptor owns the resolved field slot and maps to exactly one
/// field-order executor request.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct MedianIdBySlotTerminal {
    target_field: FieldSlot,
}

impl MedianIdBySlotTerminal {
    /// Prepare one fluent `median_by(field)` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new(target_field: FieldSlot) -> Self {
        Self { target_field }
    }

    /// Move the executor scalar terminal request out of this descriptor.
    #[must_use]
    pub(in crate::db) fn into_executor_request(self) -> ScalarTerminalBoundaryRequest {
        ScalarTerminalBoundaryRequest::MedianBySlot {
            target_field: self.target_field,
        }
    }
}

impl AggregateExplain for MedianIdBySlotTerminal {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        None
    }
}

///
/// MinMaxIdBySlotTerminal
///
/// Concrete fluent `min_max_by(field)` id-pair terminal descriptor.
/// The descriptor owns the resolved field slot and has a fixed pair-output
/// executor request.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct MinMaxIdBySlotTerminal {
    target_field: FieldSlot,
}

impl MinMaxIdBySlotTerminal {
    /// Prepare one fluent `min_max_by(field)` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new(target_field: FieldSlot) -> Self {
        Self { target_field }
    }

    /// Move the executor scalar terminal request out of this descriptor.
    #[must_use]
    pub(in crate::db) fn into_executor_request(self) -> ScalarTerminalBoundaryRequest {
        ScalarTerminalBoundaryRequest::MinMaxBySlot {
            target_field: self.target_field,
        }
    }
}

impl AggregateExplain for MinMaxIdBySlotTerminal {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        None
    }
}
