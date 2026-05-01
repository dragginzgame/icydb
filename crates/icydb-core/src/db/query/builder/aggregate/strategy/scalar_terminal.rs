use crate::db::{
    executor::ScalarTerminalBoundaryRequest,
    query::{
        builder::aggregate::AggregateExplain,
        plan::{AggregateKind, FieldSlot},
    },
};

///
/// MinIdTerminal
///
/// Concrete fluent `min()` id terminal descriptor.
/// The descriptor is zero-sized because primary-key MIN has one fixed executor
/// request and no field target.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct MinIdTerminal;

impl MinIdTerminal {
    /// Prepare one fluent `min()` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new() -> Self {
        Self
    }

    /// Move the executor scalar terminal request out of this descriptor.
    #[must_use]
    pub(in crate::db) const fn into_executor_request(self) -> ScalarTerminalBoundaryRequest {
        let _ = self;

        ScalarTerminalBoundaryRequest::IdTerminal {
            kind: AggregateKind::Min,
        }
    }
}

impl AggregateExplain for MinIdTerminal {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        Some(AggregateKind::Min)
    }
}

///
/// MaxIdTerminal
///
/// Concrete fluent `max()` id terminal descriptor.
/// The descriptor is zero-sized because primary-key MAX has one fixed executor
/// request and no field target.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct MaxIdTerminal;

impl MaxIdTerminal {
    /// Prepare one fluent `max()` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new() -> Self {
        Self
    }

    /// Move the executor scalar terminal request out of this descriptor.
    #[must_use]
    pub(in crate::db) const fn into_executor_request(self) -> ScalarTerminalBoundaryRequest {
        let _ = self;

        ScalarTerminalBoundaryRequest::IdTerminal {
            kind: AggregateKind::Max,
        }
    }
}

impl AggregateExplain for MaxIdTerminal {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        Some(AggregateKind::Max)
    }
}

///
/// MinIdBySlotTerminal
///
/// Concrete fluent `min_by(field)` id terminal descriptor.
/// The descriptor owns the already-resolved planner slot so execution and
/// explain share one field-target decision without a runtime mode enum.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct MinIdBySlotTerminal {
    target_field: FieldSlot,
}

impl MinIdBySlotTerminal {
    /// Prepare one fluent `min_by(field)` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new(target_field: FieldSlot) -> Self {
        Self { target_field }
    }

    /// Move the executor scalar terminal request out of this descriptor.
    #[must_use]
    pub(in crate::db) fn into_executor_request(self) -> ScalarTerminalBoundaryRequest {
        ScalarTerminalBoundaryRequest::IdBySlot {
            kind: AggregateKind::Min,
            target_field: self.target_field,
        }
    }
}

impl AggregateExplain for MinIdBySlotTerminal {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        Some(AggregateKind::Min)
    }

    fn explain_projected_field(&self) -> Option<&str> {
        Some(self.target_field.field())
    }
}

///
/// MaxIdBySlotTerminal
///
/// Concrete fluent `max_by(field)` id terminal descriptor.
/// The descriptor owns the already-resolved planner slot so execution and
/// explain share one field-target decision without a runtime mode enum.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct MaxIdBySlotTerminal {
    target_field: FieldSlot,
}

impl MaxIdBySlotTerminal {
    /// Prepare one fluent `max_by(field)` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new(target_field: FieldSlot) -> Self {
        Self { target_field }
    }

    /// Move the executor scalar terminal request out of this descriptor.
    #[must_use]
    pub(in crate::db) fn into_executor_request(self) -> ScalarTerminalBoundaryRequest {
        ScalarTerminalBoundaryRequest::IdBySlot {
            kind: AggregateKind::Max,
            target_field: self.target_field,
        }
    }
}

impl AggregateExplain for MaxIdBySlotTerminal {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        Some(AggregateKind::Max)
    }

    fn explain_projected_field(&self) -> Option<&str> {
        Some(self.target_field.field())
    }
}
