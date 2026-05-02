use crate::db::query::{
    builder::aggregate::{
        ProjectionExplain, ProjectionExplainDescriptor, ScalarProjectionBoundaryRequest,
    },
    plan::{AggregateKind, FieldSlot},
};

///
/// ValuesBySlotTerminal
///
/// Concrete fluent `values_by(field)` projection terminal descriptor.
/// The descriptor owns the resolved field slot and maps to one projection
/// executor request plus one stable explain descriptor.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ValuesBySlotTerminal {
    target_field: FieldSlot,
}

impl ValuesBySlotTerminal {
    /// Prepare one fluent `values_by(field)` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new(target_field: FieldSlot) -> Self {
        Self { target_field }
    }

    /// Move the executor projection request out of this descriptor.
    #[must_use]
    pub(in crate::db) fn into_executor_request(
        self,
    ) -> (FieldSlot, ScalarProjectionBoundaryRequest) {
        (self.target_field, ScalarProjectionBoundaryRequest::Values)
    }

    /// Return the stable projection explain descriptor for this descriptor.
    #[must_use]
    pub(in crate::db::query::builder::aggregate) fn explain_descriptor(
        &self,
    ) -> ProjectionExplainDescriptor<'_> {
        ProjectionExplainDescriptor {
            terminal: "values_by",
            field: self.target_field.field(),
            output: "values",
        }
    }
}

impl ProjectionExplain for ValuesBySlotTerminal {
    fn explain_projection_descriptor(&self) -> ProjectionExplainDescriptor<'_> {
        self.explain_descriptor()
    }
}

///
/// DistinctValuesBySlotTerminal
///
/// Concrete fluent `distinct_values_by(field)` projection terminal descriptor.
/// DISTINCT is fixed by the descriptor type instead of carried as a runtime
/// projection mode.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct DistinctValuesBySlotTerminal {
    target_field: FieldSlot,
}

impl DistinctValuesBySlotTerminal {
    /// Prepare one fluent `distinct_values_by(field)` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new(target_field: FieldSlot) -> Self {
        Self { target_field }
    }

    /// Move the executor projection request out of this descriptor.
    #[must_use]
    pub(in crate::db) fn into_executor_request(
        self,
    ) -> (FieldSlot, ScalarProjectionBoundaryRequest) {
        (
            self.target_field,
            ScalarProjectionBoundaryRequest::DistinctValues,
        )
    }

    /// Return the stable projection explain descriptor for this descriptor.
    #[must_use]
    pub(in crate::db::query::builder::aggregate) fn explain_descriptor(
        &self,
    ) -> ProjectionExplainDescriptor<'_> {
        ProjectionExplainDescriptor {
            terminal: "distinct_values_by",
            field: self.target_field.field(),
            output: "values",
        }
    }
}

impl ProjectionExplain for DistinctValuesBySlotTerminal {
    fn explain_projection_descriptor(&self) -> ProjectionExplainDescriptor<'_> {
        self.explain_descriptor()
    }
}

///
/// CountDistinctBySlotTerminal
///
/// Concrete fluent `count_distinct_by(field)` projection terminal descriptor.
/// The descriptor fixes both DISTINCT collection and count output at the type
/// level.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CountDistinctBySlotTerminal {
    target_field: FieldSlot,
}

impl CountDistinctBySlotTerminal {
    /// Prepare one fluent `count_distinct_by(field)` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new(target_field: FieldSlot) -> Self {
        Self { target_field }
    }

    /// Move the executor projection request out of this descriptor.
    #[must_use]
    pub(in crate::db) fn into_executor_request(
        self,
    ) -> (FieldSlot, ScalarProjectionBoundaryRequest) {
        (
            self.target_field,
            ScalarProjectionBoundaryRequest::CountDistinct,
        )
    }

    /// Return the stable projection explain descriptor for this descriptor.
    #[must_use]
    pub(in crate::db::query::builder::aggregate) fn explain_descriptor(
        &self,
    ) -> ProjectionExplainDescriptor<'_> {
        ProjectionExplainDescriptor {
            terminal: "count_distinct_by",
            field: self.target_field.field(),
            output: "count",
        }
    }
}

impl ProjectionExplain for CountDistinctBySlotTerminal {
    fn explain_projection_descriptor(&self) -> ProjectionExplainDescriptor<'_> {
        self.explain_descriptor()
    }
}

///
/// ValuesBySlotWithIdsTerminal
///
/// Concrete fluent `values_by_with_ids(field)` projection terminal descriptor.
/// The descriptor fixes id/value pair output without a transport output enum.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ValuesBySlotWithIdsTerminal {
    target_field: FieldSlot,
}

impl ValuesBySlotWithIdsTerminal {
    /// Prepare one fluent `values_by_with_ids(field)` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new(target_field: FieldSlot) -> Self {
        Self { target_field }
    }

    /// Move the executor projection request out of this descriptor.
    #[must_use]
    pub(in crate::db) fn into_executor_request(
        self,
    ) -> (FieldSlot, ScalarProjectionBoundaryRequest) {
        (
            self.target_field,
            ScalarProjectionBoundaryRequest::ValuesWithIds,
        )
    }

    /// Return the stable projection explain descriptor for this descriptor.
    #[must_use]
    pub(in crate::db::query::builder::aggregate) fn explain_descriptor(
        &self,
    ) -> ProjectionExplainDescriptor<'_> {
        ProjectionExplainDescriptor {
            terminal: "values_by_with_ids",
            field: self.target_field.field(),
            output: "values_with_ids",
        }
    }
}

impl ProjectionExplain for ValuesBySlotWithIdsTerminal {
    fn explain_projection_descriptor(&self) -> ProjectionExplainDescriptor<'_> {
        self.explain_descriptor()
    }
}

///
/// FirstValueBySlotTerminal
///
/// Concrete fluent `first_value_by(field)` projection terminal descriptor.
/// The descriptor fixes FIRST terminal-value semantics without storing a
/// terminal-kind mode field.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct FirstValueBySlotTerminal {
    target_field: FieldSlot,
}

impl FirstValueBySlotTerminal {
    /// Prepare one fluent `first_value_by(field)` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new(target_field: FieldSlot) -> Self {
        Self { target_field }
    }

    /// Move the executor projection request out of this descriptor.
    #[must_use]
    pub(in crate::db) fn into_executor_request(
        self,
    ) -> (FieldSlot, ScalarProjectionBoundaryRequest) {
        (
            self.target_field,
            ScalarProjectionBoundaryRequest::TerminalValue {
                terminal_kind: AggregateKind::First,
            },
        )
    }

    /// Return the stable projection explain descriptor for this descriptor.
    #[must_use]
    pub(in crate::db::query::builder::aggregate) fn explain_descriptor(
        &self,
    ) -> ProjectionExplainDescriptor<'_> {
        ProjectionExplainDescriptor {
            terminal: "first_value_by",
            field: self.target_field.field(),
            output: "terminal_value",
        }
    }
}

impl ProjectionExplain for FirstValueBySlotTerminal {
    fn explain_projection_descriptor(&self) -> ProjectionExplainDescriptor<'_> {
        self.explain_descriptor()
    }
}

///
/// LastValueBySlotTerminal
///
/// Concrete fluent `last_value_by(field)` projection terminal descriptor.
/// The descriptor fixes LAST terminal-value semantics without storing a
/// terminal-kind mode field.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct LastValueBySlotTerminal {
    target_field: FieldSlot,
}

impl LastValueBySlotTerminal {
    /// Prepare one fluent `last_value_by(field)` terminal descriptor.
    #[must_use]
    pub(in crate::db) const fn new(target_field: FieldSlot) -> Self {
        Self { target_field }
    }

    /// Move the executor projection request out of this descriptor.
    #[must_use]
    pub(in crate::db) fn into_executor_request(
        self,
    ) -> (FieldSlot, ScalarProjectionBoundaryRequest) {
        (
            self.target_field,
            ScalarProjectionBoundaryRequest::TerminalValue {
                terminal_kind: AggregateKind::Last,
            },
        )
    }

    /// Return the stable projection explain descriptor for this descriptor.
    #[must_use]
    pub(in crate::db::query::builder::aggregate) fn explain_descriptor(
        &self,
    ) -> ProjectionExplainDescriptor<'_> {
        ProjectionExplainDescriptor {
            terminal: "last_value_by",
            field: self.target_field.field(),
            output: "terminal_value",
        }
    }
}

impl ProjectionExplain for LastValueBySlotTerminal {
    fn explain_projection_descriptor(&self) -> ProjectionExplainDescriptor<'_> {
        self.explain_descriptor()
    }
}
