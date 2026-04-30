use crate::db::{
    executor::ScalarNumericFieldBoundaryRequest,
    query::{
        builder::aggregate::AggregateExplain,
        plan::{AggregateKind, FieldSlot},
    },
};

///
/// SumBySlotTerminal
///
/// Concrete fluent `sum(field)` terminal descriptor.
/// The descriptor owns the resolved field slot and maps directly to the
/// matching scalar numeric executor boundary request.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SumBySlotTerminal {
    target_field: FieldSlot,
}

impl SumBySlotTerminal {
    /// Prepare one fluent `sum(field)` terminal descriptor.
    #[must_use]
    pub(crate) const fn new(target_field: FieldSlot) -> Self {
        Self { target_field }
    }

    /// Move the executor numeric-field request out of this descriptor.
    #[must_use]
    pub(in crate::db) fn into_executor_request(
        self,
    ) -> (FieldSlot, ScalarNumericFieldBoundaryRequest) {
        (self.target_field, ScalarNumericFieldBoundaryRequest::Sum)
    }
}

impl AggregateExplain for SumBySlotTerminal {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        Some(AggregateKind::Sum)
    }

    fn explain_projected_field(&self) -> Option<&str> {
        Some(self.target_field.field())
    }
}

///
/// SumDistinctBySlotTerminal
///
/// Concrete fluent `sum(distinct field)` terminal descriptor.
/// The descriptor keeps DISTINCT as part of the chosen execution path instead
/// of carrying a runtime request enum through the fluent terminal layer.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SumDistinctBySlotTerminal {
    target_field: FieldSlot,
}

impl SumDistinctBySlotTerminal {
    /// Prepare one fluent `sum(distinct field)` terminal descriptor.
    #[must_use]
    pub(crate) const fn new(target_field: FieldSlot) -> Self {
        Self { target_field }
    }

    /// Move the executor numeric-field request out of this descriptor.
    #[must_use]
    pub(in crate::db) fn into_executor_request(
        self,
    ) -> (FieldSlot, ScalarNumericFieldBoundaryRequest) {
        (
            self.target_field,
            ScalarNumericFieldBoundaryRequest::SumDistinct,
        )
    }
}

impl AggregateExplain for SumDistinctBySlotTerminal {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        Some(AggregateKind::Sum)
    }

    fn explain_projected_field(&self) -> Option<&str> {
        Some(self.target_field.field())
    }
}

///
/// AvgBySlotTerminal
///
/// Concrete fluent `avg(field)` terminal descriptor.
/// The descriptor owns the resolved field slot and has a single direct
/// executor request projection.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AvgBySlotTerminal {
    target_field: FieldSlot,
}

impl AvgBySlotTerminal {
    /// Prepare one fluent `avg(field)` terminal descriptor.
    #[must_use]
    pub(crate) const fn new(target_field: FieldSlot) -> Self {
        Self { target_field }
    }

    /// Move the executor numeric-field request out of this descriptor.
    #[must_use]
    pub(in crate::db) fn into_executor_request(
        self,
    ) -> (FieldSlot, ScalarNumericFieldBoundaryRequest) {
        (self.target_field, ScalarNumericFieldBoundaryRequest::Avg)
    }
}

impl AggregateExplain for AvgBySlotTerminal {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        Some(AggregateKind::Avg)
    }

    fn explain_projected_field(&self) -> Option<&str> {
        Some(self.target_field.field())
    }
}

///
/// AvgDistinctBySlotTerminal
///
/// Concrete fluent `avg(distinct field)` terminal descriptor.
/// The descriptor represents exactly one numeric execution path and keeps the
/// session adapter branch-free.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AvgDistinctBySlotTerminal {
    target_field: FieldSlot,
}

impl AvgDistinctBySlotTerminal {
    /// Prepare one fluent `avg(distinct field)` terminal descriptor.
    #[must_use]
    pub(crate) const fn new(target_field: FieldSlot) -> Self {
        Self { target_field }
    }

    /// Move the executor numeric-field request out of this descriptor.
    #[must_use]
    pub(in crate::db) fn into_executor_request(
        self,
    ) -> (FieldSlot, ScalarNumericFieldBoundaryRequest) {
        (
            self.target_field,
            ScalarNumericFieldBoundaryRequest::AvgDistinct,
        )
    }
}

impl AggregateExplain for AvgDistinctBySlotTerminal {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        Some(AggregateKind::Avg)
    }

    fn explain_projected_field(&self) -> Option<&str> {
        Some(self.target_field.field())
    }
}
