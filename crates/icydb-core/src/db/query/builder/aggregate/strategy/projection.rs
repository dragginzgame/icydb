use crate::db::{
    executor::ScalarProjectionBoundaryRequest,
    query::{
        builder::aggregate::ProjectionExplainDescriptor,
        plan::{AggregateKind, FieldSlot},
    },
};

///
/// ProjectionRequest
///
/// Stable projection/distinct executor request projection derived once at the
/// fluent aggregate entrypoint boundary.
/// This keeps field-target projection terminal request shape aligned with the
/// strategy state that fluent execution consumes.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProjectionRequest {
    Values,
    DistinctValues,
    CountDistinct,
    ValuesWithIds,
    TerminalValue { terminal_kind: AggregateKind },
}

impl ProjectionRequest {
    // Return the scalar projection executor request represented by this
    // projection request.
    const fn boundary_request(self) -> ScalarProjectionBoundaryRequest {
        match self {
            Self::Values => ScalarProjectionBoundaryRequest::Values,
            Self::DistinctValues => ScalarProjectionBoundaryRequest::DistinctValues,
            Self::CountDistinct => ScalarProjectionBoundaryRequest::CountDistinct,
            Self::ValuesWithIds => ScalarProjectionBoundaryRequest::ValuesWithIds,
            Self::TerminalValue { terminal_kind } => {
                ScalarProjectionBoundaryRequest::TerminalValue { terminal_kind }
            }
        }
    }

    // Return the explain-visible terminal label for this projection request.
    fn terminal_label(self) -> &'static str {
        match self {
            Self::Values => "values_by",
            Self::DistinctValues => "distinct_values_by",
            Self::CountDistinct => "count_distinct_by",
            Self::ValuesWithIds => "values_by_with_ids",
            Self::TerminalValue {
                terminal_kind: AggregateKind::First,
            } => "first_value_by",
            Self::TerminalValue {
                terminal_kind: AggregateKind::Last,
            } => "last_value_by",
            Self::TerminalValue { .. } => {
                unreachable!("projection terminal value explain requires FIRST/LAST kind")
            }
        }
    }

    // Return the explain-visible output shape label for this projection
    // request.
    const fn output_label(self) -> &'static str {
        match self {
            Self::Values | Self::DistinctValues => "values",
            Self::CountDistinct => "count",
            Self::ValuesWithIds => "values_with_ids",
            Self::TerminalValue { .. } => "terminal_value",
        }
    }
}

///
/// ProjectionStrategy
///
/// ProjectionStrategy is the single fluent projection/distinct behavior
/// source.
/// It resolves target-slot ownership plus request shape once so
/// `values_by`/`distinct_values_by`/`count_distinct_by`/`values_by_with_ids`/
/// `first_value_by`/`last_value_by` do not rebuild those decisions inline.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProjectionStrategy {
    target_field: FieldSlot,
    request: ProjectionRequest,
}

impl ProjectionStrategy {
    /// Prepare one fluent `values_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn values_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            request: ProjectionRequest::Values,
        }
    }

    /// Prepare one fluent `distinct_values_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn distinct_values_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            request: ProjectionRequest::DistinctValues,
        }
    }

    /// Prepare one fluent `count_distinct_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn count_distinct_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            request: ProjectionRequest::CountDistinct,
        }
    }

    /// Prepare one fluent `values_by_with_ids(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn values_by_with_ids_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            request: ProjectionRequest::ValuesWithIds,
        }
    }

    /// Prepare one fluent `first_value_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn first_value_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            request: ProjectionRequest::TerminalValue {
                terminal_kind: AggregateKind::First,
            },
        }
    }

    /// Prepare one fluent `last_value_by(field)` terminal strategy.
    #[must_use]
    pub(crate) const fn last_value_by_slot(target_field: FieldSlot) -> Self {
        Self {
            target_field,
            request: ProjectionRequest::TerminalValue {
                terminal_kind: AggregateKind::Last,
            },
        }
    }

    /// Borrow the resolved planner target slot owned by this projection strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn target_field(&self) -> &FieldSlot {
        &self.target_field
    }

    /// Return the request projected by this projection strategy.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn request(&self) -> ProjectionRequest {
        self.request
    }

    /// Move the executor projection request and output shape out of this strategy.
    #[must_use]
    pub(in crate::db) fn into_executor_request(
        self,
    ) -> (
        FieldSlot,
        ScalarProjectionBoundaryRequest,
        ProjectionRequest,
    ) {
        (
            self.target_field,
            self.request.boundary_request(),
            self.request,
        )
    }

    /// Return the stable projection explain descriptor for this strategy.
    #[must_use]
    pub(crate) fn explain_descriptor(&self) -> ProjectionExplainDescriptor<'_> {
        ProjectionExplainDescriptor {
            terminal: self.request.terminal_label(),
            field: self.target_field.field(),
            output: self.request.output_label(),
        }
    }
}
