use crate::db::{
    executor::ScalarTerminalBoundaryRequest,
    query::{
        builder::aggregate::AggregateExplain,
        plan::{AggregateKind, FieldSlot},
    },
};

///
/// ScalarTerminalRequest
///
/// Stable scalar terminal executor request projection derived once at the
/// fluent aggregate entrypoint boundary.
/// This keeps id/extrema execution-side request choice aligned with the same
/// strategy metadata that explain projects on demand.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ScalarTerminalRequest {
    IdTerminal {
        kind: AggregateKind,
    },
    IdBySlot {
        kind: AggregateKind,
        target_field: FieldSlot,
    },
}

///
/// ScalarTerminalStrategy
///
/// ScalarTerminalStrategy is the fluent scalar id/extrema behavior source.
/// It resolves terminal request shape once so the id/extrema family does not
/// rebuild those decisions through parallel branch trees.
/// Explain-visible aggregate shape is projected from that same strategy
/// metadata only when explain needs it.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ScalarTerminalStrategy {
    request: ScalarTerminalRequest,
}

impl ScalarTerminalStrategy {
    /// Prepare one fluent id-returning scalar terminal without a field target.
    #[must_use]
    pub(crate) const fn id_terminal(kind: AggregateKind) -> Self {
        Self {
            request: ScalarTerminalRequest::IdTerminal { kind },
        }
    }

    /// Prepare one fluent field-targeted extrema terminal with a resolved
    /// planner slot.
    #[must_use]
    pub(crate) const fn id_by_slot(kind: AggregateKind, target_field: FieldSlot) -> Self {
        Self {
            request: ScalarTerminalRequest::IdBySlot { kind, target_field },
        }
    }

    /// Move the executor scalar terminal request out of this strategy.
    #[must_use]
    pub(in crate::db) fn into_executor_request(self) -> ScalarTerminalBoundaryRequest {
        match self.request {
            ScalarTerminalRequest::IdTerminal { kind } => {
                ScalarTerminalBoundaryRequest::IdTerminal { kind }
            }
            ScalarTerminalRequest::IdBySlot { kind, target_field } => {
                ScalarTerminalBoundaryRequest::IdBySlot { kind, target_field }
            }
        }
    }
}

impl AggregateExplain for ScalarTerminalStrategy {
    fn explain_aggregate_kind(&self) -> Option<AggregateKind> {
        Some(match self.request {
            ScalarTerminalRequest::IdTerminal { kind }
            | ScalarTerminalRequest::IdBySlot { kind, .. } => kind,
        })
    }

    fn explain_projected_field(&self) -> Option<&str> {
        match &self.request {
            ScalarTerminalRequest::IdTerminal { .. } => None,
            ScalarTerminalRequest::IdBySlot { target_field, .. } => Some(target_field.field()),
        }
    }
}
