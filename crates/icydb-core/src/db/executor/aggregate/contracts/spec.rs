//! Module: executor::aggregate::contracts::spec
//! Responsibility: aggregate spec/output contract types.
//! Does not own: grouped logical-spec validation or aggregate reducer state machines.
//! Boundary: declarative aggregate contracts consumed by state/grouped modules.

// -----------------------------------------------------------------------------
// Execution-Agnostic Guard
// -----------------------------------------------------------------------------
// This module must remain execution-agnostic.
// No imports from executor load/kernel/route are allowed.

use crate::{
    traits::EntityKind,
    types::{Decimal, Id},
};

pub(in crate::db::executor) use crate::db::query::plan::AggregateKind;

///
/// AggregateOutput
///
/// Internal aggregate terminal result container shared by aggregate routing and fold execution.
///

pub(in crate::db::executor) enum AggregateOutput<E: EntityKind> {
    Count(u32),
    Sum(Option<Decimal>),
    Exists(bool),
    Min(Option<Id<E>>),
    Max(Option<Id<E>>),
    First(Option<Id<E>>),
    Last(Option<Id<E>>),
}

impl AggregateKind {
    /// Build the canonical empty-window aggregate output for this terminal kind.
    #[must_use]
    pub(in crate::db::executor) const fn zero_output<E: EntityKind>(self) -> AggregateOutput<E> {
        match self {
            Self::Count => AggregateOutput::Count(0),
            Self::Sum | Self::Avg => AggregateOutput::Sum(None),
            Self::Exists => AggregateOutput::Exists(false),
            Self::Min => AggregateOutput::Min(None),
            Self::Max => AggregateOutput::Max(None),
            Self::First => AggregateOutput::First(None),
            Self::Last => AggregateOutput::Last(None),
        }
    }

    /// Build an extrema output payload when this kind is MIN or MAX.
    #[must_use]
    pub(in crate::db::executor) const fn extrema_output<E: EntityKind>(
        self,
        id: Option<Id<E>>,
    ) -> Option<AggregateOutput<E>> {
        match self {
            Self::Min => Some(AggregateOutput::Min(id)),
            Self::Max => Some(AggregateOutput::Max(id)),
            Self::Count | Self::Sum | Self::Avg | Self::Exists | Self::First | Self::Last => None,
        }
    }

    /// Return true when this kind/output pair is an unresolved extrema result.
    #[must_use]
    pub(in crate::db::executor) const fn is_unresolved_extrema_output<E: EntityKind>(
        self,
        output: &AggregateOutput<E>,
    ) -> bool {
        matches!(
            (self, output),
            (Self::Min, AggregateOutput::Min(None)) | (Self::Max, AggregateOutput::Max(None))
        )
    }
}
