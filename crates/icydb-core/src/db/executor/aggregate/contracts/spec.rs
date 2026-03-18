//! Module: executor::aggregate::contracts::spec
//! Responsibility: aggregate spec/output contract types.
//! Does not own: grouped logical-spec validation or aggregate reducer state machines.
//! Boundary: declarative aggregate contracts consumed by state/grouped modules.

// -----------------------------------------------------------------------------
// Execution-Agnostic Guard
// -----------------------------------------------------------------------------
// This module must remain execution-agnostic.
// No imports from executor load/kernel/route are allowed.

use crate::{types::Decimal, value::StorageKey};

pub(in crate::db::executor) use crate::db::query::plan::AggregateKind;

///
/// ScalarAggregateOutput
///
/// Structural scalar aggregate terminal result shared by scalar aggregate
/// routing and fold execution.
///

pub(in crate::db::executor) enum ScalarAggregateOutput {
    Count(u32),
    /// Numeric SUM/AVG execution finalizes through dedicated numeric paths,
    /// but zero-window scalar aggregate contracts still use this shared payload.
    #[expect(
        dead_code,
        reason = "numeric zero-window aggregate contracts still share ScalarAggregateOutput"
    )]
    Sum(Option<Decimal>),
    Exists(bool),
    Min(Option<StorageKey>),
    Max(Option<StorageKey>),
    First(Option<StorageKey>),
    Last(Option<StorageKey>),
}

impl AggregateKind {
    /// Build the canonical empty-window scalar aggregate output for this terminal kind.
    #[must_use]
    pub(in crate::db::executor) const fn zero_output(self) -> ScalarAggregateOutput {
        match self {
            Self::Count => ScalarAggregateOutput::Count(0),
            Self::Sum | Self::Avg => ScalarAggregateOutput::Sum(None),
            Self::Exists => ScalarAggregateOutput::Exists(false),
            Self::Min => ScalarAggregateOutput::Min(None),
            Self::Max => ScalarAggregateOutput::Max(None),
            Self::First => ScalarAggregateOutput::First(None),
            Self::Last => ScalarAggregateOutput::Last(None),
        }
    }

    /// Build a structural extrema output payload when this kind is MIN or MAX.
    #[must_use]
    pub(in crate::db::executor) const fn extrema_output(
        self,
        key: Option<StorageKey>,
    ) -> Option<ScalarAggregateOutput> {
        match self {
            Self::Min => Some(ScalarAggregateOutput::Min(key)),
            Self::Max => Some(ScalarAggregateOutput::Max(key)),
            Self::Count | Self::Sum | Self::Avg | Self::Exists | Self::First | Self::Last => None,
        }
    }

    /// Return true when this kind/output pair is an unresolved structural extrema result.
    #[must_use]
    pub(in crate::db::executor) const fn is_unresolved_extrema_output(
        self,
        output: &ScalarAggregateOutput,
    ) -> bool {
        matches!(
            (self, output),
            (Self::Min, ScalarAggregateOutput::Min(None))
                | (Self::Max, ScalarAggregateOutput::Max(None))
        )
    }
}
