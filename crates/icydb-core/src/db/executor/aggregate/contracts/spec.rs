//! Module: executor::aggregate::contracts::spec
//! Responsibility: aggregate spec/output contract types.
//! Does not own: grouped logical-spec validation or aggregate reducer state machines.
//! Boundary: declarative aggregate contracts consumed by state/grouped modules.

// -----------------------------------------------------------------------------
// Execution-Agnostic Guard
// -----------------------------------------------------------------------------
// This module must remain execution-agnostic.
// No imports from executor load/kernel/route are allowed.

use crate::{error::InternalError, types::Decimal, value::StorageKey};

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

impl ScalarAggregateOutput {
    // Build the canonical aggregate output-kind mismatch on the owner type.
    fn output_kind_mismatch(mismatch_context: &'static str) -> InternalError {
        InternalError::query_executor_invariant(mismatch_context)
    }

    // Decode COUNT reducer output while preserving the caller's contract label.
    pub(in crate::db::executor) fn into_count(
        self,
        mismatch_context: &'static str,
    ) -> Result<u32, InternalError> {
        match self {
            Self::Count(value) => Ok(value),
            _ => Err(Self::output_kind_mismatch(mismatch_context)),
        }
    }

    // Decode EXISTS reducer output while preserving the caller's contract label.
    pub(in crate::db::executor) fn into_exists(
        self,
        mismatch_context: &'static str,
    ) -> Result<bool, InternalError> {
        match self {
            Self::Exists(value) => Ok(value),
            _ => Err(Self::output_kind_mismatch(mismatch_context)),
        }
    }

    // Decode one structural id-returning aggregate output for MIN/MAX/FIRST/LAST
    // terminals while keeping the aggregate-shape mismatch on the owner type.
    pub(in crate::db::executor) fn into_optional_id_terminal(
        self,
        kind: AggregateKind,
        mismatch_context: &'static str,
    ) -> Result<Option<StorageKey>, InternalError> {
        match (kind, self) {
            (AggregateKind::Min, Self::Min(value))
            | (AggregateKind::Max, Self::Max(value))
            | (AggregateKind::First, Self::First(value))
            | (AggregateKind::Last, Self::Last(value)) => Ok(value),
            _ => Err(Self::output_kind_mismatch(mismatch_context)),
        }
    }
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
