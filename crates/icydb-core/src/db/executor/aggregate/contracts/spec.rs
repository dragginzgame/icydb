//! Module: executor::aggregate::contracts::spec
//! Responsibility: aggregate spec/output contract types.
//! Does not own: grouped logical-spec validation or aggregate reducer state machines.
//! Boundary: declarative aggregate contracts consumed by state/grouped modules.

// -----------------------------------------------------------------------------
// Execution-Agnostic Guard
// -----------------------------------------------------------------------------
// This module must remain execution-agnostic.
// No imports from executor load/kernel/route are allowed.

use crate::{error::InternalError, value::StorageKey};

pub(in crate::db::executor) use crate::db::query::plan::AggregateKind;

///
/// ScalarAggregateOutput
///
/// Structural scalar terminal result shared by COUNT/EXISTS/id-style scalar
/// aggregate routing and fold execution.
/// Numeric scalar terminals execute through their dedicated decimal boundary
/// and do not reuse this terminal output taxonomy.
///

pub(in crate::db::executor) enum ScalarAggregateOutput {
    Count(u32),
    Exists(bool),
    Min(Option<StorageKey>),
    Max(Option<StorageKey>),
    First(Option<StorageKey>),
    Last(Option<StorageKey>),
}

///
/// ScalarTerminalKind
///
/// ScalarTerminalKind narrows scalar reducer execution to the terminal families
/// that the shared key-based scalar reducer can actually execute.
/// This prevents numeric SUM/AVG requests from re-entering the structural
/// scalar reducer path under zero-window or fast-path special cases.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum ScalarTerminalKind {
    Count,
    Exists,
    Min,
    Max,
    First,
    Last,
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

impl ScalarTerminalKind {
    // Build the canonical scalar terminal kind rejection for unsupported
    // numeric or otherwise non-terminal aggregate families.
    fn unsupported_aggregate_kind(kind: AggregateKind) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "scalar terminal reducer requires COUNT/EXISTS/MIN/MAX/FIRST/LAST aggregate kind, found {kind:?}",
        ))
    }

    /// Narrow one aggregate kind onto the supported scalar terminal reducer family.
    pub(in crate::db::executor) fn try_from_aggregate_kind(
        kind: AggregateKind,
    ) -> Result<Self, InternalError> {
        match kind {
            AggregateKind::Count => Ok(Self::Count),
            AggregateKind::Exists => Ok(Self::Exists),
            AggregateKind::Min => Ok(Self::Min),
            AggregateKind::Max => Ok(Self::Max),
            AggregateKind::First => Ok(Self::First),
            AggregateKind::Last => Ok(Self::Last),
            AggregateKind::Sum | AggregateKind::Avg => Err(Self::unsupported_aggregate_kind(kind)),
        }
    }

    /// Return the aggregate kind represented by this scalar terminal reducer family.
    #[must_use]
    pub(in crate::db::executor) const fn aggregate_kind(self) -> AggregateKind {
        match self {
            Self::Count => AggregateKind::Count,
            Self::Exists => AggregateKind::Exists,
            Self::Min => AggregateKind::Min,
            Self::Max => AggregateKind::Max,
            Self::First => AggregateKind::First,
            Self::Last => AggregateKind::Last,
        }
    }

    /// Build the canonical empty-window scalar terminal output for this reducer family.
    #[must_use]
    pub(in crate::db::executor) const fn zero_output(self) -> ScalarAggregateOutput {
        match self {
            Self::Count => ScalarAggregateOutput::Count(0),
            Self::Exists => ScalarAggregateOutput::Exists(false),
            Self::Min => ScalarAggregateOutput::Min(None),
            Self::Max => ScalarAggregateOutput::Max(None),
            Self::First => ScalarAggregateOutput::First(None),
            Self::Last => ScalarAggregateOutput::Last(None),
        }
    }
}

impl AggregateKind {
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
