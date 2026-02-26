use crate::{
    db::{data::DataKey, direction::Direction},
    error::InternalError,
    traits::EntityKind,
    types::Id,
};
use thiserror::Error as ThisError;

///
/// AggregateKind
///
/// Internal aggregate terminal selector shared by aggregate routing and fold execution.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AggregateKind {
    Count,
    Exists,
    Min,
    Max,
    First,
    Last,
}

impl AggregateKind {
    /// Return whether this terminal kind supports explicit field targets.
    #[must_use]
    pub(in crate::db::executor) const fn supports_field_targets(self) -> bool {
        matches!(self, Self::Min | Self::Max)
    }
}

///
/// AggregateSpec
///
/// Canonical aggregate execution specification used by route/fold boundaries.
/// `target_field` is reserved for field-scoped aggregates (`min(field)` / `max(field)`).
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct AggregateSpec {
    kind: AggregateKind,
    target_field: Option<String>,
}

///
/// AggregateSpecSupportError
///
/// Canonical unsupported taxonomy for aggregate spec shape validation.
/// Keeps field-target capability errors explicit before runtime execution.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(in crate::db::executor) enum AggregateSpecSupportError {
    #[error(
        "field-target aggregates are only supported for min/max terminals: {kind:?}({target_field})"
    )]
    FieldTargetRequiresExtrema {
        kind: AggregateKind,
        target_field: String,
    },
}

impl AggregateSpec {
    /// Build a terminal aggregate spec with no explicit field target.
    #[must_use]
    pub(in crate::db::executor) const fn for_terminal(kind: AggregateKind) -> Self {
        Self {
            kind,
            target_field: None,
        }
    }

    /// Build a field-targeted aggregate spec for future field aggregates.
    #[must_use]
    pub(in crate::db::executor) fn for_target_field(
        kind: AggregateKind,
        target_field: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            target_field: Some(target_field.into()),
        }
    }

    /// Return the aggregate terminal kind.
    #[must_use]
    pub(in crate::db::executor) const fn kind(&self) -> AggregateKind {
        self.kind
    }

    /// Return the optional aggregate field target.
    #[must_use]
    pub(in crate::db::executor) fn target_field(&self) -> Option<&str> {
        self.target_field.as_deref()
    }

    /// Validate support boundaries for this aggregate spec in the current release line.
    pub(in crate::db::executor) fn ensure_supported_for_execution(
        &self,
    ) -> Result<(), AggregateSpecSupportError> {
        let Some(target_field) = self.target_field() else {
            return Ok(());
        };
        if !self.kind.supports_field_targets() {
            return Err(AggregateSpecSupportError::FieldTargetRequiresExtrema {
                kind: self.kind,
                target_field: target_field.to_string(),
            });
        }
        Ok(())
    }
}

///
/// AggregateOutput
///
/// Internal aggregate terminal result container shared by aggregate routing and fold execution.
///

pub(in crate::db::executor) enum AggregateOutput<E: EntityKind> {
    Count(u32),
    Exists(bool),
    Min(Option<Id<E>>),
    Max(Option<Id<E>>),
    First(Option<Id<E>>),
    Last(Option<Id<E>>),
}

///
/// FoldControl
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum FoldControl {
    Continue,
    Break,
}

///
/// AggregateReducerState
///
/// Shared aggregate terminal reducer state used by streaming and fast-path
/// aggregate execution so terminal update semantics stay centralized.
///

pub(in crate::db::executor) enum AggregateReducerState<E: EntityKind> {
    Count(u32),
    Exists(bool),
    Min(Option<Id<E>>),
    Max(Option<Id<E>>),
    First(Option<Id<E>>),
    Last(Option<Id<E>>),
}

impl<E: EntityKind> AggregateReducerState<E> {
    /// Build the initial reducer state for one aggregate terminal.
    #[must_use]
    pub(in crate::db::executor) const fn for_kind(kind: AggregateKind) -> Self {
        match kind {
            AggregateKind::Count => Self::Count(0),
            AggregateKind::Exists => Self::Exists(false),
            AggregateKind::Min => Self::Min(None),
            AggregateKind::Max => Self::Max(None),
            AggregateKind::First => Self::First(None),
            AggregateKind::Last => Self::Last(None),
        }
    }

    /// Apply one candidate data key to the reducer and return fold control.
    pub(in crate::db::executor) fn update_from_data_key(
        &mut self,
        kind: AggregateKind,
        direction: Direction,
        key: &DataKey,
    ) -> Result<FoldControl, InternalError> {
        let id = match kind {
            AggregateKind::Count | AggregateKind::Exists => None,
            AggregateKind::Min
            | AggregateKind::Max
            | AggregateKind::First
            | AggregateKind::Last => Some(Id::from_key(key.try_key::<E>()?)),
        };

        self.update_with_optional_id(kind, direction, id)
    }

    /// Apply one reducer update using an optional decoded id payload.
    pub(in crate::db::executor) fn update_with_optional_id(
        &mut self,
        kind: AggregateKind,
        direction: Direction,
        id: Option<Id<E>>,
    ) -> Result<FoldControl, InternalError> {
        match (kind, self) {
            (AggregateKind::Count, Self::Count(count)) => {
                *count = count.saturating_add(1);
                Ok(FoldControl::Continue)
            }
            (AggregateKind::Exists, Self::Exists(exists)) => {
                *exists = true;
                Ok(FoldControl::Break)
            }
            (AggregateKind::Min, Self::Min(min_id)) => {
                let Some(id) = id else {
                    return Err(InternalError::query_executor_invariant(
                        "aggregate reducer MIN update requires decoded id",
                    ));
                };
                *min_id = Some(id);
                if direction == Direction::Asc {
                    return Ok(FoldControl::Break);
                }

                Ok(FoldControl::Continue)
            }
            (AggregateKind::Max, Self::Max(max_id)) => {
                let Some(id) = id else {
                    return Err(InternalError::query_executor_invariant(
                        "aggregate reducer MAX update requires decoded id",
                    ));
                };
                *max_id = Some(id);
                if direction == Direction::Desc {
                    return Ok(FoldControl::Break);
                }

                Ok(FoldControl::Continue)
            }
            (AggregateKind::First, Self::First(first_id)) => {
                let Some(id) = id else {
                    return Err(InternalError::query_executor_invariant(
                        "aggregate reducer FIRST update requires decoded id",
                    ));
                };
                *first_id = Some(id);
                Ok(FoldControl::Break)
            }
            (AggregateKind::Last, Self::Last(last_id)) => {
                let Some(id) = id else {
                    return Err(InternalError::query_executor_invariant(
                        "aggregate reducer LAST update requires decoded id",
                    ));
                };
                *last_id = Some(id);
                Ok(FoldControl::Continue)
            }
            _ => Err(InternalError::query_executor_invariant(
                "aggregate reducer state/kind mismatch",
            )),
        }
    }

    /// Convert reducer state into the aggregate terminal output payload.
    #[must_use]
    pub(in crate::db::executor) const fn into_output(self) -> AggregateOutput<E> {
        match self {
            Self::Count(value) => AggregateOutput::Count(value),
            Self::Exists(value) => AggregateOutput::Exists(value),
            Self::Min(value) => AggregateOutput::Min(value),
            Self::Max(value) => AggregateOutput::Max(value),
            Self::First(value) => AggregateOutput::First(value),
            Self::Last(value) => AggregateOutput::Last(value),
        }
    }
}

///
/// AggregateFoldMode
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AggregateFoldMode {
    ExistingRows,
    KeysOnly,
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{AggregateKind, AggregateSpec, AggregateSpecSupportError};

    #[test]
    fn aggregate_spec_support_accepts_terminal_specs_without_field_targets() {
        let spec = AggregateSpec::for_terminal(AggregateKind::Count);

        assert!(spec.ensure_supported_for_execution().is_ok());
    }

    #[test]
    fn aggregate_spec_support_rejects_field_target_non_extrema() {
        let spec = AggregateSpec::for_target_field(AggregateKind::Count, "rank");
        let err = spec
            .ensure_supported_for_execution()
            .expect_err("field-target COUNT should be rejected by support taxonomy");

        assert!(matches!(
            err,
            AggregateSpecSupportError::FieldTargetRequiresExtrema { .. }
        ));
    }

    #[test]
    fn aggregate_spec_support_accepts_field_target_extrema() {
        let spec = AggregateSpec::for_target_field(AggregateKind::Min, "rank");
        assert!(spec.ensure_supported_for_execution().is_ok());
    }
}
