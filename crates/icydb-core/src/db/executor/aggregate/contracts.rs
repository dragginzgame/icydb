use crate::{
    db::{data::DataKey, direction::Direction},
    error::InternalError,
    traits::EntityKind,
    types::Id,
};
use std::collections::BTreeSet;
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

    /// Return whether this terminal kind belongs to the extrema family.
    #[must_use]
    pub(in crate::db::executor) const fn is_extrema(self) -> bool {
        self.supports_field_targets()
    }

    /// Return whether this terminal kind supports first/last value projection.
    #[must_use]
    pub(in crate::db::executor) const fn supports_terminal_value_projection(self) -> bool {
        matches!(self, Self::First | Self::Last)
    }

    /// Return whether reducer updates for this kind require a decoded id payload.
    #[must_use]
    pub(in crate::db::executor) const fn requires_decoded_id(self) -> bool {
        !matches!(self, Self::Count | Self::Exists)
    }

    /// Return the canonical extrema traversal direction for this terminal kind.
    #[must_use]
    pub(in crate::db::executor) const fn extrema_direction(self) -> Option<Direction> {
        match self {
            Self::Min => Some(Direction::Asc),
            Self::Max => Some(Direction::Desc),
            Self::Count | Self::Exists | Self::First | Self::Last => None,
        }
    }

    /// Build the canonical empty-window aggregate output for this terminal kind.
    #[must_use]
    pub(in crate::db::executor) const fn zero_output<E: EntityKind>(self) -> AggregateOutput<E> {
        match self {
            Self::Count => AggregateOutput::Count(0),
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
            Self::Count | Self::Exists | Self::First | Self::Last => None,
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
/// GroupAggregateSpec
///
/// Canonical grouped aggregate contract for future GROUP BY execution.
/// Carries grouping keys plus one-or-more aggregate terminal specifications.
///

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db::executor) struct GroupAggregateSpec {
    group_keys: Vec<String>,
    aggregate_specs: Vec<AggregateSpec>,
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

///
/// GroupAggregateSpecSupportError
///
/// Canonical unsupported taxonomy for grouped aggregate contract validation.
/// Keeps GROUP BY contract shape failures explicit before execution is enabled.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db::executor) enum GroupAggregateSpecSupportError {
    #[error("group aggregate spec requires at least one aggregate terminal")]
    MissingAggregateSpecs,

    #[error("group aggregate spec has duplicate group key: {field}")]
    DuplicateGroupKey { field: String },

    #[error("group aggregate spec contains unsupported terminal at index={index}: {source}")]
    AggregateSpecUnsupported {
        index: usize,
        #[source]
        source: AggregateSpecSupportError,
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

#[cfg_attr(not(test), allow(dead_code))]
impl GroupAggregateSpec {
    /// Build one grouped aggregate contract from group-key + terminal specs.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        group_keys: Vec<String>,
        aggregate_specs: Vec<AggregateSpec>,
    ) -> Self {
        Self {
            group_keys,
            aggregate_specs,
        }
    }

    /// Build one global aggregate contract from a single terminal aggregate.
    #[must_use]
    pub(in crate::db::executor) fn for_global_terminal(spec: AggregateSpec) -> Self {
        Self {
            group_keys: Vec::new(),
            aggregate_specs: vec![spec],
        }
    }

    /// Borrow grouped key fields in declared order.
    #[must_use]
    pub(in crate::db::executor) const fn group_keys(&self) -> &[String] {
        self.group_keys.as_slice()
    }

    /// Borrow aggregate terminal specifications in declared projection order.
    #[must_use]
    pub(in crate::db::executor) const fn aggregate_specs(&self) -> &[AggregateSpec] {
        self.aggregate_specs.as_slice()
    }

    /// Return true when this contract models grouped aggregation.
    #[must_use]
    pub(in crate::db::executor) const fn is_grouped(&self) -> bool {
        !self.group_keys.is_empty()
    }

    /// Validate support boundaries for grouped aggregate contracts.
    pub(in crate::db::executor) fn ensure_supported_for_execution(
        &self,
    ) -> Result<(), GroupAggregateSpecSupportError> {
        if self.aggregate_specs.is_empty() {
            return Err(GroupAggregateSpecSupportError::MissingAggregateSpecs);
        }

        let mut seen_group_keys = BTreeSet::<&str>::new();
        for field in &self.group_keys {
            if !seen_group_keys.insert(field.as_str()) {
                return Err(GroupAggregateSpecSupportError::DuplicateGroupKey {
                    field: field.clone(),
                });
            }
        }

        for (index, spec) in self.aggregate_specs.iter().enumerate() {
            spec.ensure_supported_for_execution().map_err(|source| {
                GroupAggregateSpecSupportError::AggregateSpecUnsupported { index, source }
            })?;
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
        let id = if kind.requires_decoded_id() {
            Some(Id::from_key(key.try_key::<E>()?))
        } else {
            None
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
    use super::{
        AggregateKind, AggregateSpec, AggregateSpecSupportError, GroupAggregateSpec,
        GroupAggregateSpecSupportError,
    };

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

    #[test]
    fn group_aggregate_spec_support_accepts_group_keys_and_supported_specs() {
        let grouped = GroupAggregateSpec::new(
            vec!["tenant".to_string(), "region".to_string()],
            vec![
                AggregateSpec::for_terminal(AggregateKind::Count),
                AggregateSpec::for_target_field(AggregateKind::Max, "score"),
            ],
        );

        assert!(grouped.is_grouped());
        assert_eq!(
            grouped.group_keys(),
            &["tenant".to_string(), "region".to_string()]
        );
        assert_eq!(grouped.aggregate_specs().len(), 2);
        assert!(grouped.ensure_supported_for_execution().is_ok());
    }

    #[test]
    fn group_aggregate_spec_support_rejects_empty_terminal_list() {
        let grouped = GroupAggregateSpec::new(vec!["tenant".to_string()], Vec::new());
        let err = grouped
            .ensure_supported_for_execution()
            .expect_err("grouped aggregate contract must reject empty aggregate terminal list");

        assert_eq!(err, GroupAggregateSpecSupportError::MissingAggregateSpecs);
    }

    #[test]
    fn group_aggregate_spec_support_rejects_duplicate_group_key() {
        let grouped = GroupAggregateSpec::new(
            vec!["tenant".to_string(), "tenant".to_string()],
            vec![AggregateSpec::for_terminal(AggregateKind::Count)],
        );
        let err = grouped
            .ensure_supported_for_execution()
            .expect_err("grouped aggregate contract must reject duplicate group keys");

        assert_eq!(
            err,
            GroupAggregateSpecSupportError::DuplicateGroupKey {
                field: "tenant".to_string(),
            }
        );
    }

    #[test]
    fn group_aggregate_spec_support_rejects_unsupported_nested_terminal() {
        let grouped = GroupAggregateSpec::new(
            vec!["tenant".to_string()],
            vec![
                AggregateSpec::for_terminal(AggregateKind::Count),
                AggregateSpec::for_target_field(AggregateKind::Exists, "rank"),
            ],
        );
        let err = grouped
            .ensure_supported_for_execution()
            .expect_err("grouped aggregate contract must reject unsupported nested terminals");

        assert!(matches!(
            err,
            GroupAggregateSpecSupportError::AggregateSpecUnsupported {
                index: 1,
                source: AggregateSpecSupportError::FieldTargetRequiresExtrema { .. },
            }
        ));
    }

    #[test]
    fn group_aggregate_spec_support_accepts_global_terminal_constructor() {
        let grouped = GroupAggregateSpec::for_global_terminal(AggregateSpec::for_terminal(
            AggregateKind::Count,
        ));

        assert!(!grouped.is_grouped());
        assert!(grouped.group_keys().is_empty());
        assert_eq!(grouped.aggregate_specs().len(), 1);
        assert!(grouped.ensure_supported_for_execution().is_ok());
    }
}
