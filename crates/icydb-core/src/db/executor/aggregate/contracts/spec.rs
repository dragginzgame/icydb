//! Module: executor::aggregate::contracts::spec
//! Responsibility: aggregate spec/output contract types and grouped-spec validation.
//! Does not own: aggregate reducer state machines.
//! Boundary: declarative aggregate contracts consumed by state/grouped modules.

// -----------------------------------------------------------------------------
// Execution-Agnostic Guard
// -----------------------------------------------------------------------------
// This module must remain execution-agnostic.
// No imports from executor load/kernel/route are allowed.

use crate::{
    db::query::plan::{FieldSlot, GroupAggregateSpec as QueryGroupAggregateSpec},
    traits::EntityKind,
    types::Id,
};
use std::collections::BTreeSet;

use crate::db::executor::aggregate::contracts::error::{
    AggregateSpecSupportError, GroupAggregateSpecSupportError,
};

pub(in crate::db::executor) use crate::db::query::plan::AggregateKind;

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

impl AggregateKind {
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

/// Validate support boundaries for grouped aggregate contracts using canonical
/// query-owned grouped specs.
pub(in crate::db::executor) fn ensure_grouped_spec_supported_for_execution(
    group_fields: &[FieldSlot],
    aggregate_specs: &[QueryGroupAggregateSpec],
) -> Result<(), GroupAggregateSpecSupportError> {
    if aggregate_specs.is_empty() {
        return Err(GroupAggregateSpecSupportError::MissingAggregateSpecs);
    }

    let mut seen_group_slots = BTreeSet::<usize>::new();
    for field in group_fields {
        if !seen_group_slots.insert(field.index()) {
            return Err(GroupAggregateSpecSupportError::DuplicateGroupKey {
                field: field.field().to_string(),
            });
        }
    }

    for (index, spec) in aggregate_specs.iter().enumerate() {
        ensure_query_group_aggregate_supported(spec).map_err(|source| {
            GroupAggregateSpecSupportError::AggregateSpecUnsupported { index, source }
        })?;
    }

    Ok(())
}

fn ensure_query_group_aggregate_supported(
    spec: &QueryGroupAggregateSpec,
) -> Result<(), AggregateSpecSupportError> {
    let Some(target_field) = spec.target_field() else {
        return Ok(());
    };

    if !spec.kind().supports_field_targets() {
        return Err(AggregateSpecSupportError::FieldTargetRequiresExtrema {
            kind: spec.kind(),
            target_field: target_field.to_string(),
        });
    }

    Ok(())
}
