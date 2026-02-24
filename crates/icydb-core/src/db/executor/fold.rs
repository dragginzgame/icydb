use crate::{
    db::{
        Context,
        data::DataKey,
        executor::{LoadExecutor, OrderedKeyStream},
        query::{ReadConsistency, plan::Direction},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
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
    #[cfg_attr(not(test), allow(dead_code))]
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
/// AggregateFoldStateContainer
///
/// Internal fold-state boundary wrapper for aggregate streaming execution.
/// This keeps the fold engine ready for grouped multi-state expansion while
/// preserving single-state reducer behavior in the current release line.
///

pub(in crate::db::executor) struct AggregateFoldStateContainer<E: EntityKind> {
    reducer: AggregateReducerState<E>,
}

impl<E: EntityKind> AggregateFoldStateContainer<E> {
    /// Build one aggregate fold-state container for the selected terminal kind.
    #[must_use]
    pub(in crate::db::executor) const fn for_kind(kind: AggregateKind) -> Self {
        Self {
            reducer: AggregateReducerState::for_kind(kind),
        }
    }

    /// Apply one candidate data key update to the inner aggregate reducer.
    pub(in crate::db::executor) fn update_from_data_key(
        &mut self,
        kind: AggregateKind,
        direction: Direction,
        key: &DataKey,
    ) -> Result<FoldControl, InternalError> {
        self.reducer.update_from_data_key(kind, direction, key)
    }

    /// Convert the wrapped reducer state into one aggregate output payload.
    #[must_use]
    pub(in crate::db::executor) const fn into_output(self) -> AggregateOutput<E> {
        self.reducer.into_output()
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

// Map aggregate terminals onto the canonical fold mode contract.
const fn aggregate_expected_fold_mode(kind: AggregateKind) -> AggregateFoldMode {
    match kind {
        AggregateKind::Count => AggregateFoldMode::KeysOnly,
        AggregateKind::Exists
        | AggregateKind::Min
        | AggregateKind::Max
        | AggregateKind::First
        | AggregateKind::Last => AggregateFoldMode::ExistingRows,
    }
}

// Validate mode/terminal contract so executor callers cannot drift into
// kind-derived mode inference outside route planning.
fn ensure_aggregate_fold_mode_contract(
    kind: AggregateKind,
    mode: AggregateFoldMode,
) -> Result<(), InternalError> {
    if mode == aggregate_expected_fold_mode(kind) {
        return Ok(());
    }

    Err(InternalError::query_executor_invariant(
        "aggregate fold mode must match route fold-mode contract for aggregate terminal",
    ))
}

///
/// AggregateWindowState
///
/// Tracks effective offset/limit progression for aggregate terminals.
/// Windowing is applied after missing-row consistency handling so
/// aggregate cardinality matches normal load materialization semantics.
///

pub(in crate::db::executor) struct AggregateWindowState {
    offset_remaining: usize,
    limit_remaining: Option<usize>,
}

impl AggregateWindowState {
    pub(in crate::db::executor) fn from_plan(
        plan: &crate::db::query::plan::LogicalPlan<impl Copy>,
    ) -> Self {
        let offset = usize::try_from(plan.effective_page_offset(None)).unwrap_or(usize::MAX);
        let limit = plan
            .page
            .as_ref()
            .and_then(|page| page.limit)
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

        Self {
            offset_remaining: offset,
            limit_remaining: limit,
        }
    }

    pub(in crate::db::executor) const fn exhausted(&self) -> bool {
        matches!(self.limit_remaining, Some(0))
    }

    // Advance the window by one existing row and return whether the row
    // is part of the effective output window.
    pub(in crate::db::executor) const fn accept_existing_row(&mut self) -> bool {
        if self.offset_remaining > 0 {
            self.offset_remaining = self.offset_remaining.saturating_sub(1);
            return false;
        }

        if let Some(remaining) = self.limit_remaining.as_mut() {
            if *remaining == 0 {
                return false;
            }

            *remaining = remaining.saturating_sub(1);
        }

        true
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Single streaming fold entry for all aggregate terminals.
    // Key-only COUNT pushdown and row-aware terminals share this engine.
    pub(in crate::db::executor) fn fold_streaming_aggregate(
        ctx: &Context<'_, E>,
        plan: &crate::db::query::plan::LogicalPlan<E::Key>,
        consistency: ReadConsistency,
        direction: Direction,
        key_stream: &mut dyn OrderedKeyStream,
        kind: AggregateKind,
        mode: AggregateFoldMode,
    ) -> Result<(AggregateOutput<E>, usize), InternalError> {
        ensure_aggregate_fold_mode_contract(kind, mode)?;
        let window = AggregateWindowState::from_plan(plan);
        let (state_container, keys_scanned) = Self::fold_streaming(
            ctx,
            consistency,
            key_stream,
            window,
            mode,
            AggregateFoldStateContainer::for_kind(kind),
            |state_container, key| state_container.update_from_data_key(kind, direction, key),
        )?;

        Ok((state_container.into_output(), keys_scanned))
    }

    // Generic streaming fold loop used by all aggregate terminal reducers.
    // `mode` controls whether keys require row-existence validation.
    // Lifetime/retention contract:
    // - Fold state is scoped to one execution call and dropped at return.
    // - State updates consume only key metadata; no row references are retained.
    fn fold_streaming<S, F>(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        key_stream: &mut dyn OrderedKeyStream,
        window: AggregateWindowState,
        mode: AggregateFoldMode,
        mut state: S,
        mut apply: F,
    ) -> Result<(S, usize), InternalError>
    where
        F: FnMut(&mut S, &DataKey) -> Result<FoldControl, InternalError>,
    {
        let mut window = window;
        let mut keys_scanned = 0usize;

        while !window.exhausted() {
            let Some(key) = key_stream.next_key()? else {
                break;
            };

            keys_scanned = keys_scanned.saturating_add(1);
            if !Self::key_qualifies_for_fold(ctx, consistency, mode, &key)? {
                continue;
            }
            if !window.accept_existing_row() {
                continue;
            }
            if matches!(apply(&mut state, &key)?, FoldControl::Break) {
                break;
            }
        }

        Ok((state, keys_scanned))
    }

    // Determine whether a key is eligible for aggregate folding in the selected mode.
    // Key-only mode is used by COUNT pushdown and intentionally skips row reads.
    pub(in crate::db::executor) fn key_qualifies_for_fold(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        mode: AggregateFoldMode,
        key: &DataKey,
    ) -> Result<bool, InternalError> {
        match mode {
            AggregateFoldMode::KeysOnly => Ok(true),
            AggregateFoldMode::ExistingRows => Self::row_exists_for_key(ctx, consistency, key),
        }
    }

    // Keep read-consistency behavior aligned with row materialization paths.
    fn row_exists_for_key(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        key: &DataKey,
    ) -> Result<bool, InternalError> {
        match consistency {
            ReadConsistency::Strict => {
                let _ = ctx.read_strict(key)?;

                Ok(true)
            }
            ReadConsistency::MissingOk => match ctx.read(key) {
                Ok(_) => Ok(true),
                Err(err) if err.is_not_found() => Ok(false),
                Err(err) => Err(err),
            },
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        AggregateFoldMode, AggregateKind, AggregateSpec, AggregateSpecSupportError,
        aggregate_expected_fold_mode, ensure_aggregate_fold_mode_contract,
    };

    #[test]
    fn aggregate_fold_mode_contract_maps_count_to_keys_only() {
        assert_eq!(
            aggregate_expected_fold_mode(AggregateKind::Count),
            AggregateFoldMode::KeysOnly
        );
        assert!(
            ensure_aggregate_fold_mode_contract(AggregateKind::Count, AggregateFoldMode::KeysOnly)
                .is_ok()
        );
    }

    #[test]
    fn aggregate_fold_mode_contract_maps_non_count_to_existing_rows() {
        for kind in [
            AggregateKind::Exists,
            AggregateKind::Min,
            AggregateKind::Max,
            AggregateKind::First,
            AggregateKind::Last,
        ] {
            assert_eq!(
                aggregate_expected_fold_mode(kind),
                AggregateFoldMode::ExistingRows
            );
            assert!(
                ensure_aggregate_fold_mode_contract(kind, AggregateFoldMode::ExistingRows).is_ok()
            );
        }
    }

    #[test]
    fn aggregate_fold_mode_contract_rejects_count_existing_rows() {
        let result = ensure_aggregate_fold_mode_contract(
            AggregateKind::Count,
            AggregateFoldMode::ExistingRows,
        );

        assert!(result.is_err());
    }

    #[test]
    fn aggregate_fold_mode_contract_rejects_non_count_keys_only() {
        for kind in [
            AggregateKind::Exists,
            AggregateKind::Min,
            AggregateKind::Max,
            AggregateKind::First,
            AggregateKind::Last,
        ] {
            let result = ensure_aggregate_fold_mode_contract(kind, AggregateFoldMode::KeysOnly);

            assert!(result.is_err());
        }
    }

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
