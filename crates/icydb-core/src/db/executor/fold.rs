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

///
/// AggregateKind
///
/// Internal aggregate terminal selector shared by aggregate routing and fold execution.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum AggregateKind {
    Count,
    Exists,
    Min,
    Max,
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
/// AggregateFoldMode
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum AggregateFoldMode {
    ExistingRows,
    KeysOnly,
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
        let window = AggregateWindowState::from_plan(plan);

        match kind {
            AggregateKind::Count => {
                let (count, keys_scanned) = Self::fold_streaming(
                    ctx,
                    consistency,
                    key_stream,
                    window,
                    mode,
                    0u32,
                    |count, _key| {
                        *count = count.saturating_add(1);
                        Ok(FoldControl::Continue)
                    },
                )?;

                Ok((AggregateOutput::Count(count), keys_scanned))
            }
            AggregateKind::Exists => {
                let (exists, keys_scanned) = Self::fold_streaming(
                    ctx,
                    consistency,
                    key_stream,
                    window,
                    mode,
                    false,
                    |exists, _key| {
                        *exists = true;
                        Ok(FoldControl::Break)
                    },
                )?;

                Ok((AggregateOutput::Exists(exists), keys_scanned))
            }
            AggregateKind::Min => {
                let (min_id, keys_scanned) = Self::fold_streaming(
                    ctx,
                    consistency,
                    key_stream,
                    window,
                    mode,
                    None::<Id<E>>,
                    |min_id, key| {
                        *min_id = Some(Id::from_key(key.try_key::<E>()?));
                        if direction == Direction::Asc {
                            return Ok(FoldControl::Break);
                        }

                        Ok(FoldControl::Continue)
                    },
                )?;

                Ok((AggregateOutput::Min(min_id), keys_scanned))
            }
            AggregateKind::Max => {
                let (max_id, keys_scanned) = Self::fold_streaming(
                    ctx,
                    consistency,
                    key_stream,
                    window,
                    mode,
                    None::<Id<E>>,
                    |max_id, key| {
                        *max_id = Some(Id::from_key(key.try_key::<E>()?));
                        if direction == Direction::Desc {
                            return Ok(FoldControl::Break);
                        }

                        Ok(FoldControl::Continue)
                    },
                )?;

                Ok((AggregateOutput::Max(max_id), keys_scanned))
            }
        }
    }

    // Generic streaming fold loop used by all aggregate terminal reducers.
    // `mode` controls whether keys require row-existence validation.
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
