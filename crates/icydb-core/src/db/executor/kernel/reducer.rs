use crate::{
    db::{
        Context,
        contracts::ReadConsistency,
        cursor::CursorBoundary,
        data::DataKey,
        direction::Direction,
        executor::{
            ExecutionKernel, LoadExecutor, OrderedKeyStream,
            aggregate::{
                AggregateFoldMode, AggregateKind, AggregateOutput, AggregateReducerState,
                FoldControl,
            },
            load::CursorPage,
        },
        plan::AccessPlannedQuery,
        response::Response,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};

///
/// StreamInputMode
///
/// Declares what item shape one kernel reducer consumes from execution.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum StreamInputMode {
    KeyOnly,
    RowOnly,
}

///
/// StreamItem
///
/// Item payload delivered by the kernel reducer runner.
/// Items are borrowed from kernel-local staging for one `on_item` call.
/// Reducers must treat these references as ephemeral and must not retain them.
///

pub(in crate::db::executor) enum StreamItem<'a, E: EntityKind + EntityValue> {
    Key(&'a DataKey),
    Row(&'a E),
}

///
/// ReducerControl
///
/// Reducer step-control contract returned by one `on_item` call.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum ReducerControl {
    Continue,
    StopEarly,
}

///
/// KernelReducer
///
/// KernelReducer is the canonical reducer contract for kernel-owned runner
/// orchestration. Reducers must be deterministic and restart-safe, and must
/// not retain `StreamItem` references after `on_item` returns.
///

pub(in crate::db::executor) trait KernelReducer<E: EntityKind + EntityValue> {
    type Output;
    const INPUT_MODE: StreamInputMode;

    fn on_item(&mut self, item: StreamItem<'_, E>) -> Result<ReducerControl, InternalError>;
    fn finish(self) -> Result<Self::Output, InternalError>;
}

///
/// CountExistsReducer
///
/// Adapter reducer that reuses canonical `AggregateReducerState` transition
/// semantics for key-stream COUNT/EXISTS terminals under the kernel runner.
///

struct CountExistsReducer<E: EntityKind + EntityValue> {
    kind: AggregateKind,
    state: AggregateReducerState<E>,
}

impl<E> CountExistsReducer<E>
where
    E: EntityKind + EntityValue,
{
    // Build one reducer adapter for COUNT/EXISTS streaming terminals.
    fn new(kind: AggregateKind) -> Result<Self, InternalError> {
        if !matches!(kind, AggregateKind::Count | AggregateKind::Exists) {
            return Err(InternalError::query_executor_invariant(
                "count/exists reducer supports only COUNT and EXISTS terminals",
            ));
        }

        Ok(Self {
            kind,
            state: AggregateReducerState::for_kind(kind),
        })
    }
}

impl<E> KernelReducer<E> for CountExistsReducer<E>
where
    E: EntityKind + EntityValue,
{
    type Output = AggregateOutput<E>;
    const INPUT_MODE: StreamInputMode = StreamInputMode::KeyOnly;

    fn on_item(&mut self, item: StreamItem<'_, E>) -> Result<ReducerControl, InternalError> {
        match item {
            StreamItem::Key(key) => {
                let fold_control =
                    self.state
                        .update_from_data_key(self.kind, Direction::Asc, key)?;

                Ok(match fold_control {
                    FoldControl::Continue => ReducerControl::Continue,
                    FoldControl::Break => ReducerControl::StopEarly,
                })
            }
            StreamItem::Row(_row) => Err(InternalError::query_executor_invariant(
                "aggregate state reducer received row item for key-only input mode",
            )),
        }
    }

    fn finish(self) -> Result<Self::Output, InternalError> {
        Ok(self.state.into_output())
    }
}

///
/// ExtremumFoldReducer
///
/// Reducer adapter for scalar MIN/MAX terminals over the key stream.
/// Field-target extrema stay on dedicated kernel orchestration paths.
///

struct ExtremumFoldReducer<E: EntityKind + EntityValue> {
    kind: AggregateKind,
    direction: Direction,
    state: AggregateReducerState<E>,
}

impl<E> ExtremumFoldReducer<E>
where
    E: EntityKind + EntityValue,
{
    // Build one reducer adapter for scalar MIN/MAX streaming terminals.
    fn new(kind: AggregateKind, direction: Direction) -> Result<Self, InternalError> {
        if !matches!(kind, AggregateKind::Min | AggregateKind::Max) {
            return Err(InternalError::query_executor_invariant(
                "extremum reducer supports only MIN and MAX terminals",
            ));
        }

        Ok(Self {
            kind,
            direction,
            state: AggregateReducerState::for_kind(kind),
        })
    }
}

impl<E> KernelReducer<E> for ExtremumFoldReducer<E>
where
    E: EntityKind + EntityValue,
{
    type Output = AggregateOutput<E>;
    const INPUT_MODE: StreamInputMode = StreamInputMode::KeyOnly;

    fn on_item(&mut self, item: StreamItem<'_, E>) -> Result<ReducerControl, InternalError> {
        match item {
            StreamItem::Key(key) => {
                let fold_control =
                    self.state
                        .update_from_data_key(self.kind, self.direction, key)?;

                Ok(match fold_control {
                    FoldControl::Continue => ReducerControl::Continue,
                    FoldControl::Break => ReducerControl::StopEarly,
                })
            }
            StreamItem::Row(_row) => Err(InternalError::query_executor_invariant(
                "extremum reducer received row item for key-only input mode",
            )),
        }
    }

    fn finish(self) -> Result<Self::Output, InternalError> {
        Ok(self.state.into_output())
    }
}

///
/// FirstLastReducer
///
/// Reducer adapter for scalar FIRST/LAST terminals over the key stream.
/// This preserves canonical direction-aware first/last fold semantics.
///

struct FirstLastReducer<E: EntityKind + EntityValue> {
    kind: AggregateKind,
    direction: Direction,
    state: AggregateReducerState<E>,
}

impl<E> FirstLastReducer<E>
where
    E: EntityKind + EntityValue,
{
    // Build one reducer adapter for scalar FIRST/LAST streaming terminals.
    fn new(kind: AggregateKind, direction: Direction) -> Result<Self, InternalError> {
        if !matches!(kind, AggregateKind::First | AggregateKind::Last) {
            return Err(InternalError::query_executor_invariant(
                "first/last reducer supports only FIRST and LAST terminals",
            ));
        }

        Ok(Self {
            kind,
            direction,
            state: AggregateReducerState::for_kind(kind),
        })
    }
}

impl<E> KernelReducer<E> for FirstLastReducer<E>
where
    E: EntityKind + EntityValue,
{
    type Output = AggregateOutput<E>;
    const INPUT_MODE: StreamInputMode = StreamInputMode::KeyOnly;

    fn on_item(&mut self, item: StreamItem<'_, E>) -> Result<ReducerControl, InternalError> {
        match item {
            StreamItem::Key(key) => {
                let fold_control =
                    self.state
                        .update_from_data_key(self.kind, self.direction, key)?;

                Ok(match fold_control {
                    FoldControl::Continue => ReducerControl::Continue,
                    FoldControl::Break => ReducerControl::StopEarly,
                })
            }
            StreamItem::Row(_row) => Err(InternalError::query_executor_invariant(
                "first/last reducer received row item for key-only input mode",
            )),
        }
    }

    fn finish(self) -> Result<Self::Output, InternalError> {
        Ok(self.state.into_output())
    }
}

///
/// RowCollectorReducer
///
/// RowCollectorReducer accepts ephemeral row items and keeps canonical load
/// row-collection behavior in the kernel-owned runner boundary.
///

struct RowCollectorReducer;

impl<E> KernelReducer<E> for RowCollectorReducer
where
    E: EntityKind + EntityValue,
{
    type Output = ();
    const INPUT_MODE: StreamInputMode = StreamInputMode::RowOnly;

    fn on_item(&mut self, item: StreamItem<'_, E>) -> Result<ReducerControl, InternalError> {
        match item {
            StreamItem::Row(_row) => Ok(ReducerControl::Continue),
            StreamItem::Key(_key) => Err(InternalError::query_executor_invariant(
                "row collector reducer received key item for row-only input mode",
            )),
        }
    }

    fn finish(self) -> Result<Self::Output, InternalError> {
        Ok(())
    }
}

impl ExecutionKernel {
    // Determine whether one key is eligible for aggregate folding in the
    // selected mode. Key-only mode intentionally skips row reads.
    fn key_qualifies_for_fold<E>(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        mode: AggregateFoldMode,
        key: &DataKey,
    ) -> Result<bool, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match mode {
            AggregateFoldMode::KeysOnly => Ok(true),
            AggregateFoldMode::ExistingRows => Self::row_exists_for_key(ctx, consistency, key),
        }
    }

    // Keep read-consistency behavior aligned with materialized row reads.
    fn row_exists_for_key<E>(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        key: &DataKey,
    ) -> Result<bool, InternalError>
    where
        E: EntityKind + EntityValue,
    {
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

    // Run a key-stream reducer under canonical aggregate window and
    // read-consistency eligibility contracts.
    fn run_key_stream_reducer<E, R>(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        mode: AggregateFoldMode,
        key_stream: &mut dyn OrderedKeyStream,
        mut reducer: R,
    ) -> Result<(R::Output, usize), InternalError>
    where
        E: EntityKind + EntityValue,
        R: KernelReducer<E>,
    {
        if !matches!(R::INPUT_MODE, StreamInputMode::KeyOnly) {
            return Err(InternalError::query_executor_invariant(
                "key-stream reducer runner currently supports key-only reducers",
            ));
        }

        let mut window = Self::window_cursor_contract(plan, None);
        let mut keys_scanned = 0usize;

        while !window.exhausted() {
            let Some(key) = key_stream.next_key()? else {
                break;
            };
            keys_scanned = keys_scanned.saturating_add(1);

            if !Self::key_qualifies_for_fold(ctx, plan.consistency, mode, &key)? {
                continue;
            }
            if !window.accept_existing_row() {
                continue;
            }

            match reducer.on_item(StreamItem::Key(&key))? {
                ReducerControl::Continue => {}
                ReducerControl::StopEarly => break,
            }
        }

        Ok((reducer.finish()?, keys_scanned))
    }

    // Run one row-only reducer for load collection over the already decorated
    // key stream. Rows are fetched only for keys that survive upstream stream
    // decorators and are staged before ephemeral row-item delivery.
    #[expect(clippy::type_complexity)]
    fn run_row_stream_reducer<E, R>(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        key_stream: &mut dyn OrderedKeyStream,
        mut reducer: R,
    ) -> Result<(Vec<(Id<E>, E)>, usize), InternalError>
    where
        E: EntityKind + EntityValue,
        R: KernelReducer<E>,
    {
        if !matches!(R::INPUT_MODE, StreamInputMode::RowOnly) {
            return Err(InternalError::query_executor_invariant(
                "row-stream reducer runner requires row-only reducer input mode",
            ));
        }

        let mut rows: Vec<(Id<E>, E)> = Vec::new();
        let mut keys_scanned = 0usize;

        while let Some(data_key) = key_stream.next_key()? {
            let Some(entity) =
                LoadExecutor::<E>::read_entity_for_field_extrema(ctx, plan.consistency, &data_key)?
            else {
                continue;
            };
            keys_scanned = keys_scanned.saturating_add(1);
            rows.push((Id::from_key(data_key.try_key::<E>()?), entity));

            // Ephemeral staging contract: pass a borrow scoped to this call only.
            let Some((_, staged_entity)) = rows.last() else {
                return Err(InternalError::query_executor_invariant(
                    "row-stream reducer staging unexpectedly missing last row",
                ));
            };
            match reducer.on_item(StreamItem::Row(staged_entity))? {
                ReducerControl::Continue => {}
                ReducerControl::StopEarly => break,
            }
        }

        let _ = reducer.finish()?;

        Ok((rows, keys_scanned))
    }

    // Return whether load execution can safely use the row-collector short path
    // without changing cursor/pagination/filter semantics.
    fn load_row_collector_short_path_eligible<K>(
        plan: &AccessPlannedQuery<K>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> bool {
        plan.mode.is_load()
            && cursor_boundary.is_none()
            && plan.predicate.is_none()
            && plan.order.is_none()
            && plan.page.is_none()
    }

    // Attempt one row-collector load materialization short path.
    // This path is intentionally narrow (cursorless, unpaged, no post-access
    // phases) to preserve exact behavior while proving row-only reducer wiring.
    pub(in crate::db::executor) fn try_materialize_load_via_row_collector<E>(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        key_stream: &mut dyn OrderedKeyStream,
    ) -> Result<Option<(CursorPage<E>, usize, usize)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        if !Self::load_row_collector_short_path_eligible(plan, cursor_boundary) {
            return Ok(None);
        }

        let (rows, keys_scanned) =
            Self::run_row_stream_reducer(ctx, plan, key_stream, RowCollectorReducer)?;
        let page = CursorPage {
            items: Response(rows),
            next_cursor: None,
        };
        let post_access_rows = page.items.0.len();

        Ok(Some((page, keys_scanned, post_access_rows)))
    }

    // Kernel-owned reducer runner for scalar aggregate terminals over one
    // canonical key stream. Field-target reducers stay in dedicated paths.
    pub(in crate::db::executor) fn run_streaming_aggregate_reducer<E>(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        kind: AggregateKind,
        direction: Direction,
        mode: AggregateFoldMode,
        key_stream: &mut dyn OrderedKeyStream,
    ) -> Result<(AggregateOutput<E>, usize), InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match (kind, mode) {
            (AggregateKind::Count, AggregateFoldMode::KeysOnly) => Self::run_key_stream_reducer(
                ctx,
                plan,
                mode,
                key_stream,
                CountExistsReducer::<E>::new(kind)?,
            ),
            (AggregateKind::Exists, AggregateFoldMode::ExistingRows) => {
                Self::run_key_stream_reducer(
                    ctx,
                    plan,
                    mode,
                    key_stream,
                    CountExistsReducer::<E>::new(kind)?,
                )
            }
            (AggregateKind::Min | AggregateKind::Max, AggregateFoldMode::ExistingRows) => {
                Self::run_key_stream_reducer(
                    ctx,
                    plan,
                    mode,
                    key_stream,
                    ExtremumFoldReducer::<E>::new(kind, direction)?,
                )
            }
            (AggregateKind::First | AggregateKind::Last, AggregateFoldMode::ExistingRows) => {
                Self::run_key_stream_reducer(
                    ctx,
                    plan,
                    mode,
                    key_stream,
                    FirstLastReducer::<E>::new(kind, direction)?,
                )
            }
            (
                AggregateKind::Count
                | AggregateKind::Exists
                | AggregateKind::Min
                | AggregateKind::Max
                | AggregateKind::First
                | AggregateKind::Last,
                AggregateFoldMode::KeysOnly | AggregateFoldMode::ExistingRows,
            ) => Err(InternalError::query_executor_invariant(
                "aggregate fold mode must match route fold-mode contract for aggregate terminal",
            )),
        }
    }
}
