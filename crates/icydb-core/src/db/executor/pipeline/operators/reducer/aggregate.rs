use crate::{
    db::{
        Context,
        direction::Direction,
        executor::{
            ExecutionKernel, OrderedKeyStream,
            aggregate::{
                AggregateEngine, AggregateExecutionMode, AggregateExecutionSpec,
                AggregateFinalizeAdapter, AggregateFoldMode, AggregateIngestAdapter, AggregateKind,
                AggregateOutput, FoldControl, GroupError,
            },
            pipeline::operators::reducer::contracts::{
                KernelReducer, ReducerControl, StreamInputMode, StreamItem,
            },
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// AggregateStateReducer
///
/// AggregateStateReducer adapts the canonical aggregate state-machine boundary
/// to the kernel key-stream reducer contract.
/// All scalar aggregate terminals fold through this one reducer adapter.
///

struct AggregateStateReducer<E: EntityKind + EntityValue> {
    engine: AggregateEngine<E>,
    finalize_adapter: AggregateFinalizeAdapter<E>,
}

impl<E> AggregateStateReducer<E>
where
    E: EntityKind + EntityValue,
{
    // Build one reducer adapter for any scalar aggregate terminal.
    fn new(kind: AggregateKind, direction: Direction) -> Self {
        Self {
            engine: AggregateEngine::new_scalar(kind, direction),
            finalize_adapter: AggregateFinalizeAdapter::from_execution_mode(
                AggregateExecutionMode::Scalar,
            ),
        }
    }
}

impl<E> KernelReducer<E> for AggregateStateReducer<E>
where
    E: EntityKind + EntityValue,
{
    type Output = AggregateOutput<E>;
    const INPUT_MODE: StreamInputMode = StreamInputMode::KeyOnly;

    fn on_item(&mut self, item: StreamItem<'_, E>) -> Result<ReducerControl, InternalError> {
        match item {
            StreamItem::Key(key) => {
                let mut ingest_adapter = AggregateIngestAdapter::from_execution_spec(
                    &mut self.engine,
                    AggregateExecutionSpec::scalar(),
                )
                .map_err(GroupError::into_internal_error)?;
                let fold_control = ingest_adapter
                    .ingest(key, None)
                    .map_err(GroupError::into_internal_error)?;

                Ok(match fold_control {
                    FoldControl::Continue => ReducerControl::Continue,
                    FoldControl::Break => ReducerControl::StopEarly,
                })
            }
            StreamItem::Row(_row) => Err(crate::db::error::query_executor_invariant(
                "aggregate state reducer received row item for key-only input mode",
            )),
        }
    }

    fn finish(self) -> Result<Self::Output, InternalError> {
        self.finalize_adapter.finalize(self.engine)?.into_scalar()
    }
}

impl ExecutionKernel {
    // Validate aggregate kind/fold-mode compatibility against route contracts.
    const fn aggregate_fold_mode_matches_terminal(
        kind: AggregateKind,
        mode: AggregateFoldMode,
    ) -> bool {
        matches!(
            (kind, mode),
            (
                AggregateKind::Count,
                AggregateFoldMode::KeysOnly | AggregateFoldMode::ExistingRows
            ) | (
                AggregateKind::Sum
                    | AggregateKind::Exists
                    | AggregateKind::Min
                    | AggregateKind::Max
                    | AggregateKind::First
                    | AggregateKind::Last,
                AggregateFoldMode::ExistingRows
            )
        )
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
        if !Self::aggregate_fold_mode_matches_terminal(kind, mode) {
            return Err(crate::db::error::query_executor_invariant(
                "aggregate fold mode must match route fold-mode contract for aggregate terminal",
            ));
        }

        Self::run_key_stream_reducer(
            ctx,
            plan,
            mode,
            key_stream,
            AggregateStateReducer::<E>::new(kind, direction),
        )
    }
}
