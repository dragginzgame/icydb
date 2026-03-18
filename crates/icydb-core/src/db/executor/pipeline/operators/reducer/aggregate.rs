use crate::{
    db::{
        Context,
        data::DataKey,
        direction::Direction,
        executor::{
            ExecutionKernel, OrderedKeyStream,
            aggregate::{
                AggregateEngine, AggregateFoldMode, AggregateKind, AggregateOutput, FoldControl,
                execute_aggregate,
            },
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

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
        plan: &AccessPlannedQuery,
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

        // Build one scalar aggregate reducer engine and fold all eligible keys
        // through one adapter-owned ingest authority.
        let engine = AggregateEngine::new_scalar(kind, direction);
        let mut keys_scanned = 0usize;
        let mut ingest_all = |engine: &mut AggregateEngine<E>| -> Result<(), InternalError> {
            let mut on_key =
                |key: &DataKey| -> Result<FoldControl, InternalError> { engine.ingest(key) };
            keys_scanned = Self::run_aggregate_key_fold(ctx, plan, mode, key_stream, &mut on_key)?;

            Ok(())
        };
        let aggregate_output = execute_aggregate(engine, &mut ingest_all)?;

        Ok((aggregate_output, keys_scanned))
    }
}
