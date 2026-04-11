//! Module: db::executor::pipeline::operators::reducer::aggregate
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::operators::reducer::aggregate.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        direction::Direction,
        executor::{
            ExecutionKernel, OrderedKeyStream,
            aggregate::{
                AggregateFoldMode, AggregateKind, FoldControl, ScalarAggregateEngine,
                ScalarAggregateOutput, execute_scalar_aggregate,
            },
        },
        query::plan::AccessPlannedQuery,
        registry::StoreHandle,
    },
    error::InternalError,
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
    pub(in crate::db::executor) fn run_streaming_aggregate_reducer<S>(
        store: StoreHandle,
        plan: &AccessPlannedQuery,
        kind: AggregateKind,
        direction: Direction,
        mode: AggregateFoldMode,
        key_stream: &mut S,
    ) -> Result<(ScalarAggregateOutput, usize), InternalError>
    where
        S: OrderedKeyStream + ?Sized,
    {
        if !Self::aggregate_fold_mode_matches_terminal(kind, mode) {
            return Err(InternalError::aggregate_fold_mode_terminal_contract_required());
        }

        // Build one scalar aggregate reducer engine and fold all eligible keys
        // through one adapter-owned ingest authority.
        let engine = ScalarAggregateEngine::new_scalar(kind, direction);
        let mut keys_scanned = 0usize;
        let aggregate_output = execute_scalar_aggregate(engine, |engine| {
            keys_scanned = Self::run_aggregate_key_fold(store, plan, mode, key_stream, |key| {
                let fold_control: FoldControl = engine.ingest(key)?;

                Ok(fold_control)
            })?;

            Ok(())
        })?;

        Ok((aggregate_output, keys_scanned))
    }
}
