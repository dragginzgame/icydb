//! Module: db::executor::pipeline::operators::reducer::aggregate
//! Defines aggregate reducer operators used by grouped and scalar reduction
//! paths.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        direction::Direction,
        executor::{
            ExecutionKernel, OrderedKeyStream,
            aggregate::{
                AggregateFoldMode, FoldControl, ScalarAggregateEngine, ScalarAggregateOutput,
                ScalarTerminalKind, execute_scalar_aggregate,
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
        kind: ScalarTerminalKind,
        mode: AggregateFoldMode,
    ) -> bool {
        matches!(
            (kind, mode),
            (
                ScalarTerminalKind::Count,
                AggregateFoldMode::KeysOnly | AggregateFoldMode::ExistingRows
            ) | (
                ScalarTerminalKind::Exists
                    | ScalarTerminalKind::Min
                    | ScalarTerminalKind::Max
                    | ScalarTerminalKind::First
                    | ScalarTerminalKind::Last,
                AggregateFoldMode::ExistingRows
            )
        )
    }

    // Kernel-owned reducer runner for scalar aggregate terminals over one
    // canonical key stream. Field-target reducers stay in dedicated paths.
    pub(in crate::db::executor) fn run_streaming_aggregate_reducer<S>(
        store: StoreHandle,
        plan: &AccessPlannedQuery,
        kind: ScalarTerminalKind,
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
