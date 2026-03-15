use crate::{
    db::{
        Context,
        cursor::{ContinuationRuntime, LoopAction},
        data::DataKey,
        executor::{
            ExecutionKernel, OrderedKeyStream,
            aggregate::AggregateFoldMode,
            pipeline::operators::reducer::contracts::{
                KernelReducer, ReducerControl, StreamInputMode, StreamItem,
            },
            traversal::row_read_consistency_for_plan,
        },
        predicate::MissingRowPolicy,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl ExecutionKernel {
    // Determine whether one key is eligible for aggregate folding in the
    // selected mode. Key-only mode intentionally skips row reads.
    fn key_qualifies_for_fold<E>(
        ctx: &Context<'_, E>,
        consistency: MissingRowPolicy,
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
        consistency: MissingRowPolicy,
        key: &DataKey,
    ) -> Result<bool, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match consistency {
            MissingRowPolicy::Error => {
                let _ = ctx.read_strict(key)?;

                Ok(true)
            }
            MissingRowPolicy::Ignore => match ctx.read(key) {
                Ok(_) => Ok(true),
                Err(err) if err.is_not_found() => Ok(false),
                Err(err) => Err(err),
            },
        }
    }

    // Run a key-stream reducer under canonical aggregate window and
    // read-consistency eligibility contracts.
    pub(in crate::db::executor::pipeline::operators::reducer) fn run_key_stream_reducer<E, R>(
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
        // Phase 1: enforce reducer input-mode contract and initialize window counters.
        if !matches!(R::INPUT_MODE, StreamInputMode::KeyOnly) {
            return Err(crate::db::error::query_executor_invariant(
                "key-stream reducer runner currently supports key-only reducers",
            ));
        }

        let mut continuation =
            ContinuationRuntime::from_window(Self::window_cursor_contract(plan, None));
        let mut keys_scanned = 0usize;
        let consistency = row_read_consistency_for_plan(plan);

        // Phase 2: scan keys, apply fold eligibility/window gates, and feed reducer.
        loop {
            match continuation.pre_fetch() {
                LoopAction::Skip => continue,
                LoopAction::Emit => {}
                LoopAction::Stop => break,
            }

            let Some(key) = key_stream.next_key()? else {
                break;
            };
            keys_scanned = keys_scanned.saturating_add(1);

            if !Self::key_qualifies_for_fold(ctx, consistency, mode, &key)? {
                continue;
            }
            match continuation.accept_row() {
                LoopAction::Skip => continue,
                LoopAction::Emit => {}
                LoopAction::Stop => break,
            }

            match reducer.on_item(StreamItem::Key(&key))? {
                ReducerControl::Continue => {}
                ReducerControl::StopEarly => break,
            }
        }

        Ok((reducer.finish()?, keys_scanned))
    }
}
