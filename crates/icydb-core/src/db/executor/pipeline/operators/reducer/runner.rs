use crate::{
    db::{
        Context,
        cursor::{ContinuationRuntime, LoopAction},
        data::DataKey,
        executor::{
            ExecutionKernel, KeyStreamLoopControl, OrderedKeyStream,
            aggregate::{AggregateFoldMode, FoldControl},
            drive_key_stream_with_control_flow,
            traversal::row_read_consistency_for_plan,
        },
        predicate::MissingRowPolicy,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
use std::cell::RefCell;

impl ExecutionKernel {
    const fn loop_control_from_continuation_action(action: LoopAction) -> KeyStreamLoopControl {
        match action {
            LoopAction::Skip => KeyStreamLoopControl::Skip,
            LoopAction::Emit => KeyStreamLoopControl::Emit,
            LoopAction::Stop => KeyStreamLoopControl::Stop,
        }
    }

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

    // Run one scalar aggregate key fold under canonical aggregate window and
    // read-consistency eligibility contracts.
    pub(in crate::db::executor::pipeline::operators::reducer) fn run_aggregate_key_fold<E>(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery,
        mode: AggregateFoldMode,
        key_stream: &mut dyn OrderedKeyStream,
        on_key: &mut dyn FnMut(&DataKey) -> Result<FoldControl, InternalError>,
    ) -> Result<usize, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let continuation = RefCell::new(ContinuationRuntime::from_window(
            Self::window_cursor_contract(plan, None),
        ));
        let mut keys_scanned = 0usize;
        let consistency = row_read_consistency_for_plan(plan);

        // Phase 2: scan keys, apply fold eligibility/window gates, and feed reducer.
        drive_key_stream_with_control_flow(
            key_stream,
            &mut || {
                let action = continuation.borrow_mut().pre_fetch();

                Self::loop_control_from_continuation_action(action)
            },
            &mut |key| {
                keys_scanned = keys_scanned.saturating_add(1);
                if !Self::key_qualifies_for_fold(ctx, consistency, mode, &key)? {
                    return Ok(KeyStreamLoopControl::Skip);
                }
                match continuation.borrow_mut().accept_row() {
                    LoopAction::Skip => return Ok(KeyStreamLoopControl::Skip),
                    LoopAction::Emit => {}
                    LoopAction::Stop => return Ok(KeyStreamLoopControl::Stop),
                }

                Ok(match on_key(&key)? {
                    FoldControl::Continue => KeyStreamLoopControl::Emit,
                    FoldControl::Break => KeyStreamLoopControl::Stop,
                })
            },
        )?;

        Ok(keys_scanned)
    }
}
