//! Module: db::executor::pipeline::operators::reducer::runner
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::operators::reducer::runner.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
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
        registry::StoreHandle,
    },
    error::InternalError,
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
    fn key_qualifies_for_fold(
        store: StoreHandle,
        consistency: MissingRowPolicy,
        mode: AggregateFoldMode,
        key: &DataKey,
    ) -> Result<bool, InternalError> {
        match mode {
            AggregateFoldMode::KeysOnly => Ok(true),
            AggregateFoldMode::ExistingRows => Self::row_exists_for_key(store, consistency, key),
        }
    }

    // Keep read-consistency behavior aligned with materialized row reads.
    fn row_exists_for_key(
        store: StoreHandle,
        consistency: MissingRowPolicy,
        key: &DataKey,
    ) -> Result<bool, InternalError> {
        let read_row = |key: &DataKey| -> Result<Option<crate::db::data::RawRow>, InternalError> {
            let raw_key = key.to_raw()?;

            Ok(store.with_data(|data| data.get(&raw_key)))
        };

        match consistency {
            MissingRowPolicy::Error => {
                let Some(_) = read_row(key)? else {
                    return Err(crate::db::executor::ExecutorError::missing_row(key).into());
                };

                Ok(true)
            }
            MissingRowPolicy::Ignore => Ok(read_row(key)?.is_some()),
        }
    }

    // Run one scalar aggregate key fold under canonical aggregate window and
    // read-consistency eligibility contracts.
    pub(in crate::db::executor::pipeline::operators::reducer) fn run_aggregate_key_fold(
        store: StoreHandle,
        plan: &AccessPlannedQuery,
        mode: AggregateFoldMode,
        key_stream: &mut dyn OrderedKeyStream,
        on_key: &mut dyn FnMut(&DataKey) -> Result<FoldControl, InternalError>,
    ) -> Result<usize, InternalError> {
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
                if !Self::key_qualifies_for_fold(store, consistency, mode, &key)? {
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
