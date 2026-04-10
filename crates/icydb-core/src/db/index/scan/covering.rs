use crate::{
    db::{
        cursor::{
            ContinuationKeyRef, ContinuationRuntime, IndexScanContinuationInput, LoopAction,
            WindowCursorContract,
        },
        direction::Direction,
        index::{
            envelope_is_empty, key::RawIndexKey, predicate::IndexPredicateExecution,
            store::IndexStore,
        },
    },
    error::InternalError,
    model::index::IndexModel,
};
use std::ops::Bound;

use crate::db::index::scan::SingleComponentCoveringCollector;

impl IndexStore {
    #[expect(clippy::too_many_arguments)]
    pub(in crate::db) fn scan_single_component_covering_values_in_raw_range_limited<T, C>(
        &self,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        continuation: IndexScanContinuationInput<'_>,
        limit: usize,
        component_index: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        collector: &mut C,
    ) -> Result<Vec<T>, InternalError>
    where
        C: SingleComponentCoveringCollector<T>,
    {
        // Keep scan-budget semantics aligned with the older tuple-materialized
        // path: the bounded fetch limit counts scanned membership keys, not
        // only live output rows. Stale rows must therefore consume budget
        // exactly as they did before this fused fast path existed.
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(limit.min(Self::LIMITED_SCAN_PREALLOC_CAP));
        let mut scanned = 0usize;

        if !continuation.has_anchor() {
            return self.scan_single_component_covering_initial_window(
                index,
                bounds,
                continuation.direction(),
                limit,
                component_index,
                index_predicate_execution,
                &mut out,
                &mut scanned,
                collector,
            );
        }

        let continuation =
            ContinuationRuntime::new(continuation, WindowCursorContract::unbounded());
        let (start_raw, end_raw) = continuation.scan_bounds(bounds)?;

        self.scan_single_component_covering_resumed_window(
            index,
            continuation,
            (start_raw, end_raw),
            limit,
            component_index,
            index_predicate_execution,
            &mut out,
            &mut scanned,
            collector,
        )?;

        Ok(out)
    }

    #[expect(clippy::too_many_arguments)]
    fn scan_single_component_covering_initial_window<T, C>(
        &self,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        direction: Direction,
        limit: usize,
        component_index: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        out: &mut Vec<T>,
        scanned: &mut usize,
        collector: &mut C,
    ) -> Result<Vec<T>, InternalError>
    where
        C: SingleComponentCoveringCollector<T>,
    {
        if envelope_is_empty(bounds.0, bounds.1) {
            return Ok(Vec::new());
        }

        match direction {
            Direction::Asc => {
                for entry in self.map.range((bounds.0.clone(), bounds.1.clone())) {
                    if Self::decode_index_entry_and_collect_with_component(
                        index,
                        entry.key(),
                        &entry.value(),
                        out,
                        scanned,
                        limit,
                        component_index,
                        "range resolve",
                        index_predicate_execution,
                        collector,
                    )? {
                        break;
                    }
                }
            }
            Direction::Desc => {
                for entry in self.map.range((bounds.0.clone(), bounds.1.clone())).rev() {
                    if Self::decode_index_entry_and_collect_with_component(
                        index,
                        entry.key(),
                        &entry.value(),
                        out,
                        scanned,
                        limit,
                        component_index,
                        "range resolve",
                        index_predicate_execution,
                        collector,
                    )? {
                        break;
                    }
                }
            }
        }

        Ok(std::mem::take(out))
    }

    #[expect(clippy::too_many_arguments)]
    fn scan_single_component_covering_resumed_window<T, C>(
        &self,
        index: &IndexModel,
        continuation: ContinuationRuntime<'_>,
        bounds: (Bound<RawIndexKey>, Bound<RawIndexKey>),
        limit: usize,
        component_index: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        out: &mut Vec<T>,
        scanned: &mut usize,
        collector: &mut C,
    ) -> Result<(), InternalError>
    where
        C: SingleComponentCoveringCollector<T>,
    {
        if envelope_is_empty(&bounds.0, &bounds.1) {
            return Ok(());
        }

        match continuation.direction() {
            Direction::Asc => {
                for entry in self.map.range(bounds) {
                    let raw_key = entry.key();
                    let value = entry.value();

                    match continuation.accept_key(ContinuationKeyRef::scan(raw_key))? {
                        LoopAction::Skip => continue,
                        LoopAction::Emit => {}
                        LoopAction::Stop => return Ok(()),
                    }

                    if Self::decode_index_entry_and_collect_with_component(
                        index,
                        raw_key,
                        &value,
                        out,
                        scanned,
                        limit,
                        component_index,
                        "range resolve",
                        index_predicate_execution,
                        collector,
                    )? {
                        return Ok(());
                    }
                }
            }
            Direction::Desc => {
                for entry in self.map.range(bounds).rev() {
                    let raw_key = entry.key();
                    let value = entry.value();

                    match continuation.accept_key(ContinuationKeyRef::scan(raw_key))? {
                        LoopAction::Skip => continue,
                        LoopAction::Emit => {}
                        LoopAction::Stop => return Ok(()),
                    }

                    if Self::decode_index_entry_and_collect_with_component(
                        index,
                        raw_key,
                        &value,
                        out,
                        scanned,
                        limit,
                        component_index,
                        "range resolve",
                        index_predicate_execution,
                        collector,
                    )? {
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }
}
