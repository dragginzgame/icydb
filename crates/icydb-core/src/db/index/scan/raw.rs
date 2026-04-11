use crate::{
    db::{
        cursor::{
            ContinuationKeyRef, ContinuationRuntime, IndexScanContinuationInput, LoopAction,
            WindowCursorContract,
        },
        data::DataKey,
        direction::Direction,
        index::{
            entry::RawIndexEntry, envelope_is_empty, key::RawIndexKey,
            predicate::IndexPredicateExecution, store::IndexStore,
        },
    },
    error::InternalError,
    model::index::IndexModel,
    types::EntityTag,
};
use std::ops::Bound;

use crate::db::index::scan::DataKeyComponentRows;

impl IndexStore {
    // Keep bounded scan preallocation modest so common page-limited reads
    // avoid the first growth step without reserving pathologically large
    // vectors from caller-supplied limits.
    pub(in crate::db::index::scan) const LIMITED_SCAN_PREALLOC_CAP: usize = 32;

    pub(in crate::db) fn resolve_data_values_in_raw_range_limited(
        &self,
        entity: EntityTag,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        continuation: IndexScanContinuationInput<'_>,
        limit: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DataKey>, InternalError> {
        self.resolve_raw_range_limited(bounds, continuation, limit, |raw_key, value, out| {
            Self::decode_index_entry_and_push(
                entity,
                index,
                raw_key,
                value,
                out,
                Some(limit),
                "range resolve",
                index_predicate_execution,
            )
        })
    }

    #[expect(clippy::too_many_arguments)]
    pub(in crate::db) fn resolve_data_values_with_components_in_raw_range_limited(
        &self,
        entity: EntityTag,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        continuation: IndexScanContinuationInput<'_>,
        limit: usize,
        component_indices: &[usize],
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<DataKeyComponentRows, InternalError> {
        self.resolve_raw_range_limited(bounds, continuation, limit, |raw_key, value, out| {
            Self::decode_index_entry_and_push_with_components(
                entity,
                index,
                raw_key,
                value,
                out,
                Some(limit),
                component_indices,
                "range resolve",
                index_predicate_execution,
            )
        })
    }

    // Resolve one bounded directional raw-range scan with shared continuation guards.
    fn resolve_raw_range_limited<T, F>(
        &self,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        continuation: IndexScanContinuationInput<'_>,
        limit: usize,
        mut decode_and_push: F,
    ) -> Result<Vec<T>, InternalError>
    where
        F: FnMut(&RawIndexKey, &RawIndexEntry, &mut Vec<T>) -> Result<bool, InternalError>,
    {
        // Phase 1: handle degenerate and initial-window cases without paying
        // continuation-runtime setup when there is no resume anchor.
        if limit == 0 {
            return Ok(Vec::new());
        }

        if !continuation.has_anchor() {
            if envelope_is_empty(bounds.0, bounds.1) {
                return Ok(Vec::new());
            }

            let mut out = Vec::with_capacity(limit.min(Self::LIMITED_SCAN_PREALLOC_CAP));
            match continuation.direction() {
                Direction::Asc => {
                    for entry in self.map.range((bounds.0.clone(), bounds.1.clone())) {
                        if decode_and_push(entry.key(), &entry.value(), &mut out)? {
                            return Ok(out);
                        }
                    }
                }
                Direction::Desc => {
                    for entry in self.map.range((bounds.0.clone(), bounds.1.clone())).rev() {
                        if decode_and_push(entry.key(), &entry.value(), &mut out)? {
                            return Ok(out);
                        }
                    }
                }
            }

            return Ok(out);
        }

        // Phase 2: derive validated cursor-owned resume bounds for resumed scans.
        let continuation =
            ContinuationRuntime::new(continuation, WindowCursorContract::unbounded());
        let (start_raw, end_raw) = continuation.scan_bounds(bounds)?;

        if envelope_is_empty(&start_raw, &end_raw) {
            return Ok(Vec::new());
        }

        // Phase 3: scan in directional order and decode entries until limit.
        let mut out = Vec::with_capacity(limit.min(Self::LIMITED_SCAN_PREALLOC_CAP));

        match continuation.direction() {
            Direction::Asc => {
                for entry in self.map.range((start_raw, end_raw)) {
                    let raw_key = entry.key();
                    let value = entry.value();

                    if Self::scan_range_entry(
                        &continuation,
                        raw_key,
                        &value,
                        &mut out,
                        &mut decode_and_push,
                    )? {
                        return Ok(out);
                    }
                }
            }
            Direction::Desc => {
                for entry in self.map.range((start_raw, end_raw)).rev() {
                    let raw_key = entry.key();
                    let value = entry.value();

                    if Self::scan_range_entry(
                        &continuation,
                        raw_key,
                        &value,
                        &mut out,
                        &mut decode_and_push,
                    )? {
                        return Ok(out);
                    }
                }
            }
        }

        Ok(out)
    }

    // Apply continuation advancement guard and one decode/push attempt for an entry.
    fn scan_range_entry<T, F>(
        continuation: &ContinuationRuntime<'_>,
        raw_key: &RawIndexKey,
        value: &RawIndexEntry,
        out: &mut Vec<T>,
        decode_and_push: &mut F,
    ) -> Result<bool, InternalError>
    where
        F: FnMut(&RawIndexKey, &RawIndexEntry, &mut Vec<T>) -> Result<bool, InternalError>,
    {
        match continuation.accept_key(ContinuationKeyRef::scan(raw_key))? {
            LoopAction::Skip => return Ok(false),
            LoopAction::Emit => {}
            LoopAction::Stop => return Ok(true),
        }

        decode_and_push(raw_key, value, out)
    }
}
