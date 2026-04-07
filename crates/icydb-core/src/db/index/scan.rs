//! Module: index::scan
//! Responsibility: raw-range index scan resolution under cursor-owned continuation contracts.
//! Does not own: index persistence layout or predicate compilation.
//! Boundary: executor/query range reads go through this layer above `index::store`.

use crate::{
    db::{
        cursor::{
            ContinuationKeyRef, ContinuationRuntime, IndexScanContinuationInput, LoopAction,
            WindowCursorContract,
        },
        data::DataKey,
        direction::Direction,
        executor::{
            record_row_check_index_entry_scanned, record_row_check_index_membership_key_decoded,
            record_row_check_index_membership_multi_key_entry,
            record_row_check_index_membership_single_key_entry,
        },
        index::{
            IndexEntryExistenceWitness, IndexKey,
            entry::RawIndexEntry,
            envelope_is_empty,
            key::RawIndexKey,
            predicate::{IndexPredicateExecution, eval_index_execution_on_decoded_key},
            store::IndexStore,
        },
    },
    error::InternalError,
    model::index::IndexModel,
    types::EntityTag,
    value::StorageKey,
};
use std::ops::Bound;

type IndexComponentValues = Vec<Vec<u8>>;
type DataKeyWitnessRows = Vec<(DataKey, IndexEntryExistenceWitness)>;
type DataKeyComponentRows = Vec<(DataKey, IndexEntryExistenceWitness, IndexComponentValues)>;

///
/// SingleComponentCoveringCollector
///
/// Narrow collector contract for the single-component covering fast path.
/// The index layer streams decoded membership entries through this boundary
/// without owning projection semantics beyond "emit storage key + component".
///

pub(in crate::db) trait SingleComponentCoveringCollector<T> {
    fn push(
        &mut self,
        storage_key: StorageKey,
        existence_witness: IndexEntryExistenceWitness,
        component: &[u8],
        out: &mut Vec<T>,
    ) -> Result<(), InternalError>;
}

impl IndexStore {
    // Keep bounded scan preallocation modest so common page-limited reads
    // avoid the first growth step without reserving pathologically large
    // vectors from caller-supplied limits.
    const LIMITED_SCAN_PREALLOC_CAP: usize = 32;

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

    pub(in crate::db) fn resolve_data_values_with_witness_in_raw_range_limited(
        &self,
        entity: EntityTag,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        continuation: IndexScanContinuationInput<'_>,
        limit: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<DataKeyWitnessRows, InternalError> {
        self.resolve_raw_range_limited(bounds, continuation, limit, |raw_key, value, out| {
            Self::decode_index_entry_and_push_with_witness(
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

    #[expect(clippy::too_many_arguments)]
    fn decode_index_entry_and_push(
        entity: EntityTag,
        index: &IndexModel,
        raw_key: &RawIndexKey,
        value: &RawIndexEntry,
        out: &mut Vec<DataKey>,
        limit: Option<usize>,
        context: &'static str,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<bool, InternalError> {
        record_row_check_index_entry_scanned();

        // Phase 1: only decode raw key components when an index-only
        // predicate needs them. Plain membership scans only need the entry
        // payload, so they should not pay raw-key decode on every hit.
        if let Some(execution) = index_predicate_execution {
            let decoded_key = IndexKey::try_from_raw(raw_key)
                .map_err(|err| InternalError::index_scan_key_corrupted_during(context, err))?;
            if !eval_index_execution_on_decoded_key(&decoded_key, execution)? {
                return Ok(false);
            }
        }

        // Phase 2: fast-path one-key entries without allocating the full
        // membership vector.
        if let Some(storage_key) = value
            .decode_single_key()
            .map_err(InternalError::index_entry_decode_failed)?
        {
            record_row_check_index_membership_single_key_entry();
            record_row_check_index_membership_key_decoded();
            out.push(DataKey::new(entity, storage_key));

            if let Some(limit) = limit
                && out.len() == limit
            {
                return Ok(true);
            }

            return Ok(false);
        }

        // Phase 3: stream multi-key entry payloads without first allocating
        // a membership vector, but still validate the full entry before
        // returning to the caller.
        let mut halted = false;
        let mut decoded_keys = 0usize;
        record_row_check_index_membership_multi_key_entry();
        let mut storage_keys = value
            .iter_keys()
            .map_err(InternalError::index_entry_decode_failed)?;

        for storage_key in &mut storage_keys {
            let storage_key = storage_key.map_err(InternalError::index_entry_decode_failed)?;
            decoded_keys = decoded_keys.saturating_add(1);
            record_row_check_index_membership_key_decoded();

            if halted {
                continue;
            }

            out.push(DataKey::new(entity, storage_key));

            if let Some(limit) = limit
                && out.len() == limit
            {
                halted = true;
            }
        }

        if index.is_unique() && decoded_keys != 1 {
            return Err(InternalError::unique_index_entry_single_key_required());
        }

        Ok(halted)
    }

    #[expect(clippy::too_many_arguments)]
    fn decode_index_entry_and_push_with_witness(
        entity: EntityTag,
        index: &IndexModel,
        raw_key: &RawIndexKey,
        value: &RawIndexEntry,
        out: &mut DataKeyWitnessRows,
        limit: Option<usize>,
        context: &'static str,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<bool, InternalError> {
        record_row_check_index_entry_scanned();

        // Phase 1: preserve the older raw-key decode discipline. Membership
        // scans only pay decoded-key cost when an index-only predicate still
        // needs the decoded view.
        if let Some(execution) = index_predicate_execution {
            let decoded_key = IndexKey::try_from_raw(raw_key)
                .map_err(|err| InternalError::index_scan_key_corrupted_during(context, err))?;
            if !eval_index_execution_on_decoded_key(&decoded_key, execution)? {
                return Ok(false);
            }
        }

        // Phase 2: fast-path one-key entries without allocating the full
        // membership vector.
        if let Some(membership) = value
            .decode_single_membership()
            .map_err(InternalError::index_entry_decode_failed)?
        {
            record_row_check_index_membership_single_key_entry();
            record_row_check_index_membership_key_decoded();
            out.push((
                DataKey::new(entity, membership.storage_key()),
                membership.existence_witness(),
            ));

            if let Some(limit) = limit
                && out.len() == limit
            {
                return Ok(true);
            }

            return Ok(false);
        }

        // Phase 3: stream multi-key entry payloads without first allocating a
        // membership vector, but still validate the full entry before
        // returning to the caller.
        let mut halted = false;
        let mut decoded_keys = 0usize;
        record_row_check_index_membership_multi_key_entry();
        let mut memberships = value
            .iter_memberships()
            .map_err(InternalError::index_entry_decode_failed)?;

        for membership in &mut memberships {
            let membership = membership.map_err(InternalError::index_entry_decode_failed)?;
            decoded_keys = decoded_keys.saturating_add(1);
            record_row_check_index_membership_key_decoded();

            if halted {
                continue;
            }

            out.push((
                DataKey::new(entity, membership.storage_key()),
                membership.existence_witness(),
            ));

            if let Some(limit) = limit
                && out.len() == limit
            {
                halted = true;
            }
        }

        if index.is_unique() && decoded_keys != 1 {
            return Err(InternalError::unique_index_entry_single_key_required());
        }

        Ok(halted)
    }

    #[expect(clippy::too_many_arguments)]
    fn decode_index_entry_and_push_with_components(
        entity: EntityTag,
        index: &IndexModel,
        raw_key: &RawIndexKey,
        value: &RawIndexEntry,
        out: &mut Vec<(DataKey, IndexEntryExistenceWitness, Vec<Vec<u8>>)>,
        limit: Option<usize>,
        component_indices: &[usize],
        context: &'static str,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<bool, InternalError> {
        record_row_check_index_entry_scanned();

        // Phase 1: decode the raw key once, extract every requested component,
        // and evaluate any optional index-only predicate against that one
        // decoded key view.
        let decoded_key = IndexKey::try_from_raw(raw_key)
            .map_err(|err| InternalError::index_scan_key_corrupted_during(context, err))?;
        let mut components = Vec::with_capacity(component_indices.len());
        for component_index in component_indices {
            let Some(component) = decoded_key.component(*component_index) else {
                return Err(InternalError::index_projection_component_required(
                    index.name(),
                    *component_index,
                ));
            };
            components.push(component.to_vec());
        }

        if let Some(execution) = index_predicate_execution
            && !eval_index_execution_on_decoded_key(&decoded_key, execution)?
        {
            return Ok(false);
        }

        // Phase 2: fast-path one-key entries without allocating the full
        // membership vector.
        if let Some(membership) = value
            .decode_single_membership()
            .map_err(InternalError::index_entry_decode_failed)?
        {
            record_row_check_index_membership_single_key_entry();
            record_row_check_index_membership_key_decoded();
            out.push((
                DataKey::new(entity, membership.storage_key()),
                membership.existence_witness(),
                components,
            ));

            if let Some(limit) = limit
                && out.len() == limit
            {
                return Ok(true);
            }

            return Ok(false);
        }

        // Phase 3: stream multi-key entry payloads without first allocating
        // a membership vector, but still validate the full entry before
        // returning to the caller.
        let mut halted = false;
        let mut decoded_keys = 0usize;
        record_row_check_index_membership_multi_key_entry();
        let mut memberships = value
            .iter_memberships()
            .map_err(InternalError::index_entry_decode_failed)?;

        for membership in &mut memberships {
            let membership = membership.map_err(InternalError::index_entry_decode_failed)?;
            decoded_keys = decoded_keys.saturating_add(1);
            record_row_check_index_membership_key_decoded();

            if halted {
                continue;
            }

            out.push((
                DataKey::new(entity, membership.storage_key()),
                membership.existence_witness(),
                components.clone(),
            ));

            if let Some(limit) = limit
                && out.len() == limit
            {
                halted = true;
            }
        }

        if index.is_unique() && decoded_keys != 1 {
            return Err(InternalError::unique_index_entry_single_key_required());
        }

        Ok(halted)
    }

    #[expect(clippy::too_many_arguments)]
    fn decode_index_entry_and_collect_with_component<T, C>(
        index: &IndexModel,
        raw_key: &RawIndexKey,
        value: &RawIndexEntry,
        out: &mut Vec<T>,
        scanned: &mut usize,
        limit: usize,
        component_index: usize,
        context: &'static str,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        collector: &mut C,
    ) -> Result<bool, InternalError>
    where
        C: SingleComponentCoveringCollector<T>,
    {
        record_row_check_index_entry_scanned();

        // Phase 1: extract the requested component and evaluate any optional
        // index-only predicate. When no predicate needs the full decoded key,
        // validate and read only the requested component segment.
        let predicate_component;
        let component = if let Some(execution) = index_predicate_execution {
            let decoded_key = IndexKey::try_from_raw(raw_key)
                .map_err(|err| InternalError::index_scan_key_corrupted_during(context, err))?;
            if !eval_index_execution_on_decoded_key(&decoded_key, execution)? {
                return Ok(false);
            }

            // Keep the predicate-evaluation branch simple and semantically
            // identical to the older path. The raw-component fast path below
            // is reserved for the hotter no-index-predicate cohort.
            predicate_component = decoded_key
                .component(component_index)
                .ok_or_else(|| {
                    InternalError::index_projection_component_required(
                        index.name(),
                        component_index,
                    )
                })?
                .to_vec();

            predicate_component.as_slice()
        } else {
            raw_key
                .validated_component(component_index)
                .map_err(|err| InternalError::index_scan_key_corrupted_during(context, err))?
                .ok_or_else(|| {
                    InternalError::index_projection_component_required(
                        index.name(),
                        component_index,
                    )
                })?
        };

        // Phase 2: stream single-key entries directly into the caller-owned
        // collector so narrow covering-read paths can skip intermediate tuple
        // staging.
        if let Some(membership) = value
            .decode_single_membership()
            .map_err(InternalError::index_entry_decode_failed)?
        {
            record_row_check_index_membership_single_key_entry();
            record_row_check_index_membership_key_decoded();
            collector.push(
                membership.storage_key(),
                membership.existence_witness(),
                component,
                out,
            )?;
            *scanned = scanned.saturating_add(1);
            if *scanned == limit {
                return Ok(true);
            }

            return Ok(false);
        }

        // Phase 3: preserve the existing multi-key semantics while still
        // streaming directly into the caller-owned collector and validating
        // the full entry before returning.
        let mut halted = false;
        let mut decoded_keys = 0usize;
        record_row_check_index_membership_multi_key_entry();
        let mut memberships = value
            .iter_memberships()
            .map_err(InternalError::index_entry_decode_failed)?;

        for membership in &mut memberships {
            let membership = membership.map_err(InternalError::index_entry_decode_failed)?;
            decoded_keys = decoded_keys.saturating_add(1);
            record_row_check_index_membership_key_decoded();

            if halted {
                continue;
            }

            collector.push(
                membership.storage_key(),
                membership.existence_witness(),
                component,
                out,
            )?;
            *scanned = scanned.saturating_add(1);
            if *scanned == limit {
                halted = true;
            }
        }

        if index.is_unique() && decoded_keys != 1 {
            return Err(InternalError::unique_index_entry_single_key_required());
        }

        Ok(halted)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::IndexStore;
    use crate::{
        db::{
            data::{DataKey, StorageKey},
            index::{RawIndexEntry, RawIndexKey},
            with_row_check_metrics,
        },
        error::ErrorClass,
        model::index::IndexModel,
        traits::Storable,
        types::EntityTag,
    };
    use std::borrow::Cow;

    const TEST_SCAN_INDEX_FIELDS: &[&str] = &["name"];
    const TEST_SCAN_INDEX: IndexModel = IndexModel::new(
        "scan::idx_name",
        "scan::IndexStore",
        TEST_SCAN_INDEX_FIELDS,
        false,
    );

    #[test]
    fn decode_index_entry_and_push_without_index_predicate_skips_raw_key_decode() {
        let entity = EntityTag::new(7);
        let raw_key = RawIndexKey::from_bytes(Cow::Owned(vec![0xFF]));
        let raw_entry =
            RawIndexEntry::try_from_keys([StorageKey::Uint(11)]).expect("encode index entry");
        let mut out = Vec::new();

        let halted = IndexStore::decode_index_entry_and_push(
            entity,
            &TEST_SCAN_INDEX,
            &raw_key,
            &raw_entry,
            &mut out,
            Some(1),
            "test scan",
            None,
        )
        .expect("plain membership scan should not require raw key decode");

        assert!(halted, "bounded single-row scan should stop at the limit");
        assert_eq!(out, vec![DataKey::new(entity, StorageKey::Uint(11))]);
    }

    #[test]
    fn decode_index_entry_and_push_records_single_key_row_check_metrics() {
        let entity = EntityTag::new(7);
        let raw_key = RawIndexKey::from_bytes(Cow::Owned(vec![0xFF]));
        let raw_entry =
            RawIndexEntry::try_from_keys([StorageKey::Uint(11)]).expect("encode index entry");

        let ((halted, out), metrics) = with_row_check_metrics(|| {
            let mut out = Vec::new();
            let halted = IndexStore::decode_index_entry_and_push(
                entity,
                &TEST_SCAN_INDEX,
                &raw_key,
                &raw_entry,
                &mut out,
                Some(1),
                "test scan",
                None,
            )
            .expect("single-key scan should succeed");

            (halted, out)
        });

        assert!(halted, "bounded single-row scan should stop at the limit");
        assert_eq!(out, vec![DataKey::new(entity, StorageKey::Uint(11))]);
        assert_eq!(metrics.index_entries_scanned, 1);
        assert_eq!(metrics.index_membership_single_key_entries, 1);
        assert_eq!(metrics.index_membership_multi_key_entries, 0);
        assert_eq!(metrics.index_membership_keys_decoded, 1);
    }

    #[test]
    fn decode_index_entry_and_push_limit_still_validates_full_multi_key_entry() {
        let entity = EntityTag::new(7);
        let raw_key = RawIndexKey::from_bytes(Cow::Owned(vec![0xFF]));
        let duplicate = StorageKey::Uint(11);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2u32.to_be_bytes());
        bytes.extend_from_slice(&duplicate.to_bytes().expect("encode first key"));
        bytes.extend_from_slice(&duplicate.to_bytes().expect("encode second key"));
        let raw_entry = RawIndexEntry::from_bytes(Cow::Owned(bytes));
        let mut out = Vec::new();

        let err = IndexStore::decode_index_entry_and_push(
            entity,
            &TEST_SCAN_INDEX,
            &raw_key,
            &raw_entry,
            &mut out,
            Some(1),
            "test scan",
            None,
        )
        .expect_err("bounded multi-key scan must still reject duplicate membership corruption");

        assert_eq!(err.class(), ErrorClass::Corruption);
    }

    #[test]
    fn decode_index_entry_and_push_records_multi_key_row_check_metrics() {
        let entity = EntityTag::new(7);
        let raw_key = RawIndexKey::from_bytes(Cow::Owned(vec![0xFF]));
        let raw_entry = RawIndexEntry::try_from_keys([StorageKey::Uint(11), StorageKey::Uint(12)])
            .expect("encode multi-key entry");

        let ((halted, out), metrics) = with_row_check_metrics(|| {
            let mut out = Vec::new();
            let halted = IndexStore::decode_index_entry_and_push(
                entity,
                &TEST_SCAN_INDEX,
                &raw_key,
                &raw_entry,
                &mut out,
                Some(2),
                "test scan",
                None,
            )
            .expect("multi-key scan should succeed");

            (halted, out)
        });

        assert!(halted, "bounded multi-key scan should stop at the limit");
        assert_eq!(
            out,
            vec![
                DataKey::new(entity, StorageKey::Uint(11)),
                DataKey::new(entity, StorageKey::Uint(12)),
            ],
        );
        assert_eq!(metrics.index_entries_scanned, 1);
        assert_eq!(metrics.index_membership_single_key_entries, 0);
        assert_eq!(metrics.index_membership_multi_key_entries, 1);
        assert_eq!(metrics.index_membership_keys_decoded, 2);
    }
}
