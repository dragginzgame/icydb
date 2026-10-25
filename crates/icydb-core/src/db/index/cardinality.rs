//! Module: index::cardinality
//! Responsibility: in-memory exact cardinality metadata for decoded index prefixes.
//! Does not own: query planning, index scan execution, or row-store mutation ordering.
//! Boundary: index store maintains this opportunistic metadata; callers must prove row/index sync.

use crate::db::index::{
    BorrowedIndexKeyFrame, IndexEntryExistenceWitness, IndexEntryValue, IndexId, IndexKeyKind,
    RawIndexStoreKey, decode_canonical_index_int64_component, decode_index_key_frame,
};
use crate::db::journal::FoldWatermark;
use crate::db::resource::budget::charge_current_execution_budget;
use crate::error::InternalError;
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;
use std::collections::{BTreeMap as HeapBTreeMap, btree_map::Entry};
use std::ops::Bound;

/// Exact lookup key for one user-index component prefix.
///
/// This is the shared engine contract consumed by access lowering, direct
/// aggregate execution, and executor branch-liveness probes. It deliberately
/// omits `IndexKeyKind`: user-index ownership is part of the type contract.
/// The ordered cardinality map retains its separate storage key below.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct UserIndexPrefixCardinalityKey {
    index_id: IndexId,
    prefix_components: Vec<Vec<u8>>,
}

impl UserIndexPrefixCardinalityKey {
    /// Construct one lookup from an index generation and encoded components.
    #[must_use]
    pub(in crate::db) const fn new(index_id: IndexId, prefix_components: Vec<Vec<u8>>) -> Self {
        Self {
            index_id,
            prefix_components,
        }
    }

    /// Return the physical index generation owning this prefix.
    #[must_use]
    pub(in crate::db) const fn index_id(&self) -> IndexId {
        self.index_id
    }

    /// Borrow the already-encoded prefix components.
    #[must_use]
    pub(in crate::db) const fn prefix_components(&self) -> &[Vec<u8>] {
        self.prefix_components.as_slice()
    }
}

///
/// IndexPrefixCardinality
///
/// Exact in-memory counts for non-empty user-index component prefixes.
///
/// The metadata is usable only when:
/// - all observed index entries decoded cleanly; and
/// - the caller-provided row-store generation matches the generation recorded
///   after the last authoritative row/index commit transition.
///
/// For that synchronized generation, every retained first-component key owns
/// exactly one canonical non-empty leading value present in the logical row
/// set, its count is that value's row multiplicity, and no other key exists.
/// Row/index mutation invalidates synchronization before changing either
/// projection; commit, rollback, replay, and fold stamp the row generation only
/// after their complete authoritative transition.
///
#[derive(Clone, Debug)]
pub(super) struct IndexPrefixCardinality {
    first_component_counts: HeapBTreeMap<IndexPrefixCardinalityFirstKey, u64>,
    counts: HeapBTreeMap<IndexPrefixCardinalityKey, u64>,
    data_generation: Option<u64>,
    decodable: bool,
}

/// Exact signed contribution of the bounded live overlay relative to canonical state.
#[derive(Clone, Debug)]
pub(super) struct IndexPrefixCardinalityDelta {
    first_component_counts: HeapBTreeMap<IndexPrefixCardinalityFirstKey, i64>,
    counts: HeapBTreeMap<IndexPrefixCardinalityKey, i64>,
    base_watermark: Option<FoldWatermark>,
    decodable: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct IndexPrefixCardinalityFirstKey {
    key_kind: IndexKeyKind,
    index_id: IndexId,
    component: Vec<u8>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct IndexPrefixCardinalityKey {
    key_kind: IndexKeyKind,
    index_id: IndexId,
    components: Vec<Vec<u8>>,
}

impl IndexPrefixCardinality {
    const FIRST_COMPONENT_BATCH_INTERSECTION_MIN: usize = 32;

    #[must_use]
    pub(super) const fn synchronized_empty() -> Self {
        Self {
            first_component_counts: HeapBTreeMap::new(),
            counts: HeapBTreeMap::new(),
            data_generation: Some(0),
            decodable: true,
        }
    }

    #[must_use]
    pub(super) const fn unavailable() -> Self {
        Self {
            first_component_counts: HeapBTreeMap::new(),
            counts: HeapBTreeMap::new(),
            data_generation: None,
            decodable: false,
        }
    }

    pub(super) const fn mark_synchronized(&mut self, data_generation: u64) {
        if self.decodable {
            self.data_generation = Some(data_generation);
        }
    }

    #[must_use]
    pub(super) fn exact_count(
        &self,
        data_generation: u64,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        components: &[Vec<u8>],
    ) -> Option<u64> {
        if !self.decodable || self.data_generation != Some(data_generation) {
            return None;
        }

        Some(self.exact_count_synchronized(key_kind, index_id, components))
    }

    fn try_fold_exact_first_components<T>(
        &self,
        data_generation: u64,
        index_id: IndexId,
        bounds: (&Bound<Vec<u8>>, &Bound<Vec<u8>>),
        stop_after: u64,
        mut accumulator: T,
        mut fold: impl FnMut(T, &[u8], u64) -> Result<T, InternalError>,
    ) -> Result<Option<(T, u64, bool)>, InternalError> {
        if stop_after == 0 || !self.decodable || self.data_generation != Some(data_generation) {
            return Ok(None);
        }

        let key_kind = IndexKeyKind::User;
        let (lower, upper) = bounds;
        let lower = match lower {
            Bound::Unbounded => Bound::Included(IndexPrefixCardinalityFirstKey::range_start(
                key_kind, index_id,
            )),
            Bound::Included(component) => Bound::Included(IndexPrefixCardinalityFirstKey::new(
                key_kind, index_id, component,
            )),
            Bound::Excluded(component) => Bound::Excluded(IndexPrefixCardinalityFirstKey::new(
                key_kind, index_id, component,
            )),
        };
        let mut examined = 0_u64;
        for (key, multiplicity) in self.first_component_counts.range((lower, Bound::Unbounded)) {
            if !key.matches_identity(key_kind, index_id)
                || match upper {
                    Bound::Unbounded => false,
                    Bound::Included(upper) => key.component.as_slice() > upper.as_slice(),
                    Bound::Excluded(upper) => key.component.as_slice() >= upper.as_slice(),
                }
            {
                break;
            }
            if examined == stop_after {
                return Ok(Some((accumulator, examined, false)));
            }
            if *multiplicity == 0 {
                return Err(InternalError::store_invariant());
            }
            accumulator = fold(accumulator, key.component.as_slice(), *multiplicity)?;
            examined = checked_metadata_count_increment(examined)?;
        }

        Ok(Some((accumulator, examined, true)))
    }

    /// Sum exact leading-component multiplicities inside one canonical range.
    pub(super) fn exact_first_component_range_count(
        &self,
        data_generation: u64,
        index_id: IndexId,
        lower: &Bound<Vec<u8>>,
        upper: &Bound<Vec<u8>>,
        stop_after: u64,
    ) -> Result<Option<(u64, u64, bool)>, InternalError> {
        self.try_fold_exact_first_components(
            data_generation,
            index_id,
            (lower, upper),
            stop_after,
            0_u64,
            |total, _component, multiplicity| {
                total
                    .checked_add(multiplicity)
                    .ok_or_else(InternalError::store_invariant)
            },
        )
    }

    /// Fold exact synchronized `Int32` leading values and multiplicities.
    pub(super) fn exact_first_component_numeric_fold(
        &self,
        data_generation: u64,
        index_id: IndexId,
        stop_after: u64,
    ) -> Result<Option<(u64, i128, u64, bool)>, InternalError> {
        self.try_fold_exact_first_components(
            data_generation,
            index_id,
            (&Bound::Unbounded, &Bound::Unbounded),
            stop_after,
            (0_u64, 0_i128),
            |(count, sum), component, multiplicity| {
                let value = decode_canonical_index_int64_component(component)?;
                let value = i32::try_from(value).map_err(|_| InternalError::store_invariant())?;
                let contribution = i128::from(value)
                    .checked_mul(i128::from(multiplicity))
                    .ok_or_else(InternalError::store_invariant)?;
                let count = count
                    .checked_add(multiplicity)
                    .ok_or_else(InternalError::store_invariant)?;
                let sum = sum
                    .checked_add(contribution)
                    .ok_or_else(InternalError::store_invariant)?;
                Ok((count, sum))
            },
        )
        .map(|result| {
            result.map(|((count, sum), examined, complete)| (count, sum, examined, complete))
        })
    }

    #[must_use]
    pub(super) fn exact_count_sum<'a>(
        &self,
        data_generation: u64,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        component_prefixes: impl IntoIterator<Item = &'a [Vec<u8>]>,
        stop_after: Option<u64>,
    ) -> Option<u64> {
        if !self.decodable || self.data_generation != Some(data_generation) {
            return None;
        }

        let component_prefixes = component_prefixes.into_iter().collect::<Vec<_>>();
        if component_prefixes
            .iter()
            .all(|components| components.len() == 1)
        {
            return Some(self.exact_first_component_count_sum(
                key_kind,
                index_id,
                component_prefixes.as_slice(),
                stop_after,
            ));
        }

        Some(self.exact_general_count_sum(
            key_kind,
            index_id,
            component_prefixes.as_slice(),
            stop_after,
        ))
    }

    pub(super) fn exact_child_prefixes_for_parent_set<'a>(
        &self,
        data_generation: u64,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        parent_component_prefixes: impl IntoIterator<Item = &'a [Vec<u8>]>,
        max_children: usize,
    ) -> Result<Option<Vec<Vec<Vec<u8>>>>, InternalError> {
        if !self.decodable || self.data_generation != Some(data_generation) {
            return Ok(None);
        }

        self.exact_child_prefixes_for_parent_set_synchronized(
            key_kind,
            index_id,
            parent_component_prefixes,
            max_children,
        )
    }

    fn exact_child_prefixes_for_parent_set_synchronized<'a>(
        &self,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        parent_component_prefixes: impl IntoIterator<Item = &'a [Vec<u8>]>,
        max_children: usize,
    ) -> Result<Option<Vec<Vec<Vec<u8>>>>, InternalError> {
        let mut parents = parent_component_prefixes.into_iter().collect::<Vec<_>>();
        if parents.iter().any(|parent| parent.is_empty()) {
            return Ok(None);
        }
        parents.sort_unstable();
        parents.dedup();
        let Some(parent_len) = parents.first().map(|parent| parent.len()) else {
            return Ok(Some(Vec::new()));
        };
        if parents.iter().any(|parent| parent.len() != parent_len) {
            return Ok(None);
        }

        let child_len = parent_len.saturating_add(1);
        let mut children = Vec::new();

        // Equal-length, sorted parents have disjoint lexicographic ranges.
        // Seek each range directly rather than scanning unrelated metadata.
        for parent in parents {
            let start = IndexPrefixCardinalityKey::new(key_kind, index_id, parent);
            for (key, _count) in self.counts.range(start..) {
                charge_current_execution_budget(
                    DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
                    1,
                )?;
                if !key.matches_identity(key_kind, index_id) || !key.components.starts_with(parent)
                {
                    break;
                }
                if key.components.len() != child_len {
                    continue;
                }
                if children.len() == max_children {
                    return Ok(None);
                }
                children.push(key.components.clone());
            }
        }

        Ok(Some(children))
    }

    fn exact_count_synchronized(
        &self,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        components: &[Vec<u8>],
    ) -> u64 {
        if let Some(first_component) = components.first().filter(|_| components.len() == 1) {
            return self
                .first_component_counts
                .get(&IndexPrefixCardinalityFirstKey::new(
                    key_kind,
                    index_id,
                    first_component,
                ))
                .copied()
                .unwrap_or(0);
        }

        self.counts
            .get(&IndexPrefixCardinalityKey::new(
                key_kind, index_id, components,
            ))
            .copied()
            .unwrap_or(0)
    }

    fn exact_general_count_sum(
        &self,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        component_prefixes: &[&[Vec<u8>]],
        stop_after: Option<u64>,
    ) -> u64 {
        let mut total = 0_u64;
        for components in component_prefixes {
            total =
                total.saturating_add(self.exact_count_synchronized(key_kind, index_id, components));
            if stop_after.is_some_and(|required| total >= required) {
                break;
            }
        }

        total
    }

    fn exact_first_component_count_sum(
        &self,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        component_prefixes: &[&[Vec<u8>]],
        stop_after: Option<u64>,
    ) -> u64 {
        let mut requested_components = first_components(component_prefixes);
        if requested_components.is_empty() {
            return 0;
        }

        requested_components.sort_unstable();
        requested_components.dedup();
        if self.should_intersect_first_component_counts(requested_components.len()) {
            return self.exact_first_component_count_sum_by_intersection(
                key_kind,
                index_id,
                requested_components.as_slice(),
                stop_after,
            );
        }

        self.exact_first_component_count_sum_by_lookup(
            key_kind,
            index_id,
            requested_components.as_slice(),
            stop_after,
        )
    }

    fn should_intersect_first_component_counts(&self, requested_component_count: usize) -> bool {
        requested_component_count >= Self::FIRST_COMPONENT_BATCH_INTERSECTION_MIN
            && requested_component_count >= self.first_component_counts.len().saturating_div(2)
    }

    fn exact_first_component_count_sum_by_lookup(
        &self,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        requested_components: &[&[u8]],
        stop_after: Option<u64>,
    ) -> u64 {
        let mut total = 0_u64;
        for component in requested_components {
            total = total.saturating_add(
                self.first_component_counts
                    .get(&IndexPrefixCardinalityFirstKey::new(
                        key_kind, index_id, component,
                    ))
                    .copied()
                    .unwrap_or(0),
            );
            if stop_after.is_some_and(|required| total >= required) {
                break;
            }
        }

        total
    }

    fn exact_first_component_count_sum_by_intersection(
        &self,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        requested_components: &[&[u8]],
        stop_after: Option<u64>,
    ) -> u64 {
        let start = IndexPrefixCardinalityFirstKey::range_start(key_kind, index_id);
        let mut total = 0_u64;
        let mut remaining = requested_components.len();
        for (key, count) in self.first_component_counts.range(start..) {
            if !key.matches_identity(key_kind, index_id) {
                break;
            }
            if requested_components
                .binary_search_by(|component| component.cmp(&key.component.as_slice()))
                .is_err()
            {
                continue;
            }

            total = total.saturating_add(*count);
            remaining = remaining.saturating_sub(1);
            if remaining == 0 || stop_after.is_some_and(|required| total >= required) {
                break;
            }
        }

        total
    }

    pub(super) fn apply_insert(
        &mut self,
        raw_key: &RawIndexStoreKey,
        previous: Option<&IndexEntryValue>,
        new: &IndexEntryValue,
    ) {
        self.apply_transition(raw_key, previous, Some(new));
    }

    pub(super) fn apply_remove(
        &mut self,
        raw_key: &RawIndexStoreKey,
        previous: Option<&IndexEntryValue>,
    ) {
        self.apply_transition(raw_key, previous, None);
    }

    fn apply_transition(
        &mut self,
        raw_key: &RawIndexStoreKey,
        previous: Option<&IndexEntryValue>,
        next: Option<&IndexEntryValue>,
    ) {
        if !self.decodable || (previous.is_none() && next.is_none()) {
            return;
        }
        let Some((frame, delta)) = counted_prefix_change(raw_key, previous, next) else {
            self.invalidate_decoding();
            return;
        };
        if delta == 0 {
            return;
        }
        self.data_generation = None;
        let direction = if delta > 0 {
            PrefixCardinalityDelta::Increment
        } else {
            PrefixCardinalityDelta::Decrement
        };
        self.apply_delta(counted_prefixes(&frame), direction);
    }

    fn apply_delta(
        &mut self,
        prefixes: Vec<IndexPrefixCardinalityKey>,
        delta: PrefixCardinalityDelta,
    ) {
        for prefix in prefixes {
            match IndexPrefixCardinalityFirstKey::from_prefix(prefix) {
                Ok(first_key) => {
                    apply_count_delta(&mut self.first_component_counts, first_key, delta);
                }
                Err(prefix) => apply_count_delta(&mut self.counts, prefix, delta),
            }
        }
    }

    fn invalidate_decoding(&mut self) {
        self.first_component_counts.clear();
        self.counts.clear();
        self.data_generation = None;
        self.decodable = false;
    }
}

fn checked_metadata_count_increment(value: u64) -> Result<u64, InternalError> {
    value
        .checked_add(1)
        .ok_or_else(InternalError::store_invariant)
}

impl IndexPrefixCardinalityDelta {
    #[must_use]
    pub(super) const fn unbound_empty() -> Self {
        Self {
            first_component_counts: HeapBTreeMap::new(),
            counts: HeapBTreeMap::new(),
            base_watermark: None,
            decodable: true,
        }
    }

    pub(super) fn reset(&mut self, base_watermark: FoldWatermark) {
        self.first_component_counts.clear();
        self.counts.clear();
        self.base_watermark = Some(base_watermark);
        self.decodable = true;
    }

    #[must_use]
    pub(super) const fn base_watermark(&self) -> Option<FoldWatermark> {
        if self.decodable {
            self.base_watermark
        } else {
            None
        }
    }

    pub(super) fn advance_watermark(&mut self, current: FoldWatermark, next: FoldWatermark) {
        if self.decodable && matches!(self.base_watermark, Some(watermark) if watermark == current)
        {
            self.base_watermark = Some(next);
        } else {
            self.base_watermark = None;
            self.decodable = false;
        }
    }

    #[must_use]
    pub(super) fn exact_delta(
        &self,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        components: &[Vec<u8>],
    ) -> Option<i64> {
        if !self.decodable {
            return None;
        }
        if let Some(first_component) = components.first().filter(|_| components.len() == 1) {
            return Some(
                self.first_component_counts
                    .get(&IndexPrefixCardinalityFirstKey::new(
                        key_kind,
                        index_id,
                        first_component,
                    ))
                    .copied()
                    .unwrap_or(0),
            );
        }
        Some(
            self.counts
                .get(&IndexPrefixCardinalityKey::new(
                    key_kind, index_id, components,
                ))
                .copied()
                .unwrap_or(0),
        )
    }

    pub(super) fn apply_transition(
        &mut self,
        raw_key: &RawIndexStoreKey,
        previous: Option<&IndexEntryValue>,
        next: Option<&IndexEntryValue>,
    ) {
        if !self.decodable || previous == next {
            return;
        }
        let Some((frame, delta)) = counted_prefix_change(raw_key, previous, next) else {
            self.invalidate();
            return;
        };
        if delta == 0 {
            return;
        }
        self.apply_prefixes(counted_prefixes(&frame), delta);
    }

    fn apply_prefixes(&mut self, prefixes: Vec<IndexPrefixCardinalityKey>, delta: i64) {
        for prefix in prefixes {
            let applied = match IndexPrefixCardinalityFirstKey::from_prefix(prefix) {
                Ok(first_key) => {
                    apply_signed_count_delta(&mut self.first_component_counts, first_key, delta)
                }
                Err(prefix) => apply_signed_count_delta(&mut self.counts, prefix, delta),
            };
            if !applied {
                self.invalidate();
                return;
            }
        }
    }

    fn invalidate(&mut self) {
        self.first_component_counts.clear();
        self.counts.clear();
        self.base_watermark = None;
        self.decodable = false;
    }
}

fn first_components<'a>(component_prefixes: &[&'a [Vec<u8>]]) -> Vec<&'a [u8]> {
    component_prefixes
        .iter()
        .filter_map(|components| components.first().map(Vec::as_slice))
        .collect()
}

#[derive(Clone, Copy)]
enum PrefixCardinalityDelta {
    Increment,
    Decrement,
}

fn apply_count_delta<K: Ord>(
    counts: &mut HeapBTreeMap<K, u64>,
    key: K,
    delta: PrefixCardinalityDelta,
) {
    match delta {
        PrefixCardinalityDelta::Increment => {
            let count = counts.entry(key).or_insert(0);
            *count = count.saturating_add(1);
        }
        PrefixCardinalityDelta::Decrement => {
            if let Some(count) = counts.get_mut(&key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    counts.remove(&key);
                }
            }
        }
    }
}

fn apply_signed_count_delta<K: Ord>(counts: &mut HeapBTreeMap<K, i64>, key: K, delta: i64) -> bool {
    match counts.entry(key) {
        Entry::Occupied(mut entry) => {
            let Some(next) = entry.get().checked_add(delta) else {
                return false;
            };
            if next == 0 {
                entry.remove();
            } else {
                *entry.get_mut() = next;
            }
        }
        Entry::Vacant(entry) => {
            if delta != 0 {
                entry.insert(delta);
            }
        }
    }
    true
}

impl IndexPrefixCardinalityFirstKey {
    fn new(key_kind: IndexKeyKind, index_id: IndexId, component: &[u8]) -> Self {
        Self {
            key_kind,
            index_id,
            component: component.to_vec(),
        }
    }

    const fn range_start(key_kind: IndexKeyKind, index_id: IndexId) -> Self {
        Self {
            key_kind,
            index_id,
            component: Vec::new(),
        }
    }

    fn matches_identity(&self, key_kind: IndexKeyKind, index_id: IndexId) -> bool {
        self.key_kind == key_kind && self.index_id == index_id
    }

    fn from_prefix(prefix: IndexPrefixCardinalityKey) -> Result<Self, IndexPrefixCardinalityKey> {
        match <[Vec<u8>; 1]>::try_from(prefix.components) {
            Ok([component]) => Ok(Self {
                key_kind: prefix.key_kind,
                index_id: prefix.index_id,
                component,
            }),
            Err(components) => Err(IndexPrefixCardinalityKey {
                key_kind: prefix.key_kind,
                index_id: prefix.index_id,
                components,
            }),
        }
    }
}

impl IndexPrefixCardinalityKey {
    fn new(key_kind: IndexKeyKind, index_id: IndexId, components: &[Vec<u8>]) -> Self {
        Self {
            key_kind,
            index_id,
            components: components.to_vec(),
        }
    }

    fn from_frame(frame: &BorrowedIndexKeyFrame<'_>, component_len: usize) -> Self {
        let components = frame
            .components()
            .iter()
            .take(component_len)
            .map(|component| component.to_vec())
            .collect();

        Self {
            key_kind: frame.key_kind(),
            index_id: frame.index_id(),
            components,
        }
    }

    fn matches_identity(&self, key_kind: IndexKeyKind, index_id: IndexId) -> bool {
        self.key_kind == key_kind && self.index_id == index_id
    }
}

// Validate the raw frame and user witnesses before deciding whether counts change.
// The signed change is the existing present/absent contribution, not stored policy.
fn counted_prefix_change<'a>(
    raw_key: &'a RawIndexStoreKey,
    previous: Option<&IndexEntryValue>,
    next: Option<&IndexEntryValue>,
) -> Option<(BorrowedIndexKeyFrame<'a>, i64)> {
    let frame = decode_index_key_frame(raw_key).ok()?;
    if frame.key_kind() != IndexKeyKind::User {
        return Some((frame, 0));
    }
    frame.primary_key_value_and_bytes().ok()?;
    let contribution = |entry: Option<&IndexEntryValue>| {
        entry.map_or(Some(0), |entry| {
            Some(i64::from(
                entry.decode_existence_witness().ok()? == IndexEntryExistenceWitness::Present,
            ))
        })
    };
    let before = contribution(previous)?;
    let after = if previous == next {
        before
    } else {
        contribution(next)?
    };
    let delta = if frame.components().is_empty() {
        0
    } else {
        after - before
    };
    Some((frame, delta))
}

fn counted_prefixes(frame: &BorrowedIndexKeyFrame<'_>) -> Vec<IndexPrefixCardinalityKey> {
    (1..=frame.components().len())
        .map(|component_len| IndexPrefixCardinalityKey::from_frame(frame, component_len))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        IndexPrefixCardinality, IndexPrefixCardinalityDelta, IndexPrefixCardinalityFirstKey,
        IndexPrefixCardinalityKey, PrefixCardinalityDelta, apply_signed_count_delta,
        checked_metadata_count_increment,
    };
    use crate::{
        MAX_INDEX_FIELDS,
        db::{
            index::{
                EncodedValue, IndexEntryValue, IndexId, IndexKey, IndexKeyKind, RawIndexStoreKey,
                UserIndexPrefixCardinalityKey,
            },
            key_taxonomy::{CompositePrimaryKeyValue, PrimaryKeyComponent, PrimaryKeyValue},
        },
        types::EntityTag,
        value::Value,
    };
    use std::{collections::BTreeMap, ops::Bound};

    #[test]
    fn prefix_transitions_preserve_counts_sync_and_overlay_contributions() {
        let composite = PrimaryKeyValue::Composite(
            CompositePrimaryKeyValue::try_from_components(&[
                PrimaryKeyComponent::Nat64(7),
                PrimaryKeyComponent::Nat64(u64::MAX),
            ])
            .unwrap(),
        );
        let present = IndexEntryValue::presence();
        let missing = IndexEntryValue::from_persisted_bytes(vec![1]);
        for kind in [IndexKeyKind::User, IndexKeyKind::System] {
            for generation in [0, 17] {
                let id = IndexId::new_with_generation(EntityTag::new(42), 7, generation);
                for primary in [PrimaryKeyComponent::Nat64(7).into(), composite] {
                    for width in 0..=MAX_INDEX_FIELDS {
                        let components = (0..width)
                            .map(|slot| {
                                EncodedValue::try_new(&Value::Text(format!("a\0{slot}")))
                                    .unwrap()
                                    .into_bytes()
                            })
                            .collect::<Vec<_>>();
                        let raw = IndexKey::new_from_components_with_primary_key_value(
                            &id,
                            kind,
                            &components,
                            &primary,
                        )
                        .unwrap()
                        .to_raw()
                        .unwrap();
                        for previous in [None, Some(&missing), Some(&present)] {
                            for next in [None, Some(&missing), Some(&present)] {
                                let before = previous == Some(&present);
                                let after = next == Some(&present);
                                let counted = kind == IndexKeyKind::User && width > 0;
                                let mut counts = IndexPrefixCardinality::synchronized_empty();
                                if let Some(value) = previous {
                                    counts.apply_insert(&raw, None, value);
                                }
                                counts.mark_synchronized(7);
                                if let Some(value) = next {
                                    counts.apply_insert(&raw, previous, value);
                                } else {
                                    counts.apply_remove(&raw, previous);
                                }
                                assert!(counts.decodable);
                                assert_eq!(
                                    counts.data_generation,
                                    (!(counted && before != after)).then_some(7)
                                );
                                counts.mark_synchronized(8);
                                let mut overlay = IndexPrefixCardinalityDelta::unbound_empty();
                                overlay.apply_transition(&raw, previous, next);
                                assert!(overlay.decodable);
                                for length in 1..=width {
                                    let expected = u64::from(kind == IndexKeyKind::User && after);
                                    assert_eq!(
                                        counts.exact_count(8, kind, id, &components[..length]),
                                        Some(expected)
                                    );
                                    let expected_delta = if counted {
                                        i64::from(after) - i64::from(before)
                                    } else {
                                        0
                                    };
                                    assert_eq!(
                                        overlay.exact_delta(kind, id, &components[..length]),
                                        Some(expected_delta)
                                    );
                                }
                                assert_eq!(
                                    counts.first_component_counts.len(),
                                    usize::from(counted && after)
                                );
                                assert_eq!(
                                    counts.counts.len(),
                                    if counted && after { width - 1 } else { 0 }
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn first_component_count_updates_transfer_owned_component_backing() {
        let id = IndexId::new(EntityTag::new(42), 7);
        for signed in [false, true] {
            let component = vec![7; 32];
            let pointer = component.as_ptr();
            let prefix = IndexPrefixCardinalityKey {
                key_kind: IndexKeyKind::User,
                index_id: id,
                components: vec![component],
            };
            if signed {
                let mut overlay = IndexPrefixCardinalityDelta::unbound_empty();
                overlay.apply_prefixes(vec![prefix], 1);
                assert_eq!(
                    overlay
                        .first_component_counts
                        .first_key_value()
                        .unwrap()
                        .0
                        .component
                        .as_ptr(),
                    pointer
                );
            } else {
                let mut counts = IndexPrefixCardinality::synchronized_empty();
                counts.apply_delta(vec![prefix], PrefixCardinalityDelta::Increment);
                assert_eq!(
                    counts
                        .first_component_counts
                        .first_key_value()
                        .unwrap()
                        .0
                        .component
                        .as_ptr(),
                    pointer
                );
            }
        }
    }

    #[test]
    fn prefix_changes_preserve_corruption_and_ignored_namespace_semantics() {
        let primary = PrimaryKeyComponent::Nat64(7).into();
        let id = IndexId::new(EntityTag::new(42), 7);
        let present = IndexEntryValue::presence();
        let missing = IndexEntryValue::from_persisted_bytes(vec![1]);
        for kind in [IndexKeyKind::User, IndexKeyKind::System] {
            let raw = IndexKey::new_from_components_with_primary_key_value(
                &id,
                kind,
                &[vec![7]],
                &primary,
            )
            .unwrap()
            .to_raw()
            .unwrap();
            let mut truncated = raw.as_bytes().to_vec();
            truncated.pop();
            let mut trailing = raw.as_bytes().to_vec();
            trailing.push(0);
            let mut bad_primary = raw.as_bytes().to_vec();
            let offset = bad_primary.len()
                - IndexKey::compact_primary_key_value_bytes(&primary)
                    .unwrap()
                    .len();
            bad_primary[offset] = 0xff;
            let user = kind == IndexKeyKind::User;
            let cases = [
                (raw.clone(), present.clone(), false),
                (
                    RawIndexStoreKey::from_persisted_bytes(truncated),
                    present.clone(),
                    true,
                ),
                (
                    RawIndexStoreKey::from_persisted_bytes(trailing),
                    present.clone(),
                    true,
                ),
                (
                    RawIndexStoreKey::from_persisted_bytes(bad_primary),
                    present.clone(),
                    user,
                ),
                (
                    raw.clone(),
                    IndexEntryValue::from_persisted_bytes(vec![]),
                    user,
                ),
                (
                    raw.clone(),
                    IndexEntryValue::from_persisted_bytes(vec![9]),
                    user,
                ),
                (
                    raw.clone(),
                    IndexEntryValue::from_persisted_bytes(vec![0, 0]),
                    user,
                ),
            ];
            for (observed_key, next, invalid) in cases {
                let mut counts = IndexPrefixCardinality::synchronized_empty();
                counts.apply_insert(&raw, None, &present);
                counts.mark_synchronized(7);
                // Even equal malformed user entries must invalidate the absolute
                // projection; equal valid entries retain its synchronization.
                counts.apply_insert(&observed_key, Some(&next), &next);
                let mut overlay = IndexPrefixCardinalityDelta::unbound_empty();
                overlay.apply_transition(&observed_key, Some(&missing), Some(&next));
                assert_eq!(counts.decodable, !invalid);
                assert_eq!(overlay.decodable, !invalid);
                if invalid {
                    assert_eq!(counts.data_generation, None);
                    assert!(counts.first_component_counts.is_empty());
                    assert!(counts.counts.is_empty());
                    assert!(overlay.first_component_counts.is_empty());
                    assert!(overlay.counts.is_empty());
                } else {
                    assert_eq!(counts.data_generation, Some(7));
                    assert_eq!(counts.first_component_counts.len(), usize::from(user));
                    assert_eq!(overlay.first_component_counts.len(), usize::from(user));
                }
            }
        }
    }

    #[test]
    fn signed_count_deltas_cancel_and_preserve_counts_on_overflow() {
        for delta in [0, 1, -1] {
            let mut counts = BTreeMap::new();
            assert!(apply_signed_count_delta(&mut counts, 7, delta));
            assert!(apply_signed_count_delta(&mut counts, 7, delta));
            assert_eq!(counts.get(&7).copied(), (delta != 0).then_some(2 * delta));
            assert!(apply_signed_count_delta(&mut counts, 7, -2 * delta));
            assert!(counts.is_empty());
        }

        for (initial, delta) in [(i64::MAX, 1), (i64::MIN, -1)] {
            let mut counts = BTreeMap::new();
            assert!(apply_signed_count_delta(&mut counts, 7, initial));
            assert!(!apply_signed_count_delta(&mut counts, 7, delta));
            assert_eq!(counts.get(&7), Some(&initial));
        }
    }

    #[test]
    fn user_prefix_lookup_key_preserves_physical_generation_and_encoded_components() {
        let entity_tag = EntityTag::new(0xCA7D);
        let components = vec![b"collection-a".to_vec(), b"Draft".to_vec()];
        let current_index = IndexId::new_with_generation(entity_tag, 2, 7);
        let next_index = IndexId::new_with_generation(entity_tag, 2, 8);

        let current = UserIndexPrefixCardinalityKey::new(current_index, components.clone());
        let next = UserIndexPrefixCardinalityKey::new(next_index, components.clone());

        assert_eq!(current.index_id(), current_index);
        assert_eq!(current.prefix_components(), components.as_slice());
        assert_ne!(current, next);
    }

    #[test]
    fn impossible_zero_multiplicity_is_a_store_invariant_failure() {
        let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
        let mut cardinality = IndexPrefixCardinality::synchronized_empty();
        cardinality.first_component_counts.insert(
            IndexPrefixCardinalityFirstKey {
                key_kind: IndexKeyKind::User,
                index_id,
                component: b"invalid-zero".to_vec(),
            },
            0,
        );

        assert!(
            cardinality
                .exact_first_component_range_count(
                    0,
                    index_id,
                    &Bound::Unbounded,
                    &Bound::Unbounded,
                    2,
                )
                .is_err(),
        );
    }

    #[test]
    fn impossible_metadata_count_overflow_is_a_store_invariant_failure() {
        assert!(checked_metadata_count_increment(u64::MAX).is_err());
    }

    #[test]
    fn first_component_range_cardinality_preserves_bounds_and_work_cap() {
        let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
        let mut cardinality = IndexPrefixCardinality::synchronized_empty();
        for (component, multiplicity) in [(b"a".as_slice(), 1), (b"b", 2), (b"c", 3)] {
            cardinality.first_component_counts.insert(
                IndexPrefixCardinalityFirstKey::new(IndexKeyKind::User, index_id, component),
                multiplicity,
            );
        }
        cardinality.mark_synchronized(7);
        let lower = Bound::Included(b"b".to_vec());
        let upper = Bound::Excluded(b"d".to_vec());

        assert_eq!(
            cardinality
                .exact_first_component_range_count(7, index_id, &lower, &upper, 3)
                .expect("synchronized range metadata should be valid"),
            Some((5, 2, true)),
        );
        assert_eq!(
            cardinality
                .exact_first_component_range_count(7, index_id, &lower, &upper, 1)
                .expect("bounded range metadata should be valid"),
            Some((2, 1, false)),
        );
        assert_eq!(
            cardinality
                .exact_first_component_range_count(8, index_id, &lower, &upper, 3)
                .expect("stale range metadata should fail closed"),
            None,
        );

        let empty_lower = Bound::Excluded(b"z".to_vec());
        assert_eq!(
            cardinality
                .exact_first_component_range_count(7, index_id, &empty_lower, &Bound::Unbounded, 1,)
                .expect("empty synchronized range should be valid"),
            Some((0, 0, true)),
        );
    }

    #[test]
    fn first_component_numeric_fold_is_exact_bounded_and_generation_matched() {
        let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
        let mut cardinality = IndexPrefixCardinality::synchronized_empty();
        for (value, multiplicity) in [
            (i64::from(i32::MIN), 1_u64),
            (-7, 2),
            (0, 3),
            (11, 4),
            (i64::from(i32::MAX), 1),
        ] {
            let component = EncodedValue::try_new(&Value::Int64(value))
                .expect("test Int64 component should encode")
                .into_bytes();
            cardinality.first_component_counts.insert(
                IndexPrefixCardinalityFirstKey::new(IndexKeyKind::User, index_id, &component),
                multiplicity,
            );
        }
        cardinality.mark_synchronized(9);

        let exact = cardinality
            .exact_first_component_numeric_fold(9, index_id, 6)
            .expect("synchronized numeric metadata should be valid")
            .expect("synchronized numeric metadata should be available");
        assert_eq!(exact, (11, 29, 5, true));

        let bounded = cardinality
            .exact_first_component_numeric_fold(9, index_id, 2)
            .expect("bounded numeric metadata should be valid")
            .expect("bounded numeric metadata should be available");
        assert_eq!(bounded, (3, -2_147_483_662, 2, false));
        assert!(
            cardinality
                .exact_first_component_numeric_fold(10, index_id, 6)
                .expect("stale numeric metadata should fail closed")
                .is_none(),
        );
    }

    #[test]
    fn first_component_numeric_fold_rejects_impossible_metadata() {
        let index_id = IndexId::new(EntityTag::new(0xCA7D), 1);
        for value in [
            Value::Text("wrong-kind".to_string()),
            Value::Int64(i64::MAX),
        ] {
            let component = EncodedValue::try_new(&value)
                .expect("test component should encode")
                .into_bytes();
            let mut cardinality = IndexPrefixCardinality::synchronized_empty();
            cardinality.first_component_counts.insert(
                IndexPrefixCardinalityFirstKey::new(IndexKeyKind::User, index_id, &component),
                1,
            );

            assert!(
                cardinality
                    .exact_first_component_numeric_fold(0, index_id, 1)
                    .is_err(),
            );
        }
    }
}
