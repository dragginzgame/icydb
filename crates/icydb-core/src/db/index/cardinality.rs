//! Module: index::cardinality
//! Responsibility: in-memory exact cardinality metadata for decoded index prefixes.
//! Does not own: query planning, index scan execution, or row-store mutation ordering.
//! Boundary: index store maintains this opportunistic metadata; callers must prove row/index sync.

use crate::db::index::{
    IndexEntryExistenceWitness, IndexEntryValue, IndexId, IndexKey, IndexKeyKind, RawIndexStoreKey,
    decode_canonical_index_int64_component,
};
use crate::db::journal::FoldWatermark;
use crate::error::InternalError;
use std::collections::BTreeMap as HeapBTreeMap;
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

    pub(super) fn clear_unsynchronized(&mut self) {
        self.first_component_counts.clear();
        self.counts.clear();
        self.data_generation = None;
        self.decodable = true;
    }

    pub(super) const fn mark_synchronized(&mut self, data_generation: u64) {
        if self.decodable {
            self.data_generation = Some(data_generation);
        }
    }

    #[must_use]
    #[cfg(any(test, feature = "migration"))]
    pub(super) const fn synchronized_generation(&self) -> Option<u64> {
        if self.decodable {
            self.data_generation
        } else {
            None
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

    /// Count distinct non-empty leading components for one accepted user
    /// index while retaining a bounded physical metadata-work observation.
    #[cfg(test)]
    pub(super) fn exact_first_component_distinct_count(
        &self,
        data_generation: u64,
        index_id: IndexId,
        stop_after: u64,
    ) -> Result<Option<(u64, u64)>, InternalError> {
        self.exact_first_component_range_count(
            data_generation,
            index_id,
            &Bound::Unbounded,
            &Bound::Unbounded,
            stop_after,
        )
        .map(|result| {
            result.map(|(_total, examined, complete)| {
                (if complete { examined } else { stop_after }, examined)
            })
        })
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

    #[must_use]
    pub(super) fn exact_child_prefixes_for_parent_set<'a>(
        &self,
        data_generation: u64,
        key_kind: IndexKeyKind,
        index_id: IndexId,
        parent_component_prefixes: impl IntoIterator<Item = &'a [Vec<u8>]>,
        max_children: usize,
    ) -> Option<Vec<Vec<Vec<u8>>>> {
        if !self.decodable || self.data_generation != Some(data_generation) {
            return None;
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
    ) -> Option<Vec<Vec<Vec<u8>>>> {
        let mut parents = parent_component_prefixes
            .into_iter()
            .map(<[Vec<u8>]>::to_vec)
            .collect::<Vec<_>>();
        if parents.iter().any(Vec::is_empty) {
            return None;
        }
        parents.sort_unstable();
        parents.dedup();
        let Some(parent_len) = parents.first().map(Vec::len) else {
            return Some(Vec::new());
        };
        if parents.iter().any(|parent| parent.len() != parent_len) {
            return None;
        }

        let child_len = parent_len.saturating_add(1);
        let start = IndexPrefixCardinalityKey::range_start(key_kind, index_id);
        let mut children = Vec::new();

        for (key, _count) in self.counts.range(start..) {
            if !key.matches_identity(key_kind, index_id) {
                break;
            }
            if key.components.len() != child_len {
                continue;
            }
            let parent = &key.components[..parent_len];
            if parents
                .binary_search_by(|candidate| candidate.as_slice().cmp(parent))
                .is_err()
            {
                continue;
            }
            if children.len() == max_children {
                return None;
            }
            children.push(key.components.clone());
        }

        Some(children)
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
        if !self.decodable {
            return;
        }

        let previous_prefixes = match previous {
            Some(previous) => self.counted_prefixes_or_invalidate(raw_key, previous),
            None => Some(Vec::new()),
        };
        let Some(previous_prefixes) = previous_prefixes else {
            return;
        };
        let Some(new_prefixes) = self.counted_prefixes_or_invalidate(raw_key, new) else {
            return;
        };
        if previous_prefixes == new_prefixes {
            return;
        }

        self.data_generation = None;
        self.apply_delta(previous_prefixes, PrefixCardinalityDelta::Decrement);
        self.apply_delta(new_prefixes, PrefixCardinalityDelta::Increment);
    }

    pub(super) fn apply_remove(
        &mut self,
        raw_key: &RawIndexStoreKey,
        previous: Option<&IndexEntryValue>,
    ) {
        if !self.decodable {
            return;
        }

        let Some(previous) = previous else {
            return;
        };
        let Some(prefixes) = self.counted_prefixes_or_invalidate(raw_key, previous) else {
            return;
        };
        if prefixes.is_empty() {
            return;
        }

        self.data_generation = None;
        self.apply_delta(prefixes, PrefixCardinalityDelta::Decrement);
    }

    fn counted_prefixes_or_invalidate(
        &mut self,
        raw_key: &RawIndexStoreKey,
        entry: &IndexEntryValue,
    ) -> Option<Vec<IndexPrefixCardinalityKey>> {
        let Some(prefixes) = counted_prefixes(raw_key, entry) else {
            self.invalidate_decoding();
            return None;
        };

        Some(prefixes)
    }

    fn apply_delta(
        &mut self,
        prefixes: Vec<IndexPrefixCardinalityKey>,
        delta: PrefixCardinalityDelta,
    ) {
        for prefix in prefixes {
            if let Some(first_key) = IndexPrefixCardinalityFirstKey::from_prefix(&prefix) {
                apply_count_delta(&mut self.first_component_counts, first_key, delta);
            } else {
                apply_count_delta(&mut self.counts, prefix, delta);
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
        let Some(previous_prefixes) = previous.map_or_else(
            || Some(Vec::new()),
            |value| counted_prefixes(raw_key, value),
        ) else {
            self.invalidate();
            return;
        };
        let Some(next_prefixes) = next.map_or_else(
            || Some(Vec::new()),
            |value| counted_prefixes(raw_key, value),
        ) else {
            self.invalidate();
            return;
        };
        if previous_prefixes == next_prefixes {
            return;
        }
        self.apply_prefixes(previous_prefixes, -1);
        self.apply_prefixes(next_prefixes, 1);
    }

    fn apply_prefixes(&mut self, prefixes: Vec<IndexPrefixCardinalityKey>, delta: i64) {
        for prefix in prefixes {
            let applied =
                if let Some(first_key) = IndexPrefixCardinalityFirstKey::from_prefix(&prefix) {
                    apply_signed_count_delta(&mut self.first_component_counts, first_key, delta)
                } else {
                    apply_signed_count_delta(&mut self.counts, prefix, delta)
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

    pub(super) fn clear_unavailable(&mut self) {
        self.invalidate();
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

fn apply_signed_count_delta<K: Clone + Ord>(
    counts: &mut HeapBTreeMap<K, i64>,
    key: K,
    delta: i64,
) -> bool {
    let count = counts.entry(key.clone()).or_insert(0);
    let Some(next) = count.checked_add(delta) else {
        return false;
    };
    *count = next;
    if *count == 0 {
        counts.remove(&key);
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

    fn from_prefix(prefix: &IndexPrefixCardinalityKey) -> Option<Self> {
        let component = prefix
            .components
            .first()
            .filter(|_| prefix.components.len() == 1)?;

        Some(Self {
            key_kind: prefix.key_kind,
            index_id: prefix.index_id,
            component: component.clone(),
        })
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

    const fn range_start(key_kind: IndexKeyKind, index_id: IndexId) -> Self {
        Self {
            key_kind,
            index_id,
            components: Vec::new(),
        }
    }

    fn from_index_key(index_key: &IndexKey, component_len: usize) -> Self {
        let components = (0..component_len)
            .filter_map(|slot| index_key.component(slot).map(<[u8]>::to_vec))
            .collect();

        Self {
            key_kind: index_key.key_kind(),
            index_id: *index_key.index_id(),
            components,
        }
    }

    fn matches_identity(&self, key_kind: IndexKeyKind, index_id: IndexId) -> bool {
        self.key_kind == key_kind && self.index_id == index_id
    }
}

fn counted_prefixes(
    raw_key: &RawIndexStoreKey,
    entry: &IndexEntryValue,
) -> Option<Vec<IndexPrefixCardinalityKey>> {
    let index_key = IndexKey::try_from_raw(raw_key).ok()?;
    if index_key.key_kind() != IndexKeyKind::User {
        return Some(Vec::new());
    }

    let witness = entry.decode_row_witness(raw_key).ok()?;
    if witness.existence_witness() != IndexEntryExistenceWitness::Present {
        return Some(Vec::new());
    }

    Some(
        (1..=index_key.component_count())
            .map(|component_len| {
                IndexPrefixCardinalityKey::from_index_key(&index_key, component_len)
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        IndexPrefixCardinality, IndexPrefixCardinalityFirstKey, checked_metadata_count_increment,
    };
    use crate::{
        db::index::{EncodedValue, IndexId, IndexKeyKind, UserIndexPrefixCardinalityKey},
        types::EntityTag,
        value::Value,
    };
    use std::ops::Bound;

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
                .exact_first_component_distinct_count(0, index_id, 2)
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
