//! Module: index::cardinality
//! Responsibility: in-memory exact cardinality metadata for decoded index prefixes.
//! Does not own: query planning, index scan execution, or row-store mutation ordering.
//! Boundary: index store maintains this opportunistic metadata; callers must prove row/index sync.

use crate::db::index::{
    IndexEntryExistenceWitness, IndexEntryValue, IndexId, IndexKey, IndexKeyKind, RawIndexStoreKey,
};
use std::collections::BTreeMap as HeapBTreeMap;

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
#[derive(Clone, Debug)]
pub(super) struct IndexPrefixCardinality {
    first_component_counts: HeapBTreeMap<IndexPrefixCardinalityFirstKey, u64>,
    counts: HeapBTreeMap<IndexPrefixCardinalityKey, u64>,
    data_generation: Option<u64>,
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
