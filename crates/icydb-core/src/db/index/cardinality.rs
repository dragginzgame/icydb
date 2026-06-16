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
    counts: HeapBTreeMap<IndexPrefixCardinalityKey, u64>,
    data_generation: Option<u64>,
    decodable: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct IndexPrefixCardinalityKey {
    key_kind: IndexKeyKind,
    index_id: IndexId,
    components: Vec<Vec<u8>>,
}

impl IndexPrefixCardinality {
    #[must_use]
    pub(super) const fn synchronized_empty() -> Self {
        Self {
            counts: HeapBTreeMap::new(),
            data_generation: Some(0),
            decodable: true,
        }
    }

    pub(super) fn clear_unsynchronized(&mut self) {
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

        Some(
            self.counts
                .get(&IndexPrefixCardinalityKey::new(
                    key_kind, index_id, components,
                ))
                .copied()
                .unwrap_or(0),
        )
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
            match delta {
                PrefixCardinalityDelta::Increment => {
                    let count = self.counts.entry(prefix).or_insert(0);
                    *count = count.saturating_add(1);
                }
                PrefixCardinalityDelta::Decrement => {
                    if let Some(count) = self.counts.get_mut(&prefix) {
                        *count = count.saturating_sub(1);
                        if *count == 0 {
                            self.counts.remove(&prefix);
                        }
                    }
                }
            }
        }
    }

    fn invalidate_decoding(&mut self) {
        self.counts.clear();
        self.data_generation = None;
        self.decodable = false;
    }
}

#[derive(Clone, Copy)]
enum PrefixCardinalityDelta {
    Increment,
    Decrement,
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
