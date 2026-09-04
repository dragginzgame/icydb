//! Module: index::scan::raw
//! Responsibility: pure raw index range traversal over the index store.
//! Does not own: cursor continuation, executor metrics, predicate execution, or row decoding.
//! Boundary: executor and diagnostics wrap this traversal with their runtime policies.

use crate::{
    db::direction::Direction,
    db::index::{
        IndexEntryValue, envelope_is_empty,
        key::RawIndexStoreKey,
        store::{IndexStore, IndexStoreBackend},
    },
    error::InternalError,
};
use std::{mem::size_of, ops::Bound};

struct MergedRangeHead<K, E> {
    order_key: K,
    entry: E,
}

#[derive(Clone, Copy)]
struct MergedRangeContract {
    range_count: usize,
    retained_bound_bytes: usize,
    direction: Direction,
}

fn merged_range_winner<K, E>(
    heads: &[Option<MergedRangeHead<K, E>>],
    left: Option<usize>,
    right: Option<usize>,
    direction: Direction,
) -> Result<Option<usize>, InternalError>
where
    K: Ord,
{
    match (left, right) {
        (Some(left), Some(right)) => {
            let left_head = heads
                .get(left)
                .and_then(Option::as_ref)
                .ok_or_else(InternalError::executor_invariant)?;
            let right_head = heads
                .get(right)
                .and_then(Option::as_ref)
                .ok_or_else(InternalError::executor_invariant)?;
            let ordering = left_head.order_key.cmp(&right_head.order_key);
            let left_wins = match direction {
                Direction::Asc => ordering.is_lt() || (ordering.is_eq() && left < right),
                Direction::Desc => ordering.is_gt() || (ordering.is_eq() && left < right),
            };

            Ok(Some(if left_wins { left } else { right }))
        }
        (Some(index), None) | (None, Some(index)) => Ok(Some(index)),
        (None, None) => Ok(None),
    }
}

enum MergedEntryValue<'a> {
    Borrowed(&'a IndexEntryValue),
    Owned(IndexEntryValue),
}

impl MergedEntryValue<'_> {
    const fn as_value(&self) -> &IndexEntryValue {
        match self {
            Self::Borrowed(value) => value,
            Self::Owned(value) => value,
        }
    }
}

struct MergedEntryReadRecorder {
    count: u64,
}

impl MergedEntryReadRecorder {
    const fn new() -> Self {
        Self { count: 0 }
    }

    const fn record(&mut self) {
        self.count = self.count.saturating_add(1);
    }
}

impl Drop for MergedEntryReadRecorder {
    fn drop(&mut self) {}
}

fn merged_range_structural_bytes<I, E, K>(
    contract: MergedRangeContract,
) -> Result<(usize, usize), InternalError> {
    let leaf_base = contract
        .range_count
        .checked_next_power_of_two()
        .ok_or_else(InternalError::executor_invariant)?;
    let tree_len = leaf_base
        .checked_mul(2)
        .ok_or_else(InternalError::executor_invariant)?;
    let bytes = contract
        .range_count
        .checked_mul(size_of::<I>())
        .and_then(|bytes| {
            contract
                .range_count
                .checked_mul(size_of::<Option<MergedRangeHead<K, E>>>())
                .and_then(|head_bytes| bytes.checked_add(head_bytes))
        })
        .and_then(|bytes| {
            tree_len
                .checked_mul(size_of::<Option<usize>>())
                .and_then(|tree_bytes| bytes.checked_add(tree_bytes))
        })
        .and_then(|bytes| bytes.checked_add(contract.retained_bound_bytes))
        .ok_or_else(InternalError::executor_invariant)?;

    Ok((bytes, leaf_base))
}

fn visit_merged_ranges<Ranges, I, E, K, Admit, EntryKey, EntryValue, Decode, Visit>(
    ranges: Ranges,
    contract: MergedRangeContract,
    mut admit_structural_bytes: Admit,
    entry_key: EntryKey,
    entry_value: EntryValue,
    mut decode_order_key: Decode,
    mut visit: Visit,
) -> Result<(), InternalError>
where
    Ranges: IntoIterator<Item = I>,
    I: Iterator<Item = E>,
    K: Ord,
    Admit: FnMut(usize) -> Result<(), InternalError>,
    EntryKey: for<'a> Fn(&'a E) -> &'a RawIndexStoreKey,
    EntryValue: for<'a> Fn(&'a E) -> MergedEntryValue<'a>,
    Decode: FnMut(&RawIndexStoreKey) -> Result<K, InternalError>,
    Visit: FnMut(K, &RawIndexStoreKey, &IndexEntryValue) -> Result<bool, InternalError>,
{
    let (structural_bytes, leaf_base) = merged_range_structural_bytes::<I, E, K>(contract)?;
    let tree_len = leaf_base
        .checked_mul(2)
        .ok_or_else(InternalError::executor_invariant)?;
    admit_structural_bytes(structural_bytes)?;

    let mut retained_ranges = Vec::new();
    retained_ranges
        .try_reserve_exact(contract.range_count)
        .map_err(|_| InternalError::executor_internal())?;
    for range in ranges {
        if retained_ranges.len() >= contract.range_count {
            return Err(InternalError::executor_invariant());
        }
        retained_ranges.push(range);
    }
    if retained_ranges.len() != contract.range_count {
        return Err(InternalError::executor_invariant());
    }

    let mut entry_reads = MergedEntryReadRecorder::new();
    let mut heads = Vec::new();
    heads
        .try_reserve_exact(contract.range_count)
        .map_err(|_| InternalError::executor_internal())?;
    for range in &mut retained_ranges {
        let head = if let Some(entry) = range.next() {
            entry_reads.record();
            let order_key = decode_order_key(entry_key(&entry))?;
            Some(MergedRangeHead { order_key, entry })
        } else {
            None
        };
        heads.push(head);
    }

    let mut winners = Vec::new();
    winners
        .try_reserve_exact(tree_len)
        .map_err(|_| InternalError::executor_internal())?;
    winners.resize(tree_len, None);
    for index in 0..heads.len() {
        let winner = heads.get(index).and_then(Option::as_ref).map(|_| index);
        let leaf = winners
            .get_mut(leaf_base + index)
            .ok_or_else(InternalError::executor_invariant)?;
        *leaf = winner;
    }
    for node in (1..leaf_base).rev() {
        let left = winners.get(node * 2).copied().flatten();
        let right = winners.get(node * 2 + 1).copied().flatten();
        let winner = merged_range_winner(&heads, left, right, contract.direction)?;
        let slot = winners
            .get_mut(node)
            .ok_or_else(InternalError::executor_invariant)?;
        *slot = winner;
    }

    while let Some(child) = winners.get(1).copied().flatten() {
        let head = heads
            .get_mut(child)
            .and_then(Option::take)
            .ok_or_else(InternalError::executor_invariant)?;
        let value = entry_value(&head.entry);
        if visit(head.order_key, entry_key(&head.entry), value.as_value())? {
            return Ok(());
        }
        let next_head = if let Some(entry) = retained_ranges
            .get_mut(child)
            .ok_or_else(InternalError::executor_invariant)?
            .next()
        {
            entry_reads.record();
            let order_key = decode_order_key(entry_key(&entry))?;
            Some(MergedRangeHead { order_key, entry })
        } else {
            None
        };
        let head_slot = heads
            .get_mut(child)
            .ok_or_else(InternalError::executor_invariant)?;
        *head_slot = next_head;

        let mut node = leaf_base + child;
        let leaf = winners
            .get_mut(node)
            .ok_or_else(InternalError::executor_invariant)?;
        *leaf = heads.get(child).and_then(Option::as_ref).map(|_| child);
        node /= 2;
        while node != 0 {
            let left = winners.get(node * 2).copied().flatten();
            let right = winners.get(node * 2 + 1).copied().flatten();
            let winner = merged_range_winner(&heads, left, right, contract.direction)?;
            let slot = winners
                .get_mut(node)
                .ok_or_else(InternalError::executor_invariant)?;
            *slot = winner;
            node /= 2;
        }
    }

    Ok(())
}

impl IndexStore {
    /// Merge disjoint raw index ranges by a caller-decoded logical order key.
    ///
    /// The direct route is available only while one physical backing owns the
    /// complete visible index. A mixed journal overlay returns `false` so the
    /// executor can retain its ordinary bounded child-stream path.
    #[expect(
        clippy::too_many_lines,
        reason = "the explicit backing/direction matrix keeps each iterator borrowed and allocation-free"
    )]
    pub(in crate::db) fn visit_raw_entries_in_merged_ranges<K, Admit, Decode, Visit>(
        &self,
        bounds: &[(Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>)],
        direction: Direction,
        admit_structural_bytes: Admit,
        decode_order_key: Decode,
        visit: Visit,
    ) -> Result<bool, InternalError>
    where
        K: Ord,
        Admit: FnMut(usize) -> Result<(), InternalError>,
        Decode: FnMut(&RawIndexStoreKey) -> Result<K, InternalError>,
        Visit: FnMut(K, &RawIndexStoreKey, &IndexEntryValue) -> Result<bool, InternalError>,
    {
        if bounds.is_empty() {
            return Ok(true);
        }

        let retained_range_bound_bytes =
            bounds.iter().try_fold(0usize, |bytes, (lower, upper)| {
                bytes
                    .checked_add(RawIndexStoreKey::bound_backing_bytes(lower))
                    .and_then(|bytes| {
                        bytes.checked_add(RawIndexStoreKey::bound_backing_bytes(upper))
                    })
                    .ok_or_else(InternalError::executor_invariant)
            })?;
        let contract = MergedRangeContract {
            range_count: bounds.len(),
            retained_bound_bytes: retained_range_bound_bytes,
            direction,
        };

        match (&self.backend, direction) {
            (IndexStoreBackend::Heap(map), Direction::Asc) => {
                let ranges = bounds
                    .iter()
                    .map(|(lower, upper)| map.range((lower.clone(), upper.clone())));
                visit_merged_ranges(
                    ranges,
                    contract,
                    admit_structural_bytes,
                    |entry| entry.0,
                    |entry| MergedEntryValue::Borrowed(entry.1),
                    decode_order_key,
                    visit,
                )?;
            }
            (IndexStoreBackend::Heap(map), Direction::Desc) => {
                let ranges = bounds
                    .iter()
                    .map(|(lower, upper)| map.range((lower.clone(), upper.clone())).rev());
                visit_merged_ranges(
                    ranges,
                    contract,
                    admit_structural_bytes,
                    |entry| entry.0,
                    |entry| MergedEntryValue::Borrowed(entry.1),
                    decode_order_key,
                    visit,
                )?;
            }
            (
                IndexStoreBackend::Journaled {
                    canonical,
                    live,
                    tombstones,
                    ..
                },
                Direction::Asc,
            ) if canonical.is_empty() => {
                let ranges = bounds.iter().map(|(lower, upper)| {
                    live.range((lower.clone(), upper.clone()))
                        .filter(|(key, _value)| !tombstones.contains(*key))
                });
                visit_merged_ranges(
                    ranges,
                    contract,
                    admit_structural_bytes,
                    |entry| entry.0,
                    |entry| MergedEntryValue::Borrowed(entry.1),
                    decode_order_key,
                    visit,
                )?;
            }
            (
                IndexStoreBackend::Journaled {
                    canonical,
                    live,
                    tombstones,
                    ..
                },
                Direction::Desc,
            ) if canonical.is_empty() => {
                let ranges = bounds.iter().map(|(lower, upper)| {
                    live.range((lower.clone(), upper.clone()))
                        .rev()
                        .filter(|(key, _value)| !tombstones.contains(*key))
                });
                visit_merged_ranges(
                    ranges,
                    contract,
                    admit_structural_bytes,
                    |entry| entry.0,
                    |entry| MergedEntryValue::Borrowed(entry.1),
                    decode_order_key,
                    visit,
                )?;
            }
            (
                IndexStoreBackend::Journaled {
                    canonical,
                    live,
                    tombstones,
                    ..
                },
                Direction::Asc,
            ) if live.is_empty() && tombstones.is_empty() => {
                let ranges = bounds
                    .iter()
                    .map(|(lower, upper)| canonical.range((lower.clone(), upper.clone())));
                visit_merged_ranges(
                    ranges,
                    contract,
                    admit_structural_bytes,
                    |entry| entry.key(),
                    |entry| MergedEntryValue::Owned(entry.value()),
                    decode_order_key,
                    visit,
                )?;
            }
            (
                IndexStoreBackend::Journaled {
                    canonical,
                    live,
                    tombstones,
                    ..
                },
                Direction::Desc,
            ) if live.is_empty() && tombstones.is_empty() => {
                let ranges = bounds
                    .iter()
                    .map(|(lower, upper)| canonical.range((lower.clone(), upper.clone())).rev());
                visit_merged_ranges(
                    ranges,
                    contract,
                    admit_structural_bytes,
                    |entry| entry.key(),
                    |entry| MergedEntryValue::Owned(entry.value()),
                    decode_order_key,
                    visit,
                )?;
            }
            (IndexStoreBackend::Journaled { .. }, Direction::Asc | Direction::Desc) => {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Visit raw index entries in one bounded range using canonical store order.
    ///
    /// The visitor returns `true` to stop traversal. This keeps the index layer
    /// independent of emitted-row limits, cursor anchors, predicate filters, and
    /// metric attribution while preserving the existing BTreeMap range order.
    pub(in crate::db) fn visit_raw_entries_in_range<F>(
        &self,
        bounds: (&Bound<RawIndexStoreKey>, &Bound<RawIndexStoreKey>),
        direction: Direction,
        mut visit: F,
    ) -> Result<(), InternalError>
    where
        F: FnMut(&RawIndexStoreKey, &IndexEntryValue) -> Result<bool, InternalError>,
    {
        if envelope_is_empty(bounds.0, bounds.1) {
            return Ok(());
        }

        match direction {
            Direction::Asc => match &self.backend {
                IndexStoreBackend::Heap(map) => {
                    let mut entry_reads = MergedEntryReadRecorder::new();
                    for (key, value) in map.range((bounds.0.clone(), bounds.1.clone())) {
                        entry_reads.record();
                        if visit(key, value)? {
                            return Ok(());
                        }
                    }
                }
                IndexStoreBackend::Journaled { .. } => {
                    self.visit_journaled_entries_in_range(bounds, direction, visit)?;
                }
            },
            Direction::Desc => match &self.backend {
                IndexStoreBackend::Heap(map) => {
                    let mut entry_reads = MergedEntryReadRecorder::new();
                    for (key, value) in map.range((bounds.0.clone(), bounds.1.clone())).rev() {
                        entry_reads.record();
                        if visit(key, value)? {
                            return Ok(());
                        }
                    }
                }
                IndexStoreBackend::Journaled { .. } => {
                    self.visit_journaled_entries_in_range(bounds, direction, visit)?;
                }
            },
        }

        Ok(())
    }

    /// Visit only canonical predecessor entries in one bounded raw-key range.
    ///
    /// Complete online folds use this view while reconstructing the exact
    /// derived effects of an older batch ahead of newer live overlays.
    pub(in crate::db) fn visit_canonical_raw_entries_in_range<F>(
        &self,
        bounds: (&Bound<RawIndexStoreKey>, &Bound<RawIndexStoreKey>),
        mut visit: F,
    ) -> Result<(), InternalError>
    where
        F: FnMut(&RawIndexStoreKey, &IndexEntryValue) -> Result<bool, InternalError>,
    {
        if envelope_is_empty(bounds.0, bounds.1) {
            return Ok(());
        }

        match &self.backend {
            IndexStoreBackend::Heap(map) => {
                for (key, value) in map.range((bounds.0.clone(), bounds.1.clone())) {
                    if visit(key, value)? {
                        break;
                    }
                }
            }
            IndexStoreBackend::Journaled { canonical, .. } => {
                for entry in canonical.range((bounds.0.clone(), bounds.1.clone())) {
                    if visit(entry.key(), &entry.value())? {
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}
