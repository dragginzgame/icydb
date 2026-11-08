//! Module: db::ordered_overlay
//! Responsibility: ordered traversal over a canonical map plus a live overlay.
//! Does not own: store key/value types or persistence semantics.
//! Boundary: store wrappers adapt their concrete iterator entries into this merge helper.

use crate::db::direction::Direction;
use std::{cmp::Ordering, collections::BTreeSet, iter::Peekable};

/// One visible entry selected from an ordered canonical/live overlay.
pub(in crate::db) enum OrderedOverlayEntry<Canonical, Live> {
    Canonical(Canonical),
    Live(Live),
}

enum MergeStep {
    Canonical,
    Live,
    Both,
    Done,
}

/// Allocation-free iterator over the visible union of canonical and live entries.
pub(in crate::db) struct OrderedOverlay<'a, C, L, K, T, CanonicalKey, LiveKey>
where
    C: Iterator<Item = T>,
    L: Iterator,
    K: Ord,
    CanonicalKey: for<'entry> Fn(&'entry T) -> &'entry K,
    LiveKey: for<'entry> Fn(&'entry L::Item) -> &'entry K,
{
    canonical_iter: Peekable<C>,
    live_iter: Peekable<L>,
    direction: Direction,
    canonical_key: CanonicalKey,
    live_key: LiveKey,
    tombstones: &'a BTreeSet<K>,
}

impl<C, L, K, T, CanonicalKey, LiveKey> Iterator
    for OrderedOverlay<'_, C, L, K, T, CanonicalKey, LiveKey>
where
    C: Iterator<Item = T>,
    L: Iterator,
    K: Ord,
    CanonicalKey: for<'entry> Fn(&'entry T) -> &'entry K,
    LiveKey: for<'entry> Fn(&'entry L::Item) -> &'entry K,
{
    type Item = OrderedOverlayEntry<T, L::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let step = {
                let canonical_entry = self.canonical_iter.peek();
                let live_entry = self.live_iter.peek();
                match (canonical_entry, live_entry) {
                    (None, None) => MergeStep::Done,
                    (Some(_), None) => MergeStep::Canonical,
                    (None, Some(_)) => MergeStep::Live,
                    (Some(canonical_entry), Some(live_entry)) => {
                        let canonical_key = (self.canonical_key)(canonical_entry);
                        let live_key = (self.live_key)(live_entry);
                        match (self.direction, canonical_key.cmp(live_key)) {
                            (_, Ordering::Equal) => MergeStep::Both,
                            (Direction::Asc, Ordering::Less)
                            | (Direction::Desc, Ordering::Greater) => MergeStep::Canonical,
                            (Direction::Asc, Ordering::Greater)
                            | (Direction::Desc, Ordering::Less) => MergeStep::Live,
                        }
                    }
                }
            };

            match step {
                MergeStep::Canonical => {
                    let entry = self.canonical_iter.next()?;
                    if !self.tombstones.contains((self.canonical_key)(&entry)) {
                        return Some(OrderedOverlayEntry::Canonical(entry));
                    }
                }
                MergeStep::Live => {
                    let entry = self.live_iter.next()?;
                    if !self.tombstones.contains((self.live_key)(&entry)) {
                        return Some(OrderedOverlayEntry::Live(entry));
                    }
                }
                MergeStep::Both => {
                    // Equal keys select the live value after consuming both entries.
                    let _ = self.canonical_iter.next()?;
                    let live_entry = self.live_iter.next()?;
                    if !self.tombstones.contains((self.live_key)(&live_entry)) {
                        return Some(OrderedOverlayEntry::Live(live_entry));
                    }
                }
                MergeStep::Done => return None,
            }
        }
    }
}

/// Build an ordered canonical/live overlay iterator.
///
/// Callers must pass both iterators in `direction`. Equal keys prefer the live
/// entry, matching journaled cached-stable projection semantics. Key projection
/// stays statically dispatched, while visitor and error types remain outside
/// the merge state machine.
pub(in crate::db) fn ordered_overlay_entries<C, L, K, T, CanonicalKey, LiveKey>(
    canonical_iter: C,
    live_iter: L,
    direction: Direction,
    canonical_key: CanonicalKey,
    live_key: LiveKey,
    tombstones: &BTreeSet<K>,
) -> OrderedOverlay<'_, C, L, K, T, CanonicalKey, LiveKey>
where
    C: Iterator<Item = T>,
    L: Iterator,
    K: Ord,
    CanonicalKey: for<'entry> Fn(&'entry T) -> &'entry K,
    LiveKey: for<'entry> Fn(&'entry L::Item) -> &'entry K,
{
    OrderedOverlay {
        canonical_iter: canonical_iter.peekable(),
        live_iter: live_iter.peekable(),
        direction,
        canonical_key,
        live_key,
        tombstones,
    }
}

#[cfg(test)]
mod tests;
