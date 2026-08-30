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
    Canonical { visible: bool },
    Live { visible: bool },
    Both { live_is_visible: bool },
    Done,
}

/// Allocation-free iterator over the visible union of canonical and live entries.
pub(in crate::db) struct OrderedOverlay<'a, C, L, K, CanonicalKey, LiveKey>
where
    C: Iterator,
    L: Iterator,
    K: Ord,
    CanonicalKey: for<'entry> Fn(&'entry C::Item) -> &'entry K,
    LiveKey: for<'entry> Fn(&'entry L::Item) -> &'entry K,
{
    canonical_iter: Peekable<C>,
    live_iter: Peekable<L>,
    direction: Direction,
    canonical_key: CanonicalKey,
    live_key: LiveKey,
    tombstones: &'a BTreeSet<K>,
}

impl<C, L, K, CanonicalKey, LiveKey> Iterator for OrderedOverlay<'_, C, L, K, CanonicalKey, LiveKey>
where
    C: Iterator,
    L: Iterator,
    K: Ord,
    CanonicalKey: for<'entry> Fn(&'entry C::Item) -> &'entry K,
    LiveKey: for<'entry> Fn(&'entry L::Item) -> &'entry K,
{
    type Item = OrderedOverlayEntry<C::Item, L::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let step = {
                let canonical_entry = self.canonical_iter.peek();
                let live_entry = self.live_iter.peek();
                match (canonical_entry, live_entry) {
                    (None, None) => MergeStep::Done,
                    (Some(canonical_entry), None) => MergeStep::Canonical {
                        visible: !self
                            .tombstones
                            .contains((self.canonical_key)(canonical_entry)),
                    },
                    (None, Some(live_entry)) => MergeStep::Live {
                        visible: !self.tombstones.contains((self.live_key)(live_entry)),
                    },
                    (Some(canonical_entry), Some(live_entry)) => {
                        let canonical_key = (self.canonical_key)(canonical_entry);
                        let live_key = (self.live_key)(live_entry);
                        let live_is_visible = !self.tombstones.contains(live_key);
                        match (self.direction, canonical_key.cmp(live_key)) {
                            (_, Ordering::Equal) => MergeStep::Both { live_is_visible },
                            (Direction::Asc, Ordering::Less)
                            | (Direction::Desc, Ordering::Greater) => MergeStep::Canonical {
                                visible: !self.tombstones.contains(canonical_key),
                            },
                            (Direction::Asc, Ordering::Greater)
                            | (Direction::Desc, Ordering::Less) => MergeStep::Live {
                                visible: live_is_visible,
                            },
                        }
                    }
                }
            };

            match step {
                MergeStep::Canonical { visible } => {
                    let entry = self.canonical_iter.next()?;
                    if visible {
                        return Some(OrderedOverlayEntry::Canonical(entry));
                    }
                }
                MergeStep::Live { visible } => {
                    let entry = self.live_iter.next()?;
                    if visible {
                        return Some(OrderedOverlayEntry::Live(entry));
                    }
                }
                MergeStep::Both { live_is_visible } => {
                    self.canonical_iter.next()?;
                    let live_entry = self.live_iter.next()?;
                    if live_is_visible {
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
pub(in crate::db) fn ordered_overlay_entries<C, L, K, CanonicalKey, LiveKey>(
    canonical_iter: C,
    live_iter: L,
    direction: Direction,
    canonical_key: CanonicalKey,
    live_key: LiveKey,
    tombstones: &BTreeSet<K>,
) -> OrderedOverlay<'_, C, L, K, CanonicalKey, LiveKey>
where
    C: Iterator,
    L: Iterator,
    K: Ord,
    CanonicalKey: for<'entry> Fn(&'entry C::Item) -> &'entry K,
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
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    fn collect_overlay<'a>(
        entries: impl Iterator<Item = OrderedOverlayEntry<(&'a u8, &'a u16), (&'a u8, &'a u16)>>,
        stop_after: usize,
    ) -> Vec<(u8, u16)> {
        entries
            .take(stop_after)
            .map(|entry| match entry {
                OrderedOverlayEntry::Canonical((key, value))
                | OrderedOverlayEntry::Live((key, value)) => (*key, *value),
            })
            .collect()
    }

    fn visit_overlay(
        canonical: &BTreeMap<u8, u16>,
        live: &BTreeMap<u8, u16>,
        tombstones: &BTreeSet<u8>,
        direction: Direction,
        stop_after: usize,
    ) -> Vec<(u8, u16)> {
        match direction {
            Direction::Asc => collect_overlay(
                ordered_overlay_entries(
                    canonical.iter(),
                    live.iter(),
                    direction,
                    |entry| entry.0,
                    |entry| entry.0,
                    tombstones,
                ),
                stop_after,
            ),
            Direction::Desc => collect_overlay(
                ordered_overlay_entries(
                    canonical.iter().rev(),
                    live.iter().rev(),
                    direction,
                    |entry| entry.0,
                    |entry| entry.0,
                    tombstones,
                ),
                stop_after,
            ),
        }
    }

    #[test]
    fn overlay_visit_preserves_order_overrides_and_tombstones() {
        let canonical = BTreeMap::from([(1, 10), (3, 30), (5, 50)]);
        let live = BTreeMap::from([(0, 100), (3, 300), (4, 400), (5, 500)]);
        let tombstones = BTreeSet::from([1]);

        assert_eq!(
            visit_overlay(&canonical, &live, &tombstones, Direction::Asc, usize::MAX),
            vec![(0, 100), (3, 300), (4, 400), (5, 500)]
        );
        assert_eq!(
            visit_overlay(&canonical, &live, &tombstones, Direction::Desc, usize::MAX),
            vec![(5, 500), (4, 400), (3, 300), (0, 100)]
        );
    }

    #[test]
    fn overlay_visit_desc_interleaves_live_between_canonical_entries() {
        let canonical = BTreeMap::from([(1, 10), (2, 20), (3, 30)]);
        let live = BTreeMap::from([(2, 200)]);
        let tombstones = BTreeSet::new();

        assert_eq!(
            visit_overlay(&canonical, &live, &tombstones, Direction::Asc, usize::MAX),
            vec![(1, 10), (2, 200), (3, 30)]
        );
        assert_eq!(
            visit_overlay(&canonical, &live, &tombstones, Direction::Desc, usize::MAX),
            vec![(3, 30), (2, 200), (1, 10)]
        );
    }

    #[test]
    fn overlay_visit_honors_early_stop() {
        let canonical = BTreeMap::from([(1, 10), (3, 30), (5, 50)]);
        let live = BTreeMap::from([(0, 100), (4, 400)]);
        let tombstones = BTreeSet::new();

        assert_eq!(
            visit_overlay(&canonical, &live, &tombstones, Direction::Asc, 2),
            vec![(0, 100), (1, 10)]
        );
        assert_eq!(
            visit_overlay(&canonical, &live, &tombstones, Direction::Desc, 2),
            vec![(5, 50), (4, 400)]
        );
    }

    #[test]
    fn overlay_visit_handles_empty_sides() {
        let empty = BTreeMap::new();
        let canonical = BTreeMap::from([(1, 10), (3, 30)]);
        let live = BTreeMap::from([(2, 20), (4, 40)]);
        let tombstones = BTreeSet::new();

        assert_eq!(
            visit_overlay(&empty, &live, &tombstones, Direction::Asc, usize::MAX),
            vec![(2, 20), (4, 40)]
        );
        assert_eq!(
            visit_overlay(&empty, &live, &tombstones, Direction::Desc, usize::MAX),
            vec![(4, 40), (2, 20)]
        );
        assert_eq!(
            visit_overlay(&canonical, &empty, &tombstones, Direction::Asc, usize::MAX),
            vec![(1, 10), (3, 30)]
        );
        assert_eq!(
            visit_overlay(&canonical, &empty, &tombstones, Direction::Desc, usize::MAX),
            vec![(3, 30), (1, 10)]
        );
    }

    #[test]
    fn overlay_visit_suppresses_equal_key_tombstone() {
        let canonical = BTreeMap::from([(1, 10), (3, 30)]);
        let live = BTreeMap::from([(1, 100), (2, 20)]);
        let tombstones = BTreeSet::from([1]);

        assert_eq!(
            visit_overlay(&canonical, &live, &tombstones, Direction::Asc, usize::MAX),
            vec![(2, 20), (3, 30)]
        );
        assert_eq!(
            visit_overlay(&canonical, &live, &tombstones, Direction::Desc, usize::MAX),
            vec![(3, 30), (2, 20)]
        );
    }
}
