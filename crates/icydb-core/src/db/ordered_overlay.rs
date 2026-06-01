//! Module: db::ordered_overlay
//! Responsibility: ordered traversal over a canonical map plus a live overlay.
//! Does not own: store key/value types or persistence semantics.
//! Boundary: store wrappers adapt their concrete iterator entries into this merge helper.

use crate::db::direction::Direction;
use std::cmp::Ordering;

/// Control-flow result for ordered overlay traversal visitors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum OrderedOverlayVisit {
    Continue,
    Stop,
}

pub(in crate::db) enum OrderedOverlayEntry<Canonical, Live> {
    Canonical(Canonical),
    Live(Live),
}

impl OrderedOverlayVisit {
    const fn should_stop(self) -> bool {
        matches!(self, Self::Stop)
    }
}

enum MergeStep {
    Canonical,
    Live,
    Both,
    Done,
}

/// Visit the ordered union of a canonical iterator and a live overlay iterator.
///
/// Callers must pass both iterators in the requested `direction`. Equal keys
/// prefer the live entry, which matches journaled cached-stable projection
/// semantics.
pub(in crate::db) fn visit_ordered_overlay<C, L, CE, LE, E>(
    canonical_iter: C,
    live_iter: L,
    direction: Direction,
    mut compare_entries: impl FnMut(&CE, &LE) -> Ordering,
    mut canonical_is_visible: impl FnMut(&CE) -> bool,
    mut live_is_visible: impl FnMut(&LE) -> bool,
    mut visit: impl FnMut(OrderedOverlayEntry<CE, LE>) -> Result<OrderedOverlayVisit, E>,
) -> Result<(), E>
where
    C: Iterator<Item = CE>,
    L: Iterator<Item = LE>,
{
    let mut canonical_iter = canonical_iter.peekable();
    let mut live_iter = live_iter.peekable();

    loop {
        let step =
            {
                let canonical_entry = canonical_iter.peek();
                let live_entry = live_iter.peek();
                match (canonical_entry, live_entry) {
                    (None, None) => MergeStep::Done,
                    (Some(_), None) => MergeStep::Canonical,
                    (None, Some(_)) => MergeStep::Live,
                    (Some(canonical_entry), Some(live_entry)) => {
                        match (direction, compare_entries(canonical_entry, live_entry)) {
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
                let entry = canonical_iter
                    .next()
                    .expect("peeked canonical overlay entry should exist");
                if canonical_is_visible(&entry)
                    && visit(OrderedOverlayEntry::Canonical(entry))?.should_stop()
                {
                    return Ok(());
                }
            }
            MergeStep::Live => {
                let entry = live_iter
                    .next()
                    .expect("peeked live overlay entry should exist");
                if live_is_visible(&entry) && visit(OrderedOverlayEntry::Live(entry))?.should_stop()
                {
                    return Ok(());
                }
            }
            MergeStep::Both => {
                let _canonical_entry = canonical_iter
                    .next()
                    .expect("peeked canonical overlay entry should exist");
                let live_entry = live_iter
                    .next()
                    .expect("peeked live overlay entry should exist");
                if live_is_visible(&live_entry)
                    && visit(OrderedOverlayEntry::Live(live_entry))?.should_stop()
                {
                    return Ok(());
                }
            }
            MergeStep::Done => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    fn visit_overlay(
        canonical: &BTreeMap<u8, u16>,
        live: &BTreeMap<u8, u16>,
        tombstones: &BTreeSet<u8>,
        direction: Direction,
        stop_after: usize,
    ) -> Vec<(u8, u16)> {
        let mut visited = Vec::new();
        let result = match direction {
            Direction::Asc => visit_ordered_overlay(
                canonical.iter(),
                live.iter(),
                direction,
                |canonical_entry, live_entry| canonical_entry.0.cmp(live_entry.0),
                |canonical_entry| !tombstones.contains(canonical_entry.0),
                |live_entry| !tombstones.contains(live_entry.0),
                |entry| {
                    match entry {
                        OrderedOverlayEntry::Canonical((key, value))
                        | OrderedOverlayEntry::Live((key, value)) => {
                            visited.push((*key, *value));
                        }
                    }
                    Ok::<_, ()>(if visited.len() >= stop_after {
                        OrderedOverlayVisit::Stop
                    } else {
                        OrderedOverlayVisit::Continue
                    })
                },
            ),
            Direction::Desc => visit_ordered_overlay(
                canonical.iter().rev(),
                live.iter().rev(),
                direction,
                |canonical_entry, live_entry| canonical_entry.0.cmp(live_entry.0),
                |canonical_entry| !tombstones.contains(canonical_entry.0),
                |live_entry| !tombstones.contains(live_entry.0),
                |entry| {
                    match entry {
                        OrderedOverlayEntry::Canonical((key, value))
                        | OrderedOverlayEntry::Live((key, value)) => {
                            visited.push((*key, *value));
                        }
                    }
                    Ok::<_, ()>(if visited.len() >= stop_after {
                        OrderedOverlayVisit::Stop
                    } else {
                        OrderedOverlayVisit::Continue
                    })
                },
            ),
        };
        result.expect("ordered overlay traversal should succeed");
        visited
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
