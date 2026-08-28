//! Compact in-place sorting for bounded control-plane collections.
//!
//! Rust's standard slice sorter is optimized for broad throughput. Each
//! concrete comparator can retain a sizeable quicksort, partition and
//! small-sort family in Wasm. Schema construction and reconciliation instead
//! need deterministic ordering at bounded, comparatively cold boundaries.

use std::cmp::Ordering;

/// Sort one bounded control-plane slice without retaining the standard
/// library's large per-type unstable-sort implementation.
///
/// This sorts compact source positions through one shared heapsort, then
/// applies the resulting permutation to the caller's values. It preserves the
/// predecessor's unstable-sort contract, uses `O(n)` auxiliary positions and
/// keeps `O(n log n)` worst-case comparison cost.
#[doc(hidden)]
pub fn compact_sort_unstable_by<T, F>(values: &mut [T], compare: F)
where
    F: FnMut(&T, &T) -> Ordering,
{
    let mut values = TypedValues { values, compare };
    compact_sort_erased(&mut values);
}

trait CompactValues {
    fn len(&self) -> usize;
    fn compare(&mut self, left: usize, right: usize) -> Ordering;
    fn swap(&mut self, left: usize, right: usize);
}

struct TypedValues<'a, T, F> {
    values: &'a mut [T],
    compare: F,
}

impl<T, F> CompactValues for TypedValues<'_, T, F>
where
    F: FnMut(&T, &T) -> Ordering,
{
    fn len(&self) -> usize {
        self.values.len()
    }

    fn compare(&mut self, left: usize, right: usize) -> Ordering {
        (self.compare)(&self.values[left], &self.values[right])
    }

    fn swap(&mut self, left: usize, right: usize) {
        self.values.swap(left, right);
    }
}

// Keep allocation, heap traversal and permutation concrete so every
// control-plane element and comparator type shares one Wasm implementation.
// The typed adapter exposes only indexed comparison and swap operations.
#[inline(never)]
fn compact_sort_erased(values: &mut dyn CompactValues) {
    let len = values.len();
    if len < 2 {
        return;
    }

    let mut positions = (0..len).collect::<Vec<_>>();
    sort_positions_by(&mut positions, values);

    let mut destinations = vec![0usize; len];
    for (destination, source) in positions.into_iter().enumerate() {
        destinations[source] = destination;
    }
    apply_position_permutation(&mut destinations, values);
}

#[inline(never)]
fn sort_positions_by(positions: &mut [usize], values: &mut dyn CompactValues) {
    let len = positions.len();
    let mut root = len / 2;
    while root > 0 {
        root -= 1;
        sift_down_positions(positions, root, len, values);
    }

    let mut end = len;
    while end > 1 {
        end -= 1;
        positions.swap(0, end);
        sift_down_positions(positions, 0, end, values);
    }
}

#[inline(never)]
fn sift_down_positions(
    positions: &mut [usize],
    mut root: usize,
    end: usize,
    values: &mut dyn CompactValues,
) {
    loop {
        let Some(left) = root.checked_mul(2).and_then(|index| index.checked_add(1)) else {
            return;
        };
        if left >= end {
            return;
        }

        let right = left + 1;
        let child = if right < end && values.compare(positions[left], positions[right]).is_lt() {
            right
        } else {
            left
        };
        if !values.compare(positions[root], positions[child]).is_lt() {
            return;
        }

        positions.swap(root, child);
        root = child;
    }
}

// Convert one source-indexed destination map into the final value order. The
// typed swap remains at the caller boundary while every cycle walk shares this
// one concrete implementation.
#[inline(never)]
fn apply_position_permutation(destinations: &mut [usize], values: &mut dyn CompactValues) {
    for position in 0..destinations.len() {
        while destinations[position] != position {
            let destination = destinations[position];
            values.swap(position, destination);
            destinations.swap(position, destination);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::compact_sort_unstable_by;

    #[test]
    fn compact_sort_matches_unstable_ordering_across_bounded_shapes() {
        for len in 0..=256 {
            let mut candidate = (0..len)
                .map(|index| ((index * 37 + 11) % 23) - 7)
                .collect::<Vec<_>>();
            let mut expected = candidate.clone();

            compact_sort_unstable_by(&mut candidate, i32::cmp);
            expected.sort_unstable();

            assert_eq!(candidate, expected);
        }
    }

    #[test]
    fn compact_sort_accepts_projected_comparators() {
        let mut values = [(3, "c"), (1, "a"), (2, "b"), (1, "d")];
        compact_sort_unstable_by(&mut values, |left, right| {
            left.0.cmp(&right.0).then_with(|| left.1.cmp(right.1))
        });

        assert_eq!(values, [(1, "a"), (1, "d"), (2, "b"), (3, "c")]);
    }

    #[test]
    fn compact_sort_preserves_owned_values_across_permutation_cycles() {
        let mut values = vec![
            (2, "two".to_string()),
            (0, "zero".to_string()),
            (1, "one".to_string()),
        ];

        compact_sort_unstable_by(&mut values, |left, right| left.0.cmp(&right.0));

        assert_eq!(
            values,
            [
                (0, "zero".to_string()),
                (1, "one".to_string()),
                (2, "two".to_string()),
            ]
        );
    }
}
