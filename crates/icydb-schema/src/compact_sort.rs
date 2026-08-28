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
/// This is an in-place heapsort. It preserves the predecessor's unstable-sort
/// contract, uses no auxiliary allocation and keeps `O(n log n)` worst-case
/// comparison cost.
#[doc(hidden)]
pub fn compact_sort_unstable_by<T, F>(values: &mut [T], mut compare: F)
where
    F: FnMut(&T, &T) -> Ordering,
{
    let len = values.len();
    if len < 2 {
        return;
    }

    let mut root = len / 2;
    while root > 0 {
        root -= 1;
        sift_down(values, root, len, &mut compare);
    }

    let mut end = len;
    while end > 1 {
        end -= 1;
        values.swap(0, end);
        sift_down(values, 0, end, &mut compare);
    }
}

#[inline(never)]
fn sift_down<T, F>(values: &mut [T], mut root: usize, end: usize, compare: &mut F)
where
    F: FnMut(&T, &T) -> Ordering,
{
    loop {
        let Some(left) = root.checked_mul(2).and_then(|index| index.checked_add(1)) else {
            return;
        };
        if left >= end {
            return;
        }

        let right = left + 1;
        let child = if right < end && compare(&values[left], &values[right]).is_lt() {
            right
        } else {
            left
        };
        if !compare(&values[root], &values[child]).is_lt() {
            return;
        }

        values.swap(root, child);
        root = child;
    }
}

#[cfg(test)]
mod tests {
    use super::compact_sort_unstable_by;

    #[test]
    fn compact_sort_matches_unstable_ordering_across_bounded_shapes() {
        for len in 0..=256 {
            let mut candidate = (0..len)
                .map(|index| ((index * 37 + 11) % 23) as i32 - 7)
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
}
