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
