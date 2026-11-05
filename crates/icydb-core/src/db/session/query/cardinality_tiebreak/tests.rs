use super::{
    MAX_CARDINALITY_TIEBREAK_LOWERED_BYTES, MAX_CARDINALITY_TIEBREAK_PREFIX_PROBES,
    MAX_CARDINALITY_TIEBREAK_PREFIXES_PER_CANDIDATE,
    MAX_CARDINALITY_TIEBREAK_TRANSIENT_LOWERED_BYTES, admit_cardinality_candidate_shape,
    cardinality_candidate_count_is_admitted, checked_exact_prefix_entries,
    exactly_one_matching_candidate,
};

#[test]
fn exact_prefix_entry_sum_is_checked() {
    crate::db::query::preparation::with_preparation_work(|work| {
        assert_eq!(
            checked_exact_prefix_entries(&[2, 3, 5], work).unwrap(),
            Some(10)
        );
        assert_eq!(
            checked_exact_prefix_entries(&[u64::MAX, 1], work).unwrap(),
            None
        );
    });
}

#[test]
fn optional_policy_admits_exact_bounds_and_rejects_every_first_excess() {
    assert!(cardinality_candidate_count_is_admitted(64));
    assert!(!cardinality_candidate_count_is_admitted(65));
    assert_eq!(
        admit_cardinality_candidate_shape(
            MAX_CARDINALITY_TIEBREAK_PREFIX_PROBES
                - MAX_CARDINALITY_TIEBREAK_PREFIXES_PER_CANDIDATE,
            MAX_CARDINALITY_TIEBREAK_PREFIXES_PER_CANDIDATE,
            MAX_CARDINALITY_TIEBREAK_LOWERED_BYTES
                - MAX_CARDINALITY_TIEBREAK_TRANSIENT_LOWERED_BYTES,
            MAX_CARDINALITY_TIEBREAK_TRANSIENT_LOWERED_BYTES,
            MAX_CARDINALITY_TIEBREAK_TRANSIENT_LOWERED_BYTES,
        ),
        Some((
            MAX_CARDINALITY_TIEBREAK_PREFIX_PROBES,
            MAX_CARDINALITY_TIEBREAK_LOWERED_BYTES,
        )),
    );
    assert!(
        admit_cardinality_candidate_shape(0, 17, 0, 0, 0).is_none(),
        "a seventeenth candidate prefix must fall back",
    );
    assert!(
        admit_cardinality_candidate_shape(MAX_CARDINALITY_TIEBREAK_PREFIX_PROBES, 1, 0, 0, 0,)
            .is_none(),
        "the 257th proof must fall back",
    );
    assert!(
        admit_cardinality_candidate_shape(0, 1, MAX_CARDINALITY_TIEBREAK_LOWERED_BYTES, 1, 0,)
            .is_none(),
        "the first byte above the cumulative envelope must fall back",
    );
    assert!(
        admit_cardinality_candidate_shape(
            0,
            1,
            0,
            0,
            MAX_CARDINALITY_TIEBREAK_TRANSIENT_LOWERED_BYTES + 1,
        )
        .is_none(),
        "the first byte above the transient envelope must fall back",
    );
}

#[test]
fn pinned_route_selection_requires_exactly_one_match() {
    assert_eq!(
        exactly_one_matching_candidate([1, 2, 3], |candidate| *candidate == 2),
        Some(2),
    );
    assert_eq!(
        exactly_one_matching_candidate([1, 2, 3], |candidate| *candidate == 4),
        None,
    );
    assert_eq!(
        exactly_one_matching_candidate([1, 2, 2], |candidate| *candidate == 2),
        None,
    );
}
