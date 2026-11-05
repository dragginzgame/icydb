use super::{AccessCandidateScore, index_candidate_outranks};

#[test]
fn candidate_names_only_break_identical_scores() {
    let base = AccessCandidateScore::new(1, false, false, 0, false);
    let exact = AccessCandidateScore {
        exact: true,
        ..base
    };
    let longer = AccessCandidateScore {
        prefix_len: 2,
        ..base
    };
    for exact_priority in [false, true] {
        assert!(index_candidate_outranks(
            (base, "a"),
            (base, "z"),
            exact_priority
        ));
        assert!(!index_candidate_outranks(
            (base, "z"),
            (base, "a"),
            exact_priority
        ));
        assert!(!index_candidate_outranks(
            (base, "a"),
            (base, "a"),
            exact_priority
        ));
        assert!(index_candidate_outranks(
            (longer, "z"),
            (base, "a"),
            exact_priority
        ));
        assert!(!index_candidate_outranks(
            (base, "a"),
            (longer, "z"),
            exact_priority
        ));
    }
    assert!(index_candidate_outranks((exact, "z"), (base, "a"), true));
    assert!(!index_candidate_outranks((base, "a"), (exact, "z"), true));
    // Ignoring exact-match priority does not make distinct scores equal.
    assert!(!index_candidate_outranks((exact, "a"), (base, "z"), false));
    assert!(!index_candidate_outranks((base, "a"), (exact, "z"), false));
}
