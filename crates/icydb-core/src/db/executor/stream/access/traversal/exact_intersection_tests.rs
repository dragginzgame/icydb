use super::{
    AccessPlanStreamResolver, ExactIntersectionPreflight, MAX_ATOMIC_EXACT_INTERSECTION_CHILDREN,
};

fn preflight(child_cardinalities: &[u64]) -> ExactIntersectionPreflight {
    let mut cardinalities = [0; MAX_ATOMIC_EXACT_INTERSECTION_CHILDREN];
    cardinalities[..child_cardinalities.len()].copy_from_slice(child_cardinalities);
    ExactIntersectionPreflight {
        child_cardinalities: cardinalities,
        child_count: child_cardinalities.len(),
        total_cardinality: child_cardinalities.iter().sum(),
    }
}

#[test]
fn worst_case_cost_gate_accepts_sparse_fixtures_and_rejects_dense_ties() {
    assert!(
        AccessPlanStreamResolver::exact_intersection_probe_can_beat_single(&preflight(&[21, 20]),)
    );
    assert!(
        AccessPlanStreamResolver::exact_intersection_probe_can_beat_single(&preflight(&[
            120, 21, 20
        ]),)
    );
    assert!(
        !AccessPlanStreamResolver::exact_intersection_probe_can_beat_single(&preflight(&[20, 20]),)
    );
}

#[test]
fn overflowed_cost_authority_fails_closed() {
    assert!(
        !AccessPlanStreamResolver::exact_intersection_cost_beats_single(
            &ExactIntersectionPreflight {
                child_cardinalities: [u64::MAX, 1, 0],
                child_count: 2,
                total_cardinality: u64::MAX,
            },
            1,
        )
    );
}
