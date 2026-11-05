use super::*;

#[test]
fn range_admission_projection_preserves_all_bound_shapes() {
    // Admission depends on keyspace constraints, not bound payload types.
    // Qualify every open/inclusive/exclusive endpoint combination with and
    // without a composite-index equality prefix.
    let lower_bounds = [
        Bound::Unbounded,
        Bound::Included(Value::Nat64(0)),
        Bound::Excluded(Value::Nat64(0)),
    ];
    let upper_bounds = [
        Bound::Unbounded,
        Bound::Included(Value::Nat64(2)),
        Bound::Excluded(Value::Nat64(2)),
    ];
    let prefix = [Value::Nat64(1)];
    for prefix in [&[][..], &prefix[..]] {
        for lower in &lower_bounds {
            for upper in &upper_bounds {
                let facts = AdmissionAccessProjection.index_range(
                    "composite_idx",
                    ["category", "operand"].into_iter(),
                    prefix.len(),
                    prefix,
                    lower,
                    upper,
                );
                let whole_index = prefix.is_empty()
                    && matches!((lower, upper), (Bound::Unbounded, Bound::Unbounded));
                assert_eq!(
                    facts.kind,
                    if whole_index {
                        QueryAdmissionAccessKind::FullScan
                    } else {
                        QueryAdmissionAccessKind::IndexRange
                    },
                );
                assert_eq!(facts.selected_index.as_deref(), Some("composite_idx"));
                assert_eq!(facts.exact_scan_bound, None);
            }
        }
    }
}
