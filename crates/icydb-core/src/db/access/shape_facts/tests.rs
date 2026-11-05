use super::*;
use crate::db::access::path::SemanticIndexAccessContractInner;
use std::sync::Arc;

fn detail(name: &str) -> IndexShapeDetails {
    IndexShapeDetails::from_access_contract(
        SemanticIndexAccessContract {
            inner: Arc::new(SemanticIndexAccessContractInner {
                ordinal: 0,
                physical_generation: 1,
                name: name.into(),
                store_path: "test".into(),
                key_items: vec![SemanticIndexKeyItem::Field("field".into())],
                unique: false,
                predicate_semantics: None,
            }),
        },
        1,
    )
}

#[test]
fn child_summary_keeps_first_range_and_checks_later_reverse_flags() {
    let first = detail("first");
    let second = detail("second");
    assert_eq!(
        summarize_child_runtime_shapes(std::iter::empty()),
        (None, true)
    );
    assert_eq!(
        summarize_child_runtime_shapes(
            [
                (None, true),
                (Some(first.clone()), true),
                (Some(second), false),
            ]
            .into_iter()
        ),
        (Some(first), false)
    );
}

#[test]
fn semantic_and_executable_summaries_agree_for_empty_and_nested_trees() {
    for (plan, reverse) in [
        (AccessPlan::<u64>::Union(vec![]), true),
        (AccessPlan::Intersection(vec![]), true),
        (AccessPlan::full_scan(), true),
        (AccessPlan::by_keys(vec![1, 2]), false),
        (
            AccessPlan::Union(vec![
                AccessPlan::full_scan(),
                AccessPlan::Intersection(vec![AccessPlan::by_keys(vec![1])]),
            ]),
            false,
        ),
        (
            AccessPlan::Intersection(vec![AccessPlan::full_scan(), AccessPlan::Union(vec![])]),
            true,
        ),
    ] {
        let semantic = plan.shape_facts();
        let executable = plan.executable_contract().shape_facts();
        assert_eq!(semantic, executable);
        assert_eq!(semantic.all_paths_support_reverse_traversal(), reverse);
    }
}
