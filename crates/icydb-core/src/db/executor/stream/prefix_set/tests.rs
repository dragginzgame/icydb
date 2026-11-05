use super::*;

#[test]
fn prefix_set_execution_shape_preserves_empty_shape() {
    let shape: PrefixSetExecutionShape<u8> = PrefixSetExecutionShape::from_active_prefixes(
        Vec::new(),
        PrefixSetMergeSafety::OrderedMergeSafe,
    );

    assert!(matches!(shape, PrefixSetExecutionShape::Empty));
}

#[test]
fn prefix_set_execution_shape_preserves_single_shape() {
    let shape = PrefixSetExecutionShape::from_active_prefixes(
        vec![7],
        PrefixSetMergeSafety::RequiresFallback,
    );

    match shape {
        PrefixSetExecutionShape::Single(prefix) => assert_eq!(prefix, 7),
        _ => panic!("one active prefix should stay direct"),
    }
}

#[test]
fn prefix_set_execution_shape_uses_ordered_merge_when_safe() {
    let shape = PrefixSetExecutionShape::from_active_prefixes(
        vec![1, 2, 3],
        PrefixSetMergeSafety::OrderedMergeSafe,
    );

    match shape {
        PrefixSetExecutionShape::OrderedMerge(prefixes) => assert_eq!(prefixes, vec![1, 2, 3]),
        _ => panic!("safe sibling prefixes should use ordered merge"),
    }
}

#[test]
fn prefix_set_execution_shape_uses_ordered_concat_when_safe() {
    let shape = PrefixSetExecutionShape::from_active_prefixes(
        vec![1, 2, 3],
        PrefixSetMergeSafety::OrderedConcatSafe,
    );

    match shape {
        PrefixSetExecutionShape::OrderedConcat(prefixes) => {
            assert_eq!(prefixes, vec![1, 2, 3]);
        }
        _ => panic!("safe branch-ordered sibling prefixes should use ordered concat"),
    }
}

#[test]
fn prefix_set_execution_shape_requires_fallback_when_unsafe() {
    let shape = PrefixSetExecutionShape::from_active_prefixes(
        vec![1, 2, 3],
        PrefixSetMergeSafety::RequiresFallback,
    );

    assert!(matches!(shape, PrefixSetExecutionShape::Fallback));
}
