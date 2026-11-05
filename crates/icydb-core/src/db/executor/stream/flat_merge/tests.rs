use super::*;

#[test]
fn flat_merge_sibling_set_preserves_pair_order() {
    let shape = FlatMergeSiblingSet::from_vec(vec!["left", "right"]);

    match shape {
        FlatMergeSiblingSet::Pair(left, right) => {
            assert_eq!(left, "left");
            assert_eq!(right, "right");
        }
        _ => panic!("two streams should produce a pair shape"),
    }
}

#[test]
fn flat_merge_sibling_set_preserves_many_order() {
    let shape = FlatMergeSiblingSet::from_vec(vec![1, 2, 3, 4]);

    match shape {
        FlatMergeSiblingSet::Many(streams) => assert_eq!(streams, vec![1, 2, 3, 4]),
        _ => panic!("three or more streams should produce a many shape"),
    }
}
