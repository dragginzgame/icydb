use std::collections::{BTreeMap, BTreeSet};

use super::{List, Map, Set};

#[test]
fn generated_collections_expose_native_container_methods_through_deref() {
    let list = List(vec!["one".to_string()]);
    let set = Set(BTreeSet::from(["one".to_string()]));
    let map = Map(BTreeMap::from([("one".to_string(), 1_u8)]));

    assert_eq!(list.iter().count(), 1);
    assert_eq!(set.len(), 1);
    assert!(!map.is_empty());
}
