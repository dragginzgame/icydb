use icydb_model::prelude::*;

///
/// List
///

#[list(item(prim = "Text", unbounded))]
pub struct List;

///
/// Map
///

#[map(key(prim = "Text", unbounded), value(item(prim = "Nat8")))]
pub struct Map;

///
/// Record
///

#[record]
pub struct Record;

///
/// Set
///

#[set(item(prim = "Text", unbounded))]
pub struct Set;

///
/// EnumSorted
///

#[enum_(
    sorted,
    variant(name = "A"),
    variant(name = "B"),
    variant(name = "C"),
    variant(name = "D")
)]
pub struct EnumSorted {}

///
/// Negative
/// (just to check on the rust-analyzer error)
///

#[newtype(
    item(prim = "Int8"),
    ty(validator(path = "base::validator::num::Range", args(-1, 3)))
)]
pub struct Negative {}

///
/// NewtypeValidated
///

#[newtype(
    item(prim = "Decimal", scale = 18),
    ty(validator(path = "base::validator::num::Lte", args(5.0)))
)]
pub struct NewtypeValidated {}

#[cfg(test)]
mod tests {
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
}
