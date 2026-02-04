use crate::prelude::*;

///
/// List
///

#[list(
    item(rel = "crate::test::entity::Entity"),
    ty(validator(path = "base::validator::len::Max", args(2)))
)]
pub struct List {}

///
/// Set
///

#[set(
    item(prim = "Ulid"),
    ty(validator(path = "base::validator::len::Max", args(2)))
)]
pub struct Set {}

///
/// Map
///

#[map(
    key(prim = "Ulid"),
    value(item(prim = "Text")),
    ty(validator(path = "base::validator::len::Max", args(2)))
)]
pub struct Map {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::validate;

    fn entity_ref() -> Ref<crate::test::entity::Entity> {
        Ref::new(Ulid::generate())
    }

    fn ulid() -> Ulid {
        Ulid::generate()
    }

    #[test]
    fn list_allows_up_to_max_length() {
        let mut list = List::default();

        list.push(entity_ref());
        assert!(validate(&list).is_ok(), "1 item should be valid");

        list.push(entity_ref());
        assert!(validate(&list).is_ok(), "2 items should still be valid");
    }

    #[test]
    fn list_rejects_over_max_length() {
        let mut list = List::default();

        list.push(entity_ref());
        list.push(entity_ref());
        list.push(entity_ref());

        let result = validate(&list);
        assert!(
            result.is_err(),
            "list with >2 entries should fail validation"
        );
    }

    #[test]
    fn set_allows_up_to_max_length() {
        let mut set = Set::default();

        set.insert(ulid());
        assert!(validate(&set).is_ok(), "1 item should be valid");

        set.insert(ulid());
        assert!(validate(&set).is_ok(), "2 items should still be valid");
    }

    #[test]
    fn set_rejects_over_max_length() {
        let mut set = Set::default();

        set.insert(ulid());
        set.insert(ulid());
        set.insert(ulid());

        let result = validate(&set);
        assert!(
            result.is_err(),
            "set with >2 entries should fail validation"
        );
    }

    #[test]
    fn map_allows_up_to_max_length() {
        let mut map = Map::default();

        map.insert(ulid(), "one".into());
        assert!(validate(&map).is_ok(), "1 pair should be valid");

        map.insert(ulid(), "two".into());
        assert!(validate(&map).is_ok(), "2 pairs should still be valid");
    }

    #[test]
    fn map_rejects_over_max_length() {
        let mut map = Map::default();

        map.insert(ulid(), "one".into());
        map.insert(ulid(), "two".into());
        map.insert(ulid(), "three".into());

        let result = validate(&map);
        assert!(
            result.is_err(),
            "map with >2 entries should fail validation"
        );
    }
}
