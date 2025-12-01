use crate::prelude::*;

///
/// User FriendsList
///

#[list(
    item(rel = "crate::test::validate::Entity"),
    ty(validator(path = "base::validator::len::Max", args(2)))
)]
pub struct FriendsList {}

///
/// TESTS
///

#[cfg(test)]
mod test {
    use super::*;
    use icydb::core::validate;

    #[test]
    fn friends_list_allows_up_to_max_length() {
        let mut list = FriendsList::default();

        // Add one friend
        list.push(Ulid::generate());
        assert!(validate(&list).is_ok(), "1 friend should be valid");

        // Add second friend (at the max)
        list.push(Ulid::generate());
        assert!(validate(&list).is_ok(), "2 friends should still be valid");
    }

    #[test]
    fn friends_list_rejects_over_max_length() {
        let mut list = FriendsList::default();

        // Add three (exceeds Max(2))
        list.push(Ulid::generate());
        list.push(Ulid::generate());
        list.push(Ulid::generate());

        let result = validate(&list);
        assert!(
            result.is_err(),
            "FriendsList with more than 2 entries should fail validation"
        );

        if let Err(e) = result {
            println!("✅ expected validation error: {e:?}");
        }
    }
}
