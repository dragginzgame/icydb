use icydb::design::prelude::*;

///
/// User FriendsList
///

#[list(
    item(rel = "crate::macro_test::entity::Entity", prim = "Ulid"),
    ty(validator(path = "base::validator::len::Max", args(2)))
)]
pub struct FriendsList {}
