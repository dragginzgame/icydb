use icydb::design::prelude::*;

///
/// User FriendsList
///

#[list(
    source_key = "schema/test/fixtures/src/macro_test/validate/list.rs::list::1",
    item(rel = "crate::macro_test::entity::Entity", prim = "Ulid"),
    ty(validator(path = "base::validator::len::Max", args(2)))
)]
pub struct FriendsList {}
