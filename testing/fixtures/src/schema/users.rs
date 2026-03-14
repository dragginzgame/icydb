use crate::schema::SqlTestStore;
use icydb::design::prelude::*;

///
/// FixtureUser
///
/// Fixture user entity used by SQL endpoint and integration harnesses.
///

#[entity(
    store = "SqlTestStore",
    pk(field = "id"),
    index(fields = "name"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text")), default = "String::new"),
        field(ident = "age", value(item(prim = "Int32")), default = 0)
    )
)]
pub struct FixtureUser {}
