use crate::schema::QuickstartStore;
use icydb::design::prelude::*;

///
/// User
///
/// Fixture user entity used by SQL endpoint and integration harnesses.
///

#[entity(
    store = "QuickstartStore",
    pk(field = "id"),
    index(fields = "name"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text"))),
        field(ident = "age", value(item(prim = "Int32")))
    )
)]
pub struct User {}
