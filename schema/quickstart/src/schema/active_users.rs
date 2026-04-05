use crate::schema::QuickstartStore;
use icydb::design::prelude::*;

///
/// ActiveUser
///
/// Fixture entity dedicated to filtered-index SQL and canister harness coverage.
///

#[entity(
    store = "QuickstartStore",
    pk(field = "id"),
    index(fields = "name", predicate = "active = true"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text"))),
        field(ident = "active", value(item(prim = "Bool")))
    )
)]
pub struct ActiveUser {}
