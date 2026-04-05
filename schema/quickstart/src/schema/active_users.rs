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
    index(fields = "tier,handle", predicate = "active = true"),
    index(
        fields = "handle",
        key_items = "LOWER(handle)",
        predicate = "active = true"
    ),
    index(
        fields = "tier,handle",
        key_items = "tier, LOWER(handle)",
        predicate = "active = true"
    ),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text"))),
        field(ident = "active", value(item(prim = "Bool"))),
        field(ident = "tier", value(item(prim = "Text"))),
        field(ident = "handle", value(item(prim = "Text")))
    )
)]
pub struct ActiveUser {}
