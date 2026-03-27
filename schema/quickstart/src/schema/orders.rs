use crate::schema::QuickstartStore;
use icydb::design::prelude::*;

///
/// Order
///
/// Fixture order entity used by SQL endpoint and integration harnesses.
///

#[entity(
    store = "QuickstartStore",
    pk(field = "id"),
    index(fields = "status"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(
            ident = "user_id",
            value(item(prim = "Ulid")),
            default = "Ulid::generate"
        ),
        field(ident = "status", value(item(prim = "Text"))),
        field(ident = "total_cents", value(item(prim = "Nat64")))
    )
)]
pub struct Order {}
