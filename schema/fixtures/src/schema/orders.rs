use crate::schema::SqlTestStore;
use icydb::design::prelude::*;

///
/// Order
///
/// Fixture order entity used by SQL endpoint and integration harnesses.
///

#[entity(
    store = "SqlTestStore",
    pk(field = "id"),
    index(fields = "status"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(
            ident = "user_id",
            value(item(prim = "Ulid")),
            default = "Ulid::generate"
        ),
        field(ident = "status", value(item(prim = "Text")), default = "String::new"),
        field(
            ident = "total_cents",
            value(item(prim = "Nat64")),
            default = "u64::default"
        )
    )
)]
pub struct Order {}
