use crate::schema::DemoRpgStore;
use icydb::design::prelude::*;

///
/// Grid
///
/// Demo RPG map cell entity used to exercise composite primary keys on a
/// second SQL-visible table.
///

#[entity(source_key = "schema/demo/rpg/src/schema/grid.rs::entity::1",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "DemoRpgStore",
    version = 1,
    pk(fields = ["x", "y"]),
    index(source_key = "index.1", fields = ["terrain"]),
    index(source_key = "index.2", fields = ["danger_level", "terrain"]),
    fields(
        field(source_key = "x", ident = "x", value(item(prim = "Nat16"))),
        field(source_key = "y", ident = "y", value(item(prim = "Nat16"))),
        field(source_key = "terrain", ident = "terrain", value(item(prim = "Text", unbounded))),
        field(source_key = "elevation", ident = "elevation", value(item(prim = "Int16"))),
        field(source_key = "danger_level", ident = "danger_level", value(item(prim = "Nat8"))),
        field(source_key = "discovered", ident = "discovered", value(item(prim = "Bool")))
    )
)]
pub struct Grid {}
