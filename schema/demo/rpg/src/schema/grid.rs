use crate::schema::DemoRpgStore;
use icydb_model::prelude::*;

///
/// Grid
///
/// Demo RPG map cell entity used to exercise composite primary keys on a
/// second SQL-visible table.
///

#[entity(store = "DemoRpgStore",
    version = 1,
    pk(fields = ["x", "y"]),
    index(fields = ["terrain"]),
    index(fields = ["danger_level", "terrain"]),
    fields(
        field(name = "x", value(item(prim = "Nat16"))),
        field(name = "y", value(item(prim = "Nat16"))),
        field(name = "terrain", value(item(prim = "Text", unbounded))),
        field(name = "elevation", value(item(prim = "Int16"))),
        field(name = "danger_level", value(item(prim = "Nat8"))),
        field(name = "discovered", value(item(prim = "Bool")))
    ),
    timestamps
)]
pub struct Grid {}
