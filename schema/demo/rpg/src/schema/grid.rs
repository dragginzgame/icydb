use crate::schema::DemoRpgStore;
use icydb::design::prelude::*;

///
/// Grid
///
/// Demo RPG map cell entity used to exercise composite primary keys on a
/// second SQL-visible table.
///

#[entity(
    store = "DemoRpgStore",
    pk(fields = ["x", "y"]),
    index(fields = ["terrain"]),
    index(fields = ["danger_level", "terrain"]),
    fields(
        field(ident = "x", value(item(prim = "Nat16"))),
        field(ident = "y", value(item(prim = "Nat16"))),
        field(ident = "terrain", value(item(prim = "Text", unbounded))),
        field(ident = "elevation", value(item(prim = "Int16"))),
        field(ident = "danger_level", value(item(prim = "Nat8"))),
        field(ident = "discovered", value(item(prim = "Bool")))
    )
)]
pub struct Grid {}
