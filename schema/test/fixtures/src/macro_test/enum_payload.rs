use crate::schema::test::TestStore;
use icydb_model::prelude::*;

///
/// EnumWithPayload
///

#[enum_(variant(name = "Icp", value(item(is = "base::types::ic::icp::Tokens"))))]
pub struct EnumWithPayload {}

///
/// EnumEntity
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "cost", value(item(is = "EnumWithPayload")))
    ),
    timestamps
)]
pub struct EnumEntity {}
