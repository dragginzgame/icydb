use crate::schema::test::TestStore;
use icydb::design::prelude::*;

///
/// EnumWithPayload
///

#[enum_(
    source_key = "schema/test/fixtures/src/macro_test/enum_payload.rs::enum_::nested::1",
    variant(
        source_key = "Icp",
        ident = "Icp",
        value(item(is = "base::types::ic::icp::Tokens"))
    )
)]
pub struct EnumWithPayload {}

///
/// EnumEntity
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/enum_payload.rs::entity::1",
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "cost", ident = "cost", value(item(is = "EnumWithPayload")))
    )
)]
pub struct EnumEntity {}
