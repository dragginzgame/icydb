use crate::macro_schema::test::TestStore;
use icydb::design::prelude::*;

///
/// EnumWithPayload
///

#[enum_(
    variant(unspecified, default),
    variant(ident = "Icp", value(item(is = "base::types::ic::icp::Tokens")))
)]
pub struct EnumWithPayload {}

///
/// EnumEntity
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "cost", value(item(is = "EnumWithPayload")))
    )
)]
pub struct EnumEntity {}
