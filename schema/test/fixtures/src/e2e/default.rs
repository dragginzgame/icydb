use icydb::design::prelude::*;

///
/// Record
///

#[record(
    source_key = "schema/test/fixtures/src/e2e/default.rs::record::1",
    fields(
        field(
            source_key = "nat8_value",
            ident = "nat8_value",
            value(item(prim = "Nat8")),
            default = 1u8
        ),
        field(
            source_key = "nat8_static_fn",
            ident = "nat8_static_fn",
            value(item(prim = "Nat8")),
            default = 32u8
        )
    )
)]
pub struct Record {}

impl Record {
    #[must_use]
    pub const fn nat8_static_fn() -> u8 {
        32
    }
}

///
/// WithPrincipal
///

#[record(
    source_key = "schema/test/fixtures/src/e2e/default.rs::record::2",
    fields(field(
        source_key = "static_fn",
        ident = "static_fn",
        value(item(prim = "Principal")),
        default = "2vxsx-fae"
    ))
)]
pub struct WithPrincipal {}

impl WithPrincipal {
    #[must_use]
    pub const fn static_fn() -> Principal {
        Principal::anonymous()
    }
}
