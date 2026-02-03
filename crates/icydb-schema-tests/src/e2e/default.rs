use crate::prelude::*;

///
/// Record
///

#[record(fields(
    field(ident = "nat8_value", value(item(prim = "Nat8")), default = 1u8),
    field(
        ident = "nat8_static_fn",
        value(item(prim = "Nat8")),
        default = "Record::nat8_static_fn"
    )
))]
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

#[record(fields(field(
    ident = "static_fn",
    value(item(prim = "Principal")),
    default = "WithPrincipal::static_fn"
)))]
pub struct WithPrincipal {}

impl WithPrincipal {
    #[must_use]
    pub const fn static_fn() -> Principal {
        Principal::anonymous()
    }
}
