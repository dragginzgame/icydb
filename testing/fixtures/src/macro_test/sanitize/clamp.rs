use crate::macro_schema::test::TestStore;
use icydb::design::prelude::*;

///
/// ClampEntity
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "cint32", value(item(is = "ClampInt32"))),
        field(ident = "cint32_opt", value(opt, item(is = "ClampInt32"))),
        field(ident = "cdec", value(item(is = "ClampDecimal"))),
        field(ident = "cdec_opt", value(opt, item(is = "ClampDecimal"))),
    )
)]
pub struct SanitizeTest {}

///
/// ClampList
///

#[list(item(is = "ClampDecimal"))]
pub struct ClampListDecimal {}

///
/// ClampInt32
///

#[newtype(
    primitive = "Int32",
    item(prim = "Int32"),
    ty(sanitizer(path = "base::sanitizer::num::Clamp", args(10, 20)))
)]
pub struct ClampInt32 {}

///
/// ClampDecimal
///

#[newtype(
    primitive = "Decimal",
    item(prim = "Decimal", scale = 1),
    ty(sanitizer(path = "base::sanitizer::num::Clamp", args(0.5, 5.5)))
)]
pub struct ClampDecimal {}
