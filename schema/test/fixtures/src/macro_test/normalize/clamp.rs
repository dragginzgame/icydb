use crate::schema::test::TestStore;
use icydb_model::prelude::*;

///
/// ClampEntity
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "cint32", value(item(is = "ClampInt32"))),
        field(name = "cint32_opt", value(opt, item(is = "ClampInt32"))),
        field(name = "cdec", value(item(is = "ClampDecimal"))),
        field(name = "cdec_opt", value(opt, item(is = "ClampDecimal"))),
    ),
    timestamps
)]
pub struct NormalizeTest {}

///
/// ClampList
///

#[list(item(is = "ClampDecimal"))]
pub struct ClampListDecimal {}

///
/// ClampInt32
///

#[newtype(
    item(prim = "Int32"),
    ty(normalizer(path = "base::normalizer::num::Clamp", args(10, 20)))
)]
pub struct ClampInt32 {}

///
/// ClampDecimal
///

#[newtype(
    item(prim = "Decimal", scale = 1),
    ty(normalizer(path = "base::normalizer::num::Clamp", args(0.5, 5.5)))
)]
pub struct ClampDecimal {}
