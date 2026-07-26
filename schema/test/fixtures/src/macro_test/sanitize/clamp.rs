use crate::schema::test::TestStore;
use icydb::design::prelude::*;

///
/// ClampEntity
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/sanitize/clamp.rs::entity::1",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "cint32", ident = "cint32", value(item(is = "ClampInt32"))),
        field(source_key = "cint32_opt", ident = "cint32_opt", value(opt, item(is = "ClampInt32"))),
        field(source_key = "cdec", ident = "cdec", value(item(is = "ClampDecimal"))),
        field(source_key = "cdec_opt", ident = "cdec_opt", value(opt, item(is = "ClampDecimal"))),
    )
)]
pub struct SanitizeTest {}

///
/// ClampList
///

#[list(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/clamp.rs::list::1",
    item(is = "ClampDecimal")
)]
pub struct ClampListDecimal {}

///
/// ClampInt32
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/clamp.rs::newtype::1",
    primitive = "Int32",
    item(prim = "Int32"),
    ty(sanitizer(path = "base::sanitizer::num::Clamp", args(10, 20)))
)]
pub struct ClampInt32 {}

///
/// ClampDecimal
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/sanitize/clamp.rs::newtype::2",
    primitive = "Decimal",
    item(prim = "Decimal", scale = 1),
    ty(sanitizer(path = "base::sanitizer::num::Clamp", args(0.5, 5.5)))
)]
pub struct ClampDecimal {}
