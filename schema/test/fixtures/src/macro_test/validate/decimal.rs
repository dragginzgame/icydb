use crate::schema::test::TestStore;
use icydb_model::prelude::*;

///
/// ValidateTest
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/validate/decimal.rs::entity::1",
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
        field(source_key = "multiple_ten", ident = "multiple_ten", value(item(is = "MultipleTenType"))),
        field(source_key = "lte_ten", ident = "lte_ten",
            value(item(prim = "Nat8", validator(path = "base::validator::num::Lte", args(10)))),
        ),
        field(source_key = "gt_fifty", ident = "gt_fifty",
            value(item(prim = "Nat8", validator(path = "base::validator::num::Gt", args(50)))),
        )
    )
)]
pub struct ValidateTest {}

///
/// MultipleTenType
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/decimal.rs::newtype::1",
    primitive = "Int32",
    item(prim = "Int32"),
    ty(validator(path = "base::validator::num::MultipleOf", args(10)))
)]
pub struct MultipleTenType {}

///
/// DecimalMaxDp
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/decimal.rs::newtype::2",
    primitive = "Decimal",
    item(prim = "Decimal", scale = 3),
    ty(validator(path = "base::validator::decimal::MaxDecimalPlaces", args(3)))
)]
pub struct DecimalMaxDp {}
