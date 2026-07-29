use crate::schema::test::TestStore;
use icydb_model::prelude::*;

///
/// ValidateTest
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "multiple_ten", value(item(is = "MultipleTenType"))),
        field(name = "lte_ten",
            value(item(prim = "Nat8", validator(path = "base::validator::num::Lte", args(10)))),
        ),
        field(name = "gt_fifty",
            value(item(prim = "Nat8", validator(path = "base::validator::num::Gt", args(50)))),
        )
    ),
    timestamps
)]
pub struct ValidateTest {}

///
/// MultipleTenType
///

#[newtype(
    primitive = "Int32",
    item(prim = "Int32"),
    ty(validator(path = "base::validator::num::MultipleOf", args(10)))
)]
pub struct MultipleTenType {}

///
/// DecimalMaxDp
///

#[newtype(
    primitive = "Decimal",
    item(prim = "Decimal", scale = 3),
    ty(validator(path = "base::validator::decimal::MaxDecimalPlaces", args(3)))
)]
pub struct DecimalMaxDp {}
