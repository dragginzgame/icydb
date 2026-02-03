use crate::prelude::*;

///
/// ValidateTest
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "multiple_ten", value(item(is = "MultipleTenType"))),
        field(
            ident = "lte_ten",
            value(item(prim = "Nat8", validator(path = "base::validator::num::Lte", args(10)))),
        ),
        field(
            ident = "gt_fifty",
            value(item(prim = "Nat8", validator(path = "base::validator::num::Gt", args(50)))),
        )
    )
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
    item(prim = "Decimal"),
    ty(validator(path = "base::validator::decimal::MaxDecimalPlaces", args(3)))
)]
pub struct DecimalMaxDp {}
