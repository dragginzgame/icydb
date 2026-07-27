use icydb_model::prelude::*;

/// --------------------
/// Primitive Newtypes
/// --------------------

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::1",
    primitive = "Account",
    item(prim = "Account")
)]
pub struct AccountN {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::2",
    primitive = "Bool",
    item(prim = "Bool")
)]
pub struct BoolN {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::3",
    primitive = "Date",
    item(prim = "Date")
)]
pub struct DateN {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::4",
    item(prim = "Decimal", scale = 18),
    primitive = "Decimal"
)]
pub struct DecimalN {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::5",
    item(prim = "Duration"),
    primitive = "Duration"
)]
pub struct DurationN {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::6",
    primitive = "Float32",
    item(prim = "Float32")
)]
pub struct Float32N {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::7",
    primitive = "Float64",
    item(prim = "Float64")
)]
pub struct Float64N {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::8",
    primitive = "IntBig",
    item(prim = "IntBig")
)]
pub struct IntN {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::9",
    primitive = "Int8",
    item(prim = "Int8")
)]
pub struct Int8N {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::10",
    primitive = "Int16",
    item(prim = "Int16")
)]
pub struct Int16N {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::11",
    primitive = "Int32",
    item(prim = "Int32")
)]
pub struct Int32N {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::12",
    primitive = "Int64",
    item(prim = "Int64")
)]
pub struct Int64N {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::13",
    primitive = "Int128",
    item(prim = "Int128")
)]
pub struct Int128N {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::14",
    primitive = "NatBig",
    item(prim = "NatBig")
)]
pub struct NatN {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::15",
    primitive = "Nat8",
    item(prim = "Nat8")
)]
pub struct Nat8N {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::16",
    primitive = "Nat16",
    item(prim = "Nat16")
)]
pub struct Nat16N {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::17",
    primitive = "Nat32",
    item(prim = "Nat32")
)]
pub struct Nat32N {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::18",
    primitive = "Nat64",
    item(prim = "Nat64")
)]
pub struct Nat64N {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::19",
    primitive = "Nat128",
    item(prim = "Nat128")
)]
pub struct Nat128N {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::20",
    primitive = "Principal",
    item(prim = "Principal")
)]
pub struct PrincipalN {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::21",
    primitive = "Subaccount",
    item(prim = "Subaccount")
)]
pub struct SubaccountN {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::22",
    primitive = "Text",
    item(prim = "Text", unbounded)
)]
pub struct TextN {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::23",
    primitive = "Timestamp",
    item(prim = "Timestamp")
)]
pub struct TimestampN {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::24",
    primitive = "Ulid",
    item(prim = "Ulid")
)]
pub struct UlidN {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::25",
    primitive = "Unit",
    item(prim = "Unit")
)]
pub struct UnitN {}

///
/// Wrapped
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::26",
    primitive = "Float32",
    item(is = "Float32N")
)]
pub struct Float32W {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::27",
    primitive = "Float32",
    item(is = "Float32W")
)]
pub struct Float32WW {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::28",
    primitive = "Nat32",
    item(is = "Nat32N")
)]
pub struct Nat32W {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::29",
    primitive = "Nat32",
    item(is = "Nat32W")
)]
pub struct Nat32WW {}

/// --------------------
/// Defaulted Newtypes
/// --------------------
///
/// These all have a default value suitable for quick initialization.
/// Each mirrors its non-default counterpart above.

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::30",
    primitive = "Account",
    item(prim = "Account")
)]
pub struct AccountD {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::31",
    primitive = "Bool",
    item(prim = "Bool"),
    default = true,
    traits(add(Default))
)]
pub struct BoolD {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::32",
    primitive = "Date",
    item(prim = "Date"),
    default = "icydb::types::Date::EPOCH",
    traits(add(Default))
)]
pub struct DateD {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::33",
    primitive = "Decimal",
    item(prim = "Decimal", scale = 18)
)]
pub struct DecimalD {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::34",
    primitive = "Duration",
    item(prim = "Duration")
)]
pub struct DurationD {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::35",
    primitive = "Float32",
    item(prim = "Float32")
)]
pub struct Float32D {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::36",
    primitive = "Float64",
    item(prim = "Float64")
)]
pub struct Float64D {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::37",
    primitive = "IntBig",
    item(prim = "IntBig")
)]
pub struct IntD {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::38",
    primitive = "Int128",
    item(prim = "Int128")
)]
pub struct Int128D {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::39",
    primitive = "NatBig",
    item(prim = "NatBig")
)]
pub struct NatD {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::40",
    primitive = "Nat32",
    item(prim = "Nat32")
)]
pub struct Nat32D {}

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::41",
    primitive = "Nat64",
    item(prim = "Nat64")
)]
pub struct Nat64D;

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::42",
    primitive = "Nat128",
    item(prim = "Nat128")
)]
pub struct Nat128D;

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::43",
    primitive = "Principal",
    item(prim = "Principal"),
    default = "icydb::types::Principal::anonymous",
    traits(add(Default))
)]
pub struct PrincipalD;

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::44",
    primitive = "Subaccount",
    item(prim = "Subaccount")
)]
pub struct SubaccountD;

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::45",
    primitive = "Text",
    item(prim = "Text", unbounded)
)]
pub struct TextD;

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::46",
    primitive = "Timestamp",
    item(prim = "Timestamp"),
    default = "icydb::types::Timestamp::EPOCH",
    traits(add(Default))
)]
pub struct TimestampD;

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/newtype.rs::newtype::47",
    primitive = "Ulid",
    item(prim = "Ulid"),
    default = "icydb::types::Ulid::nil",
    traits(add(Default))
)]
pub struct UlidD;
