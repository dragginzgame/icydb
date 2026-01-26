pub use crate::prelude::*;

/// --------------------
/// Primitive Newtypes
/// --------------------

#[newtype(primitive = "Account", item(prim = "Account"))]
pub struct Account {}

#[newtype(primitive = "Bool", item(prim = "Bool"))]
pub struct Bool {}

#[newtype(primitive = "Date", item(prim = "Date"))]
pub struct Date {}

#[newtype(item(prim = "Decimal"), primitive = "Decimal")]
pub struct Decimal {}

#[newtype(item(prim = "Duration"), primitive = "Duration")]
pub struct Duration {}

#[newtype(primitive = "E8s", item(prim = "E8s"))]
pub struct E8s {}

#[newtype(primitive = "E18s", item(prim = "E18s"))]
pub struct E18s {}

#[newtype(primitive = "Float32", item(prim = "Float32"))]
pub struct Float32 {}

#[newtype(primitive = "Float64", item(prim = "Float64"))]
pub struct Float64 {}

#[newtype(primitive = "Int", item(prim = "Int"))]
pub struct Int {}

#[newtype(primitive = "Int8", item(prim = "Int8"))]
pub struct Int8 {}

#[newtype(primitive = "Int16", item(prim = "Int16"))]
pub struct Int16 {}

#[newtype(primitive = "Int32", item(prim = "Int32"))]
pub struct Int32 {}

#[newtype(primitive = "Int64", item(prim = "Int64"))]
pub struct Int64 {}

#[newtype(primitive = "Int128", item(prim = "Int128"))]
pub struct Int128 {}

#[newtype(primitive = "Nat", item(prim = "Nat"))]
pub struct Nat {}

#[newtype(primitive = "Nat8", item(prim = "Nat8"))]
pub struct Nat8 {}

#[newtype(primitive = "Nat16", item(prim = "Nat16"))]
pub struct Nat16 {}

#[newtype(primitive = "Nat32", item(prim = "Nat32"))]
pub struct Nat32 {}

#[newtype(primitive = "Nat64", item(prim = "Nat64"))]
pub struct Nat64 {}

#[newtype(primitive = "Nat128", item(prim = "Nat128"))]
pub struct Nat128 {}

#[newtype(primitive = "Principal", item(prim = "Principal"))]
pub struct Principal {}

#[newtype(primitive = "Subaccount", item(prim = "Subaccount"))]
pub struct Subaccount {}

#[newtype(primitive = "Text", item(prim = "Text"))]
pub struct Text {}

#[newtype(primitive = "Timestamp", item(prim = "Timestamp"))]
pub struct Timestamp {}

#[newtype(primitive = "Ulid", item(prim = "Ulid"))]
pub struct Ulid {}

#[newtype(primitive = "Unit", item(prim = "Unit"))]
pub struct Unit {}

///
/// Wrappers
///

#[newtype(primitive = "Float32", item(is = "Float32"))]
pub struct Float32W {}

#[newtype(primitive = "Float32", item(is = "Float32W"))]
pub struct Float32WW {}

#[newtype(primitive = "Nat32", item(is = "Nat32"))]
pub struct Nat32W {}

#[newtype(primitive = "Nat32", item(is = "Nat32W"))]
pub struct Nat32WW {}

/// --------------------
/// Defaulted Newtypes
/// --------------------
///
/// These all have a default value suitable for quick initialization.
/// Each mirrors its non-default counterpart above.

#[newtype(primitive = "Account", item(prim = "Account"))]
pub struct AccountDefault {}

#[newtype(primitive = "Bool", item(prim = "Bool"), default = true)]
pub struct BoolDefault {}

#[newtype(
    primitive = "Date",
    item(prim = "Date"),
    default = "icydb::types::Date::EPOCH"
)]
pub struct DateDefault {}

#[newtype(primitive = "Decimal", item(prim = "Decimal"), default = 0.0)]
pub struct DecimalDefault {}

#[newtype(primitive = "Duration", item(prim = "Duration"), default = 0u64)]
pub struct DurationDefault {}

#[newtype(primitive = "E8s", item(prim = "E8s"), default = 0u64)]
pub struct E8sDefault {}

#[newtype(primitive = "E18s", item(prim = "E18s"), default = 0u128)]
pub struct E18sDefault {}

#[newtype(primitive = "Float32", item(prim = "Float32"), default = 0)]
pub struct Float32Default {}

#[newtype(primitive = "Float64", item(prim = "Float64"), default = 0)]
pub struct Float64Default {}

#[newtype(primitive = "Int", item(prim = "Int"), default = 0)]
pub struct IntDefault {}

#[newtype(primitive = "Int128", item(prim = "Int128"), default = 0)]
pub struct Int128Default {}

#[newtype(primitive = "Nat", item(prim = "Nat"), default = 0u64)]
pub struct NatDefault {}

#[newtype(primitive = "Nat32", item(prim = "Nat32"), default = 0u32)]
pub struct Nat32Default {}

#[newtype(primitive = "Nat64", item(prim = "Nat64"), default = 0u64)]
pub struct Nat64Default;

#[newtype(primitive = "Nat128", item(prim = "Nat128"), default = 0u128)]
pub struct Nat128Default;

#[newtype(
    primitive = "Principal",
    item(prim = "Principal"),
    default = "icydb::types::Principal::anonymous"
)]
pub struct PrincipalDefault;

#[newtype(primitive = "Subaccount", item(prim = "Subaccount"))]
pub struct SubaccountDefault;

#[newtype(primitive = "Text", item(prim = "Text"), default = "\"\"")]
pub struct TextDefault;

#[newtype(
    primitive = "Timestamp",
    item(prim = "Timestamp"),
    default = "icydb::types::Timestamp::EPOCH"
)]
pub struct TimestampDefault;

#[newtype(
    primitive = "Ulid",
    item(prim = "Ulid"),
    default = "icydb::types::Ulid::generate"
)]
pub struct UlidDefault;
