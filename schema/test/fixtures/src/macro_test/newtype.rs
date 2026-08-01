use icydb_model::prelude::*;

/// --------------------
/// Primitive Newtypes
/// --------------------

#[newtype(item(prim = "Account"))]
pub struct AccountN {}

#[newtype(item(prim = "Bool"))]
pub struct BoolN {}

#[newtype(item(prim = "Date"))]
pub struct DateN {}

#[newtype(item(prim = "Decimal", scale = 18))]
pub struct DecimalN {}

#[newtype(item(prim = "Duration"))]
pub struct DurationN {}

#[newtype(item(prim = "Float32"))]
pub struct Float32N {}

#[newtype(item(prim = "Float64"))]
pub struct Float64N {}

#[newtype(item(prim = "IntBig"))]
pub struct IntN {}

#[newtype(item(prim = "Int8"))]
pub struct Int8N {}

#[newtype(item(prim = "Int16"))]
pub struct Int16N {}

#[newtype(item(prim = "Int32"))]
pub struct Int32N {}

#[newtype(item(prim = "Int64"))]
pub struct Int64N {}

#[newtype(item(prim = "Int128"))]
pub struct Int128N {}

#[newtype(item(prim = "NatBig"))]
pub struct NatN {}

#[newtype(item(prim = "Nat8"))]
pub struct Nat8N {}

#[newtype(item(prim = "Nat16"))]
pub struct Nat16N {}

#[newtype(item(prim = "Nat32"))]
pub struct Nat32N {}

#[newtype(item(prim = "Nat64"))]
pub struct Nat64N {}

#[newtype(item(prim = "Nat128"))]
pub struct Nat128N {}

#[newtype(item(prim = "Principal"))]
pub struct PrincipalN {}

#[newtype(item(prim = "Subaccount"))]
pub struct SubaccountN {}

#[newtype(item(prim = "Text", unbounded))]
pub struct TextN {}

#[newtype(item(prim = "Timestamp"))]
pub struct TimestampN {}

#[newtype(item(prim = "Ulid"))]
pub struct UlidN {}

#[newtype(item(prim = "Unit"))]
pub struct UnitN {}

///
/// Wrapped
///

#[newtype(item(is = "Float32N"))]
pub struct Float32W {}

#[newtype(item(is = "Float32W"))]
pub struct Float32WW {}

#[newtype(item(is = "Nat32N"))]
pub struct Nat32W {}

#[newtype(item(is = "Nat32W"))]
pub struct Nat32WW {}

/// --------------------
/// Defaulted Newtypes
/// --------------------
///
/// These all have a default value suitable for quick initialization.
/// Each mirrors its non-default counterpart above.

#[newtype(item(prim = "Account"))]
pub struct AccountD {}

#[newtype(item(prim = "Bool"), default = true, traits(add(Default)))]
pub struct BoolD {}

#[newtype(
    item(prim = "Date"),
    default = "icydb::types::Date::EPOCH",
    traits(add(Default))
)]
pub struct DateD {}

#[newtype(item(prim = "Decimal", scale = 18))]
pub struct DecimalD {}

#[newtype(item(prim = "Duration"))]
pub struct DurationD {}

#[newtype(item(prim = "Float32"))]
pub struct Float32D {}

#[newtype(item(prim = "Float64"))]
pub struct Float64D {}

#[newtype(item(prim = "IntBig"))]
pub struct IntD {}

#[newtype(item(prim = "Int128"))]
pub struct Int128D {}

#[newtype(item(prim = "NatBig"))]
pub struct NatD {}

#[newtype(item(prim = "Nat32"))]
pub struct Nat32D {}

#[newtype(item(prim = "Nat64"))]
pub struct Nat64D;

#[newtype(item(prim = "Nat128"))]
pub struct Nat128D;

#[newtype(
    item(prim = "Principal"),
    default = "icydb::types::Principal::anonymous",
    traits(add(Default))
)]
pub struct PrincipalD;

#[newtype(item(prim = "Subaccount"))]
pub struct SubaccountD;

#[newtype(item(prim = "Text", unbounded))]
pub struct TextD;

#[newtype(
    item(prim = "Timestamp"),
    default = "icydb::types::Timestamp::EPOCH",
    traits(add(Default))
)]
pub struct TimestampD;

#[newtype(
    item(prim = "Ulid"),
    default = "icydb::types::Ulid::nil",
    traits(add(Default))
)]
pub struct UlidD;

#[cfg(test)]
mod tests {
    use super::{Int64N, Nat64N};
    use icydb_model::Inner as _;

    #[test]
    fn generated_exact_operators_delegate_to_the_wrapped_type() {
        let mut remainder = Nat64N::from(17_u64);
        remainder %= Nat64N::from(5_u64);
        assert_eq!(*remainder.inner(), 2);
        remainder %= 2_u64;
        assert_eq!(*remainder.inner(), 0);

        let product: Nat64N = [2_u64, 3, 4].into_iter().map(Nat64N::from).product();
        assert_eq!(*product.inner(), 24);
        let inner_product: Nat64N = [2_u64, 3, 4].into_iter().product();
        assert_eq!(*inner_product.inner(), 24);

        let negative = -Int64N::from(7_i64);
        assert_eq!(*negative.inner(), -7);
    }
}
