use icydb_model::prelude::*;

#[newtype(
    item(prim = "Nat64"),
    traits(remove(
        From,
        Inner,
        NormalizeCustom,
        NumericValue,
        ValidateCustom
    ))
)]
pub struct ManualOverrides {}

impl From<u64> for ManualOverrides {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl icydb_model::Inner<u64> for ManualOverrides {
    fn inner(&self) -> &u64 {
        &self.0
    }

    fn into_inner(self) -> u64 {
        self.0
    }
}

impl NormalizeCustom for ManualOverrides {}

impl NumericValue for ManualOverrides {
    fn try_to_decimal(&self) -> Option<Decimal> {
        NumericValue::try_to_decimal(&self.0)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        <u64 as NumericValue>::try_from_decimal(value).map(Self)
    }
}

impl ValidateCustom for ManualOverrides {}

#[newtype(item(prim = "Nat64"))]
pub struct NumericInner {}

#[newtype(item(is = "NumericInner"), traits(add(NumericValue)))]
pub struct OptInNumeric {}

fn assert_numeric<T: NumericValue>() {}

fn main() {
    let value = ManualOverrides::from(7_u64);
    assert_eq!(*icydb_model::Inner::inner(&value), 7);
    assert_numeric::<ManualOverrides>();
    assert_numeric::<OptInNumeric>();
}
