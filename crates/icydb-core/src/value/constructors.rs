//! Scalar constructors shared by the distinct public input and output roots.
//!
//! Collection construction and enum admission stay with each boundary wrapper.

// Keep identical scalar APIs together without merging input/output authority.
macro_rules! impl_public_value_scalar_constructors {
    ($direction:literal) => {
        impl_public_value_scalar_constructors!(@typed $direction;
            account(Account) => Account, "an account";
            blob(Vec<u8>) => Blob, "a blob";
            boolean(bool) => Bool, "a boolean";
            date(Date) => Date, "a date";
            decimal(Decimal) => Decimal, "a decimal";
            duration(Duration) => Duration, "a duration";
            float32(Float32) => Float32, "a finite 32-bit floating-point";
            float64(Float64) => Float64, "a finite 64-bit floating-point";
            int64(i64) => Int64, "a 64-bit signed integer";
            int128(i128) => Int128, "a 128-bit signed integer";
            principal(Principal) => Principal, "a principal";
            subaccount(Subaccount) => Subaccount, "a fixed subaccount";
            text(String) => Text, "a text";
            timestamp(Timestamp) => Timestamp, "a timestamp";
            nat64(u64) => Nat64, "a 64-bit natural";
            nat128(u128) => Nat128, "a 128-bit natural";
            ulid(Ulid) => Ulid, "a ULID";
            u256(U256) => U256, "a fixed-width unsigned integer";
        );

        #[doc = concat!("Build a null ", $direction, " value.")]
        #[must_use]
        pub const fn null() -> Self {
            Self(PublicValue::Null)
        }

        #[doc = concat!("Build a unit ", $direction, " value.")]
        #[must_use]
        pub const fn unit() -> Self {
            Self(PublicValue::Unit)
        }
    };
    (@typed $direction:literal; $( $name:ident($value_type:ty) => $variant:ident, $kind:literal; )*) => {
        $(
            #[doc = concat!("Build ", $kind, " ", $direction, " value.")]
            #[must_use]
            pub const fn $name(value: $value_type) -> Self {
                Self(PublicValue::$variant(value))
            }
        )*
    };
}
