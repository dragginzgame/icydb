//! Host metadata for the model owner's transparent base wrappers.
//! Describes Serde layout, not application normalization or validation rules.

use ic_wire::typescript::{GenerationError, SchemaContext, Wire};
use serde_json::Value;

use crate::base::types::{bytes, color, finance, geo, hash, ic, ident, intl, lang, num, time, web};

// The field accessor is a type witness: infer the actual generated inner type
// instead of maintaining a second primitive/representation inventory here.
fn inner_schema<Outer, Inner: Wire>(
    _: fn(&Outer) -> &Inner,
    context: &mut SchemaContext,
) -> Result<Value, GenerationError> {
    Inner::native_schema(context)
}

macro_rules! transparent_metadata {
    ($($ty:ty),* $(,)?) => {$(
        impl Wire for $ty {
            fn native_schema(context: &mut SchemaContext) -> Result<Value, GenerationError> {
                inner_schema(|value: &Self| &value.0, context)
            }
        }
    )*};
}

transparent_metadata!(
    bytes::Utf8,
    color::RgbHex,
    color::RgbaHex,
    finance::E8s,
    finance::E18s,
    finance::Usd,
    geo::AddressLine,
    geo::CityName,
    geo::PostalCode,
    geo::RegionName,
    hash::Sha256,
    ic::Memo,
    ic::icp::Tokens,
    ic::icrc1::Tokens,
    ident::Constant,
    ident::Field,
    ident::Function,
    ident::Variable,
    ident::Variant,
    intl::CountryCode,
    intl::LanguageCode,
    intl::PhoneNumber,
    lang::Code,
    num::Degrees,
    num::Percent,
    num::PercentModifier,
    time::Milliseconds,
    time::Seconds,
    time::Minutes,
    web::MimeType,
    web::Url,
);

#[cfg(test)]
mod tests;
