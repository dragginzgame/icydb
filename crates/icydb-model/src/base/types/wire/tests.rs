//! Independent primitive byte/schema controls for every supported base wrapper.

use ic_wire::{
    native,
    typescript::{Wire, native_schema},
};
use icydb_schema::{Blob, Decimal, wire::limits};
use serde::Serialize;

use crate::base::types::{bytes, color, finance, geo, hash, ic, ident, intl, lang, num, time, web};

fn matches_primitive<W: Wire, P: Wire + Serialize>(wrapper: &W, primitive: &P) {
    assert_eq!(native_schema::<W>(), native_schema::<P>());
    assert_eq!(
        native::encode(wrapper, limits(1024)).unwrap(),
        native::encode(primitive, limits(1024)).unwrap()
    );
}

#[test]
fn text_wrappers_preserve_text_without_inventing_domain_validation() {
    // Serde does not run application validators/normalizers. Metadata must not
    // add their character limits or reinterpret them as UTF-8 byte limits.
    let text = " raw é🦀 text ".to_owned();
    macro_rules! check { ($($ty:path),* $(,)?) => {$(matches_primitive(&$ty(text.clone()), &text);)*}; }
    check!(
        color::RgbHex,
        color::RgbaHex,
        geo::AddressLine,
        geo::CityName,
        geo::PostalCode,
        geo::RegionName,
        hash::Sha256,
        ident::Constant,
        ident::Field,
        ident::Function,
        ident::Variable,
        ident::Variant,
        intl::CountryCode,
        intl::LanguageCode,
        intl::PhoneNumber,
        lang::Code,
        web::MimeType,
        web::Url
    );
}

#[test]
fn blob_numeric_and_decimal_wrappers_preserve_their_serializers() {
    let blob = Blob::from(vec![0, 128, 255]);
    matches_primitive(&bytes::Utf8(blob.clone()), &blob);
    matches_primitive(&ic::Memo(blob.clone()), &blob);
    let decimal = Decimal::new(12345, 2);
    matches_primitive(&finance::Usd(decimal), &decimal);
    matches_primitive(&finance::E8s(decimal), &decimal);
    matches_primitive(&finance::E18s(decimal), &decimal);
    matches_primitive(&ic::icp::Tokens(u64::MAX), &u64::MAX);
    matches_primitive(&ic::icrc1::Tokens(u64::MAX), &u64::MAX);
    matches_primitive(&num::Degrees(u16::MAX), &u16::MAX);
    matches_primitive(&num::Percent(u8::MAX), &u8::MAX);
    matches_primitive(&num::PercentModifier(u16::MAX), &u16::MAX);
    matches_primitive(&time::Milliseconds(u64::MAX), &u64::MAX);
    matches_primitive(&time::Seconds(u64::MAX), &u64::MAX);
    matches_primitive(&time::Minutes(u64::MAX), &u64::MAX);
}
