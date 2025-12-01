use crate::prelude::*;

///
/// CountryCode
/// two-letter country codes defined in ISO 3166-1
///
/// https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(
        sanitizer(path = "sanitizer::intl::iso::Iso3166_1A2"),
        validator(path = "validator::intl::iso::Iso3166_1A2"),
    )
)]
pub struct CountryCode {}

///
/// LanguageCode
/// two letter language code
///
/// https://en.wikipedia.org/wiki/ISO_639-1
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(
        sanitizer(path = "sanitizer::intl::iso::Iso639_1"),
        validator(path = "validator::intl::iso::Iso639_1"),
    )
)]
pub struct LanguageCode {}

///
/// PhoneNumber
/// standardised international phone number
///
/// https://en.wikipedia.org/wiki/E.164
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(
        sanitizer(path = "sanitizer::intl::phone::E164PhoneNumber"),
        validator(path = "validator::intl::phone::E164PhoneNumber"),
    )
)]
pub struct PhoneNumber {}
