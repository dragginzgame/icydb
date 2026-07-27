//! Module: base::types::intl
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::prelude::*;

///
/// CountryCode
/// two-letter country codes defined in ISO 3166-1
///
/// https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2
///

#[newtype(
    source_key = "crates/icydb/src/base/types/intl/mod.rs::newtype::1",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(
        normalizer(path = "base::normalizer::intl::iso::Iso3166_1A2"),
        validator(path = "base::validator::intl::iso::Iso3166_1A2"),
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
    source_key = "crates/icydb/src/base/types/intl/mod.rs::newtype::2",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(
        normalizer(path = "base::normalizer::intl::iso::Iso639_1"),
        validator(path = "base::validator::intl::iso::Iso639_1"),
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
    source_key = "crates/icydb/src/base/types/intl/mod.rs::newtype::3",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(
        normalizer(path = "base::normalizer::intl::phone::E164PhoneNumber"),
        validator(path = "base::validator::intl::phone::E164PhoneNumber"),
    )
)]
pub struct PhoneNumber {}
