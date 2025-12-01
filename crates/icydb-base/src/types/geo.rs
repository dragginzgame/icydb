use crate::prelude::*;

///
/// AddressLine
///
/// - Trim
/// - Length: 1–100
/// - Allowed: letters, digits, spaces, commas, periods, hyphens, apostrophes, TODO
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(
        sanitizer(path = "sanitizer::text::Trim"),
        validator(path = "validator::len::Range", args(1, 100)),
    )
)]
pub struct AddressLine {}

///
/// CityName
///
/// - Trim
/// - TitleCase (optional, e.g. “new york” → “New York”)
/// - Length: 1–100
/// - Allowed: letters, spaces, apostrophes, hyphens
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(
        sanitizer(path = "sanitizer::text::Trim"),
        sanitizer(path = "sanitizer::text::case::Title"),
        validator(path = "validator::len::Range", args(1, 100)),
    )
)]
pub struct CityName {}

///
/// PostalCode
///
/// - Trim whitespace
/// - Uppercase
/// - Length: 3–12 chars
/// - Allowed: letters, digits, space, dash  TODO
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(
        sanitizer(path = "sanitizer::text::Trim"),
        sanitizer(path = "sanitizer::text::case::Upper"),
        validator(path = "validator::len::Range", args(3, 12)),
    )
)]
pub struct PostalCode {}

///
/// RegionName
/// (state/province)
///
/// - Trim
/// - Uppercase
/// - Length: 2–50
/// - Allowed: letters, spaces, hyphens  TODO
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(
        sanitizer(path = "sanitizer::text::Trim"),
        sanitizer(path = "sanitizer::text::case::Upper"),
        validator(path = "validator::len::Range", args(2, 50)),
    )
)]
pub struct RegionName {}
