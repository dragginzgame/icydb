//! Module: base::types::geo
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::design::prelude::*;

///
/// AddressLine
///
/// - Trim
/// - Length: 1–100
/// - Allowed characters are not enforced
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(
        sanitizer(path = "base::sanitizer::text::Trim"),
        validator(path = "base::validator::len::Range", args(1, 100)),
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
    item(prim = "Text", unbounded),
    ty(
        sanitizer(path = "base::sanitizer::text::Trim"),
        sanitizer(path = "base::sanitizer::text::case::Title"),
        validator(path = "base::validator::len::Range", args(1, 100)),
    )
)]
pub struct CityName {}

///
/// PostalCode
///
/// - Trim whitespace
/// - Uppercase
/// - Length: 3–12 chars
/// - Allowed characters are not enforced
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(
        sanitizer(path = "base::sanitizer::text::Trim"),
        sanitizer(path = "base::sanitizer::text::case::Upper"),
        validator(path = "base::validator::len::Range", args(3, 12)),
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
/// - Allowed characters are not enforced
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(
        sanitizer(path = "base::sanitizer::text::Trim"),
        sanitizer(path = "base::sanitizer::text::case::Upper"),
        validator(path = "base::validator::len::Range", args(2, 50)),
    )
)]
pub struct RegionName {}
