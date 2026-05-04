use crate::design::prelude::*;

///
/// MimeType
///
/// MIME type text wrapper sanitized and validated by web base rules.
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(
        sanitizer(path = "base::sanitizer::web::MimeType"),
        validator(path = "base::validator::web::MimeType"),
    )
)]
pub struct MimeType {}

///
/// Url
///
/// URL text wrapper sanitized and validated by web base rules.
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(
        sanitizer(path = "base::sanitizer::web::Url"),
        validator(path = "base::validator::web::Url"),
    )
)]
pub struct Url {}
