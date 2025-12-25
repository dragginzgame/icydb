use crate::design::prelude::*;

///
/// MimeType
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(
        sanitizer(path = "base::sanitizer::web::MimeType"),
        validator(path = "base::validator::web::MimeType"),
    )
)]
pub struct MimeType {}

///
/// Url
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(
        sanitizer(path = "base::sanitizer::web::Url"),
        validator(path = "base::validator::web::Url"),
    )
)]
pub struct Url {}
