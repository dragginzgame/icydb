use crate::prelude::*;

///
/// MimeType
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(
        sanitizer(path = "sanitizer::web::MimeType"),
        validator(path = "validator::web::MimeType"),
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
        sanitizer(path = "sanitizer::web::Url"),
        validator(path = "validator::web::Url"),
    )
)]
pub struct Url {}
