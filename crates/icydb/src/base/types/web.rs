//! Module: base::types::web
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

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
