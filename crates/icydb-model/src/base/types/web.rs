//! Module: base::types::web
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::prelude::*;

///
/// MimeType
///
/// MIME type text wrapper normalized and validated by web base rules.
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(
        normalizer(path = "base::normalizer::web::MimeType"),
        validator(path = "base::validator::web::MimeType"),
    )
)]
pub struct MimeType {}

///
/// Url
///
/// URL text wrapper normalized and validated by web base rules.
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(
        normalizer(path = "base::normalizer::web::Url"),
        validator(path = "base::validator::web::Url"),
    )
)]
pub struct Url {}
