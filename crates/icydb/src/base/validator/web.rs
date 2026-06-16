//! Module: base::validator::web
//!
//! Responsibility: base validator definitions.
//! Does not own: sanitization policy, persistence, or schema mutation semantics.
//! Boundary: reports typed visitor issues for facade schema values.

use crate::{design::prelude::*, traits::Validator};

///
/// MimeType
///
/// Validates a basic MIME type token pair in the form `type/subtype`.
/// Each token must be ASCII alphanumeric or one of `+`, `-`, `.`.
///

#[validator]
pub struct MimeType;

impl Validator<str> for MimeType {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        // Split into at most three slash segments so we can enforce exactly one '/'.
        let mut slash_segments = s.split('/');
        let type_part = slash_segments.next();
        let subtype_part = slash_segments.next();
        let extra_part = slash_segments.next();

        // Must contain exactly one '/'
        if type_part.is_none() || subtype_part.is_none() || extra_part.is_some() {
            ctx.issue("MIME type must contain exactly one slash");
            return;
        }

        // Validate token characters against the constrained ASCII subset.
        let is_valid_part = |part: &str| {
            !part.is_empty()
                && part
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || "+.-".contains(c))
        };

        let (Some(type_part), Some(subtype_part)) = (type_part, subtype_part) else {
            return;
        };

        if !is_valid_part(type_part) || !is_valid_part(subtype_part) {
            ctx.issue("MIME type contains invalid token characters");
        }
    }
}

///
/// Url
///
/// Validates that the value uses an accepted web scheme prefix.
/// This validator only checks for `http://` and `https://`.
///

#[validator]
pub struct Url;

impl Validator<str> for Url {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if !(s.starts_with("http://") || s.starts_with("https://")) {
            ctx.issue("URL must start with http:// or https://");
        }
    }
}
