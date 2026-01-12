use crate::{design::prelude::*, traits::Validator};

///
/// MimeType
///

#[validator]
pub struct MimeType;

impl Validator<str> for MimeType {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        let mut parts = s.split('/');

        let type_part = parts.next();
        let subtype_part = parts.next();

        // Must contain exactly one '/'
        if type_part.is_none() || subtype_part.is_none() || parts.next().is_some() {
            ctx.issue(format!("MIME type '{s}' must contain exactly one '/'"));
            return;
        }

        let is_valid_part = |part: &str| {
            !part.is_empty()
                && part
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || "+.-".contains(c))
        };

        let type_part = type_part.unwrap();
        let subtype_part = subtype_part.unwrap();

        if !is_valid_part(type_part) || !is_valid_part(subtype_part) {
            ctx.issue(format!(
                "MIME type '{s}' contains invalid characters; \
                 only alphanumeric, '+', '-', '.' allowed"
            ));
        }
    }
}

///
/// Url
///

#[validator]
pub struct Url;

impl Validator<str> for Url {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if !(s.starts_with("http://") || s.starts_with("https://")) {
            ctx.issue(format!("URL '{s}' must start with 'http://' or 'https://'"));
        }
    }
}
