//! Module: base::normalizer::web
//!
//! Responsibility: base normalizer definitions.
//! Does not own: validation policy, persistence, or schema mutation semantics.
//! Boundary: mutates schema field values through facade normalizer traits.

use crate::{prelude::*, visitor::Normalizer};

///
/// MimeType
/// Lowercases and trims whitespace.
///

#[normalizer]
pub struct MimeType;

impl Normalizer<String> for MimeType {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
        let trimmed = value.trim();

        if trimmed.len() != value.len() {
            *value = trimmed.to_owned();
        }

        value.make_ascii_lowercase();

        Ok(())
    }
}

///
/// Url
/// Trims whitespace and ensures a valid scheme (adds `https://` if missing).
///

#[normalizer]
pub struct Url;

impl Normalizer<String> for Url {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
        let trimmed = value.trim();

        let normalized = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            trimmed.to_owned()
        } else if trimmed.contains("://") || has_explicit_non_numeric_colon(trimmed) {
            return Err("URL scheme must be http or https".to_string());
        } else {
            format!("https://{trimmed}")
        };

        *value = normalized;

        Ok(())
    }
}

fn has_explicit_non_numeric_colon(value: &str) -> bool {
    let boundary = value.find(['/', '?', '#']).unwrap_or(value.len());
    let head = &value[..boundary];
    let Some((_host, port_or_scheme)) = head.rsplit_once(':') else {
        return false;
    };

    port_or_scheme.is_empty() || !port_or_scheme.chars().all(|ch| ch.is_ascii_digit())
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
