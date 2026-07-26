//! Module: base::normalizer::web
//!
//! Responsibility: base normalizer definitions.
//! Does not own: validation policy, persistence, or schema mutation semantics.
//! Boundary: mutates schema field values through facade normalizer traits.

use crate::design::prelude::*;

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
mod tests {
    use super::*;

    #[test]
    fn test_mime_type_normalize_trims_and_lowercases() {
        let normalizer = MimeType;

        let mut v = "  Text/HTML  ".to_string();
        normalizer.normalize(&mut v).unwrap();
        assert_eq!(v, "text/html");

        let mut v = "APPLICATION/JSON".to_string();
        normalizer.normalize(&mut v).unwrap();
        assert_eq!(v, "application/json");

        let mut v = " image/JPEG ".to_string();
        normalizer.normalize(&mut v).unwrap();
        assert_eq!(v, "image/jpeg");
    }

    #[test]
    fn test_url_normalize_adds_https_when_missing() {
        let normalizer = Url;

        let mut v = "example.com".to_string();
        normalizer.normalize(&mut v).unwrap();
        assert_eq!(v, "https://example.com");

        let mut v = " www.example.com ".to_string();
        normalizer.normalize(&mut v).unwrap();
        assert_eq!(v, "https://www.example.com");

        let mut v = "example.com:8080/path".to_string();
        normalizer.normalize(&mut v).unwrap();
        assert_eq!(v, "https://example.com:8080/path");
    }

    #[test]
    fn test_url_normalize_keeps_existing_scheme() {
        let normalizer = Url;

        let mut v = "https://example.com".to_string();
        normalizer.normalize(&mut v).unwrap();
        assert_eq!(v, "https://example.com");

        let mut v = "http://example.com".to_string();
        normalizer.normalize(&mut v).unwrap();
        assert_eq!(v, "http://example.com");
    }

    #[test]
    fn test_url_normalize_trims_whitespace() {
        let normalizer = Url;

        let mut v = "   https://example.com   ".to_string();
        normalizer.normalize(&mut v).unwrap();
        assert_eq!(v, "https://example.com");

        let mut v = "   example.com   ".to_string();
        normalizer.normalize(&mut v).unwrap();
        assert_eq!(v, "https://example.com");
    }

    #[test]
    fn test_url_normalize_rejects_explicit_unsupported_scheme() {
        let normalizer = Url;

        let mut v = "ftp://example.com".to_string();
        assert!(normalizer.normalize(&mut v).is_err());

        let mut v = "javascript:alert(1)".to_string();
        assert!(normalizer.normalize(&mut v).is_err());
    }
}
