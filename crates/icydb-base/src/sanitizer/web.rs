use crate::{core::traits::Sanitizer, prelude::*};

///
/// MimeType
/// Lowercases and trims whitespace.
///

#[sanitizer]
pub struct MimeType;

impl Sanitizer<String> for MimeType {
    fn sanitize(&self, value: String) -> String {
        value.trim().to_ascii_lowercase()
    }
}

///
/// Url
/// Trims whitespace and ensures a valid scheme (adds `https://` if missing).
///

#[sanitizer]
pub struct Url;

impl Sanitizer<String> for Url {
    fn sanitize(&self, value: String) -> String {
        let trimmed = value.trim();

        if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            trimmed.to_string()
        } else {
            format!("https://{trimmed}")
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mime_type_sanitize_trims_and_lowercases() {
        let sanitizer = MimeType;

        assert_eq!(sanitizer.sanitize("  Text/HTML  ".to_string()), "text/html");
        assert_eq!(
            sanitizer.sanitize("APPLICATION/JSON".to_string()),
            "application/json"
        );
        assert_eq!(sanitizer.sanitize(" image/JPEG ".to_string()), "image/jpeg");
    }

    #[test]
    fn test_url_sanitize_adds_https_when_missing() {
        let sanitizer = Url;

        assert_eq!(
            sanitizer.sanitize("example.com".to_string()),
            "https://example.com"
        );
        assert_eq!(
            sanitizer.sanitize(" www.example.com ".to_string()),
            "https://www.example.com"
        );
    }

    #[test]
    fn test_url_sanitize_keeps_existing_scheme() {
        let sanitizer = Url;

        assert_eq!(
            sanitizer.sanitize("https://example.com".to_string()),
            "https://example.com"
        );
        assert_eq!(
            sanitizer.sanitize("http://example.com".to_string()),
            "http://example.com"
        );
    }

    #[test]
    fn test_url_sanitize_trims_whitespace() {
        let sanitizer = Url;

        assert_eq!(
            sanitizer.sanitize("   https://example.com   ".to_string()),
            "https://example.com"
        );
        assert_eq!(
            sanitizer.sanitize("   example.com   ".to_string()),
            "https://example.com"
        );
    }
}
