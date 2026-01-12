use crate::design::prelude::*;

///
/// MimeType
/// Lowercases and trims whitespace.
///

#[sanitizer]
pub struct MimeType;

impl Sanitizer<String> for MimeType {
    fn sanitize(&self, value: &mut String) -> Result<(), String> {
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

#[sanitizer]
pub struct Url;

impl Sanitizer<String> for Url {
    fn sanitize(&self, value: &mut String) -> Result<(), String> {
        let trimmed = value.trim();

        let mut normalized = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            trimmed.to_owned()
        } else {
            format!("https://{trimmed}")
        };

        *value = std::mem::take(&mut normalized);

        Ok(())
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

        let mut v = "  Text/HTML  ".to_string();
        sanitizer.sanitize(&mut v).unwrap();
        assert_eq!(v, "text/html");

        let mut v = "APPLICATION/JSON".to_string();
        sanitizer.sanitize(&mut v).unwrap();
        assert_eq!(v, "application/json");

        let mut v = " image/JPEG ".to_string();
        sanitizer.sanitize(&mut v).unwrap();
        assert_eq!(v, "image/jpeg");
    }

    #[test]
    fn test_url_sanitize_adds_https_when_missing() {
        let sanitizer = Url;

        let mut v = "example.com".to_string();
        sanitizer.sanitize(&mut v).unwrap();
        assert_eq!(v, "https://example.com");

        let mut v = " www.example.com ".to_string();
        sanitizer.sanitize(&mut v).unwrap();
        assert_eq!(v, "https://www.example.com");
    }

    #[test]
    fn test_url_sanitize_keeps_existing_scheme() {
        let sanitizer = Url;

        let mut v = "https://example.com".to_string();
        sanitizer.sanitize(&mut v).unwrap();
        assert_eq!(v, "https://example.com");

        let mut v = "http://example.com".to_string();
        sanitizer.sanitize(&mut v).unwrap();
        assert_eq!(v, "http://example.com");
    }

    #[test]
    fn test_url_sanitize_trims_whitespace() {
        let sanitizer = Url;

        let mut v = "   https://example.com   ".to_string();
        sanitizer.sanitize(&mut v).unwrap();
        assert_eq!(v, "https://example.com");

        let mut v = "   example.com   ".to_string();
        sanitizer.sanitize(&mut v).unwrap();
        assert_eq!(v, "https://example.com");
    }
}
