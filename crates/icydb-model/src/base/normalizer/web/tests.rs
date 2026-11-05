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
