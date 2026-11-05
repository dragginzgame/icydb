use super::*;

#[test]
fn test_code() {
    let test_cases = [
        // Valid codes
        ("en", true),
        ("de", true),
        // Invalid codes
        ("D", false),
        ("DE", false),
        ("en-us", false),
        ("EN-US", false),
        ("EN-USSR", false),
    ];

    for (key, expected) in test_cases {
        let code = Code(key.into());
        assert!(
            crate::validate(&code).is_ok() == expected,
            "testing: '{}' - expected: {}, got: {}",
            key,
            expected,
            crate::validate(&code).is_ok()
        );
    }
}
