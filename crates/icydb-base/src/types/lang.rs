use crate::prelude::*;

///
/// Code
/// ISO 639-1 standard language codes
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(validator(path = "validator::intl::iso::Iso639_1"))
)]
pub struct Code {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
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
                validate(&code).is_ok() == expected,
                "testing: '{}' - expected: {}, got: {}",
                key,
                expected,
                validate(&code).is_ok()
            );
        }
    }
}
