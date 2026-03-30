use crate::case::snake::to_snake_case;

/// Convert text into a Rust-constant-safe uppercase snake form.
#[must_use]
pub fn to_constant_case(s: &str) -> String {
    let snake = to_snake_case(s);
    let mut constant_case = snake
        .chars()
        .filter(|&c| c.is_ascii_alphanumeric() || c == '_')
        .collect::<String>()
        .to_uppercase();

    if constant_case
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_digit())
    {
        constant_case.insert(0, '_');
    }

    constant_case
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_constant() {
        let test_cases = vec![
            ("PascalCase", "PASCAL_CASE"),
            ("camelCase", "CAMEL_CASE"),
            ("a a", "A_A"),
            ("a       a", "A_A"),
            ("123_POO", "_123_POO"),
            ("CAPITALS!!", "CAPITALS"),
            ("CAPITALS", "CAPITALS"),
            ("UTF8___UTF8", "UTF8_UTF8"),
            (" the the the ", "THE_THE_THE"),
            ("MyExampleString123", "MY_EXAMPLE_STRING123"),
        ];

        for (input, expected) in test_cases {
            assert_eq!(to_constant_case(input), expected);
        }
    }
}
