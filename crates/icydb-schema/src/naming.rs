//! Module: naming
//! Responsibility: canonical schema identifier component normalization.
//! Does not own: accepted identifier allocation, validation, or persistence.
//! Boundary: gives proposal generation and accepted-runtime identity one exact
//! index-name slug contract.

/// Normalize one entity or index-key label into its canonical index-name slug.
///
/// Non-ASCII-alphanumeric characters become separators before camel-case and
/// acronym boundaries are lowered. Proposal generation and runtime identity
/// construction must both use this function so derived index names cannot
/// drift.
#[must_use]
pub fn canonical_index_name_slug(value: &str) -> String {
    let separated = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();

    to_snake_case(separated.as_str())
}

fn to_snake_case(value: &str) -> String {
    let mut snake_case = String::new();
    let mut characters = value.trim().chars().peekable();
    let mut previous = None;
    let mut index = 0;

    while let Some(character) = characters.next() {
        let next = characters.peek().copied();

        if character.is_uppercase() {
            let previous_is_lower_or_digit = previous.is_some_and(|candidate: char| {
                candidate.is_lowercase() || candidate.is_ascii_digit()
            });
            let next_is_lower = next.is_some_and(char::is_lowercase);

            if index != 0
                && !snake_case.ends_with('_')
                && (previous_is_lower_or_digit || next_is_lower)
            {
                snake_case.push('_');
            }

            snake_case.extend(character.to_lowercase());
        } else if character == ' ' || character == '_' {
            if !snake_case.ends_with('_') {
                snake_case.push('_');
            }
        } else if character.is_alphanumeric() {
            snake_case.push(character);
        }

        previous = Some(character);
        index += 1;
    }

    snake_case.trim_matches('_').to_string()
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_index_slug_preserves_the_shared_identity_contract() {
        let cases = [
            ("User", "user"),
            ("UserAccount", "user_account"),
            ("HTTPServer", "http_server"),
            ("LOWER(email)", "lower_email"),
            ("profile.nickname", "profile_nickname"),
            ("Shape2D", "shape2_d"),
        ];

        for (input, expected) in cases {
            assert_eq!(canonical_index_name_slug(input), expected);
        }
    }
}
