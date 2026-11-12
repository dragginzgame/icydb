//! Module: case::snake
//! Responsibility: local snake_case conversion helper.
//! Does not own: full case-conversion policy or external `convert_case` behavior.
//! Boundary: normalizes text into the workspace snake_case form used by codegen.

/// Convert text into snake_case.
#[must_use]
pub fn to_snake_case(s: &str) -> String {
    let mut snake_case = String::new();
    let mut chars = s.trim().chars().peekable();
    let mut prev: Option<char> = None;
    let mut index = 0;

    while let Some(ch) = chars.next() {
        let next = chars.peek().copied();

        if ch.is_uppercase() {
            let prev_is_lower_or_digit =
                prev.is_some_and(|p| p.is_lowercase() || p.is_ascii_digit());
            let next_is_lower = next.is_some_and(char::is_lowercase);

            if index != 0 && !snake_case.ends_with('_') && (prev_is_lower_or_digit || next_is_lower)
            {
                snake_case.push('_');
            }

            snake_case.extend(ch.to_lowercase());
        } else if ch == ' ' || ch == '_' {
            if !snake_case.ends_with('_') {
                snake_case.push('_');
            }
        } else if ch.is_alphanumeric() {
            snake_case.push(ch);
        }

        prev = Some(ch);
        index += 1;
    }

    snake_case.trim_matches('_').to_string()
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
