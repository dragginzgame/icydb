/// Convert text into English-style title case.
#[must_use]
pub fn to_title_case(input: &str) -> String {
    let words: Vec<&str> = input.split_whitespace().collect();
    let mut output = String::new();
    let last_index = words.len().saturating_sub(1);

    for (index, word) in words.iter().enumerate() {
        if index > 0 {
            output.push(' ');
        }
        if index == 0 || index == last_index || !is_small_word(word) {
            output.push_str(capitalize_first(word).as_str());
        } else {
            output.push_str(word.to_lowercase().as_str());
        }
    }

    output
}

fn is_small_word(word: &str) -> bool {
    const SMALL_WORDS: [&str; 14] = [
        "a", "and", "as", "at", "by", "for", "in", "nor", "of", "on", "or", "the", "to", "with",
    ];

    SMALL_WORDS.contains(&word.to_lowercase().as_str())
}

// Preserve punctuation while only uppercasing the leading grapheme-ish prefix.
fn capitalize_first(word: &str) -> String {
    let mut chars = word.chars();
    if let Some(first) = chars.next() {
        first.to_uppercase().collect::<String>() + chars.as_str()
    } else {
        String::new()
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_title() {
        let test_cases = vec![
            ("Come by", "Come By"),
            ("test me", "Test Me"),
            ("Test Me", "Test Me"),
            ("Group Of Green Sacks", "Group of Green Sacks"),
            ("Spaces ", "Spaces"),
            ("Spaces   ", "Spaces"),
            ("   Spaces", "Spaces"),
            ("   Spaces   ", "Spaces"),
            ("    non title text ", "Non Title Text"),
            (" the   book    of peas ", "The Book of Peas"),
            ("I'm loving it", "I'm Loving It"),
            ("war and peace", "War and Peace"),
        ];

        for (input, expected) in test_cases {
            assert_eq!(to_title_case(input), expected);
        }
    }
}
