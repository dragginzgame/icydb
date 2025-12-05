use crate::build::reserved::WORDS;

/// Ensure an identifier is non-empty and not a reserved keyword.
pub(crate) fn validate_ident(ident: &str) -> Result<(), String> {
    if ident.is_empty() {
        return Err("ident is empty".to_string());
    }

    // reserved?
    is_reserved(ident)?;

    Ok(())
}

fn is_reserved(word: &str) -> Result<(), String> {
    if WORDS.contains(word) {
        return Err(format!("the word '{word}' is reserved"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_and_reserved_words() {
        assert!(validate_ident("").is_err(), "empty identifiers should fail");
        assert!(
            validate_ident("record").is_err(),
            "reserved keywords should be rejected"
        );
    }

    #[test]
    fn accepts_non_reserved_identifier() {
        assert!(validate_ident("custom_ident").is_ok());
    }
}
