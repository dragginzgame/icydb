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
