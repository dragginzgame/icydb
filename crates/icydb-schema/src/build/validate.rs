use crate::build::reserved::WORDS;

// validate_ident
pub fn validate_ident(ident: &str) -> Result<(), String> {
    if ident.is_empty() {
        return Err("ident is empty".to_string());
    }

    // reserved?
    is_reserved(ident)?;

    Ok(())
}

// is_reserved
pub fn is_reserved(word: &str) -> Result<(), String> {
    if WORDS.contains(word) {
        return Err(format!("the word '{word}' is reserved"));
    }

    Ok(())
}
