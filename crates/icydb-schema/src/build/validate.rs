use crate::build::reserved::WORDS;

pub const MAX_ENTITY_NAME_LEN: usize = 64;
pub const MAX_INDEX_NAME_LEN: usize = 200;

/// Ensure an identifier is non-empty and not a reserved keyword.
pub(crate) fn validate_ident(ident: &str) -> Result<(), String> {
    if ident.is_empty() {
        return Err("ident is empty".to_string());
    }

    // reserved?
    is_reserved(ident)?;

    Ok(())
}

/// Ensure entity names are non-empty, ASCII, and within the maximum length.
pub(crate) fn validate_entity_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("entity name is empty".to_string());
    }
    if name.len() > MAX_ENTITY_NAME_LEN {
        return Err(format!(
            "entity name '{name}' exceeds max length {MAX_ENTITY_NAME_LEN}"
        ));
    }
    if !name.is_ascii() {
        return Err(format!("entity name '{name}' must be ASCII"));
    }

    Ok(())
}

pub(crate) fn validate_index_name_len(entity_name: &str, fields: &[&str]) -> Result<(), String> {
    let mut len = entity_name.len();
    for field in fields {
        len = len.saturating_add(1 + field.len());
    }

    if len > MAX_INDEX_NAME_LEN {
        return Err(format!(
            "index name '{entity_name}|{fields:?}' exceeds max length {MAX_INDEX_NAME_LEN}"
        ));
    }

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
