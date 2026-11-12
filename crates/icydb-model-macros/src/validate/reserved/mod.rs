//! Module: validate::reserved
//! Responsibility: derive-side validation helpers.
//! Does not own: runtime validation.
//! Boundary: parse-time checks.

use std::{collections::HashSet, sync::LazyLock};

///
/// RESERVED_WORDS
/// Reserved Rust and IcyDB schema numeric identifiers.
///

static RESERVED_WORDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut words = Vec::new();

    // icydb schema numeric labels
    words.extend(["int128", "nat128"]);

    // rust
    // https://doc.rust-lang.org/reference/keywords.html
    words.extend([
        "as", "break", "const", "continue", "crate", "else", "enum", "extern", "false", "fn",
        "for", "gen", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub",
        "ref", "return", "self", "Self", "static", "struct", "super", "trait", "true", "type",
        "unsafe", "use", "where", "while", "async", "await", "dyn", "abstract", "become", "box",
        "do", "final", "macro", "override", "priv", "typeof", "unsized", "virtual", "yield", "try",
    ]);

    words.into_iter().collect()
});

/// Check if an identifier is a reserved word.
pub(crate) fn is_reserved_word(word: &str) -> bool {
    RESERVED_WORDS.contains(word)
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
