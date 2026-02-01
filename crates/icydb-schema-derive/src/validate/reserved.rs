use std::{collections::HashSet, sync::LazyLock};

///
/// RESERVED_WORDS
/// basic reserved words list for anything using candid and rust
///

static RESERVED_WORDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut words = Vec::new();

    // candid
    words.extend(vec![
        "blob",
        "bool",
        "composite_query",
        "empty",
        "float32",
        "float64",
        "func",
        "import",
        "int",
        "int8",
        "int16",
        "int32",
        "int64",
        "nat",
        "nat8",
        "nat16",
        "nat32",
        "nat64",
        "null",
        "oneway",
        "opt",
        "principal",
        "query",
        "record",
        "reserved",
        "service",
        "text",
        "type",
        "variant",
        "vec",
    ]);

    // rust
    // https://doc.rust-lang.org/reference/keywords.html
    words.extend(vec![
        "as", "break", "const", "continue", "crate", "else", "enum", "extern", "false", "fn",
        "for", "gen", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub",
        "ref", "return", "self", "Self", "static", "struct", "super", "trait", "true", "type",
        "unsafe", "use", "where", "while", "async", "await", "dyn", "abstract", "become", "box",
        "do", "final", "macro", "override", "priv", "typeof", "unsized", "virtual", "yield", "try",
    ]);

    words.into_iter().collect()
});

/// Check if an identifier is a reserved word.
pub fn is_reserved_word(word: &str) -> bool {
    RESERVED_WORDS.contains(word)
}
