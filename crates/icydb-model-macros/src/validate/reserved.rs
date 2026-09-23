//! Module: validate::reserved
//! Responsibility: derive-side validation helpers.
//! Does not own: runtime validation.
//! Boundary: parse-time checks.

use std::{collections::HashSet, sync::LazyLock};

///
/// RESERVED_WORDS
/// basic reserved words list for anything using candid and rust
///

static RESERVED_WORDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut words = Vec::new();

    // candid
    words.extend([
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

    // icydb schema numeric labels
    words.extend(["int128", "int_big", "nat128", "nat_big"]);

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
mod tests {
    use super::RESERVED_WORDS;
    use std::collections::HashSet;

    #[test]
    fn documentation_reserved_identifiers_match_compiled_owner() {
        let guide = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/guides/schema-authoring.md"
        ))
        .expect("repository schema authoring guide");
        let section = guide
            .split_once("<!-- icydb-reserved-field-identifiers:start -->")
            .expect("reserved identifier data start")
            .1
            .split_once("<!-- icydb-reserved-field-identifiers:end -->")
            .expect("reserved identifier data end")
            .0;
        let documented = section
            .split('`')
            .skip(1)
            .step_by(2)
            .collect::<HashSet<_>>();
        assert_eq!(&documented, &*RESERVED_WORDS);
    }
}
