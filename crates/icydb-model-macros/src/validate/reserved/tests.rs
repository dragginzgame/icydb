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
