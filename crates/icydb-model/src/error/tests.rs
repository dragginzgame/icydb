use super::*;

#[test]
fn test_empty_errors() {
    let errs = ErrorTree::new();
    assert!(errs.is_empty());
    assert_eq!(errs.result(), Ok(()));
}

#[test]
fn test_add_and_merge() {
    let mut errs = ErrorTree::new();
    errs.add("top-level error");

    let mut child_errs = ErrorTree::new();
    child_errs.add("child error 1");
    child_errs.add("child error 2");
    errs.merge_for("field", ErrorTree::from("field error"));
    errs.merge_for("nested", child_errs);

    // Check hierarchical structure.
    assert_eq!(errs.messages().len(), 1);
    assert!(errs.children().contains_key("field"));
    assert!(errs.children().contains_key("nested"));

    // Flatten and check that errors include keys.
    let flat = errs.flatten_ref();
    assert_eq!(flat.len(), 4);
}
