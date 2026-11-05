use super::{CallbackIdentity, CallbackKind, Issue};

#[test]
fn custom_issue_preserves_message() {
    let issue = Issue::from("pet name is reserved");

    assert_eq!(issue.message(), "pet name is reserved");
    assert_eq!(issue.callback(), None);
    assert_eq!(issue.to_string(), "pet name is reserved");
}

#[test]
fn typed_issue_displays_callback_identity_without_changing_its_message() {
    let mut issue = Issue::from("pet name is reserved");
    issue.bind_callback_if_unset(&CallbackIdentity::new(
        CallbackKind::Validator,
        "schema::PetName",
    ));

    let callback = issue.callback().expect("callback identity should bind");
    assert_eq!(callback.kind(), CallbackKind::Validator);
    assert_eq!(callback.type_path(), "schema::PetName");
    assert_eq!(issue.message(), "pet name is reserved");
    assert_eq!(
        issue.to_string(),
        "Validator schema::PetName: pet name is reserved"
    );
}
