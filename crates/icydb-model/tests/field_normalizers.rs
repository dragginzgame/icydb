//! Attached field hooks follow item cardinality and retain field diagnostics.

use icydb_model::{normalize, prelude::*};

#[normalizer]
pub struct Suffix;

impl icydb_model::visitor::Normalizer<String> for Suffix {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
        value.push('!');
        Ok(())
    }
}

#[record(fields(
    field(
        name = "scalar",
        value(item(
            prim = "Text",
            unbounded,
            normalizer(path = "base::normalizer::text::Trim"),
            normalizer(path = "Suffix")
        ))
    ),
    field(
        name = "optional",
        value(
            opt,
            item(
                prim = "Text",
                unbounded,
                normalizer(path = "base::normalizer::text::Trim"),
                normalizer(path = "Suffix")
            )
        )
    ),
    field(
        name = "repeated",
        value(
            many,
            item(
                prim = "Text",
                unbounded,
                normalizer(path = "base::normalizer::text::Trim"),
                normalizer(path = "Suffix")
            )
        )
    )
))]
pub struct TextFields;

#[canister(memory_namespace = "field_normalizers")]
pub struct HookCanister;

#[store(canister = "HookCanister", storage(heap()))]
pub struct HookStore;

#[entity(
    store = "HookStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Nat64"))),
        field(
            name = "optional",
            value(opt, item(prim = "Text", unbounded, normalizer(path = "Suffix")))
        ),
        field(
            name = "repeated",
            value(many, item(prim = "Text", unbounded, normalizer(path = "Suffix")))
        ),
        field(name = "child", value(item(is = "TextFields")))
    )
)]
pub struct HookRow;

#[test]
fn record_hooks_apply_in_order_to_each_present_item() {
    let mut record = TextFields {
        scalar: " scalar ".into(),
        optional: Some(" optional ".into()),
        repeated: vec![" first ".into(), " second ".into()],
    };
    normalize(&mut record).unwrap();
    assert_eq!(record.scalar, "scalar!");
    assert_eq!(record.optional.as_deref(), Some("optional!"));
    assert_eq!(record.repeated, ["first!", "second!"]);
}

#[test]
fn record_hooks_preserve_absent_and_empty_fields() {
    let mut record = TextFields {
        scalar: " scalar ".into(),
        optional: None,
        repeated: Vec::new(),
    };
    normalize(&mut record).unwrap();
    assert_eq!(record.scalar, "scalar!");
    assert!(record.optional.is_none());
    assert!(record.repeated.is_empty());
}

#[test]
fn entity_hooks_preserve_unattached_fields_and_nested_traversal() {
    let mut row = HookRow {
        id: 7,
        optional: Some("label".into()),
        repeated: vec!["first".into(), "second".into()],
        child: TextFields {
            scalar: " child ".into(),
            optional: Some(" optional child ".into()),
            repeated: vec![" repeated child ".into()],
        },
    };
    normalize(&mut row).unwrap();
    assert_eq!(row.id, 7);
    assert_eq!(row.optional.as_deref(), Some("label!"));
    assert_eq!(row.repeated, ["first!", "second!"]);
    assert_eq!(row.child.scalar, "child!");
    assert_eq!(row.child.optional.as_deref(), Some("optional child!"));
    assert_eq!(row.child.repeated, ["repeated child!"]);

    row.optional = None;
    row.repeated.clear();
    normalize(&mut row).unwrap();
    assert!(row.optional.is_none());
    assert!(row.repeated.is_empty());
    assert_eq!(row.child.scalar, "child!!");
}

#[normalizer]
pub struct Reject;

impl icydb_model::visitor::Normalizer<String> for Reject {
    fn normalize(&self, _value: &mut String) -> Result<(), String> {
        Err("rejected".into())
    }
}

#[record(fields(
    field(
        name = "scalar",
        value(item(prim = "Text", unbounded, normalizer(path = "Reject")))
    ),
    field(
        name = "optional",
        value(opt, item(prim = "Text", unbounded, normalizer(path = "Reject")))
    ),
    field(
        name = "repeated",
        value(many, item(prim = "Text", unbounded, normalizer(path = "Reject")))
    )
))]
pub struct RejectedFields;

#[test]
fn errors_keep_field_paths_and_concrete_callback_identity() {
    use icydb_model::visitor::{ApplicationOperation, CallbackIdentity, CallbackKind};

    let mut record = RejectedFields {
        scalar: "scalar".into(),
        optional: Some("optional".into()),
        repeated: vec!["first".into(), "second".into()],
    };
    let error = normalize(&mut record).unwrap_err();
    assert_eq!(error.operation(), ApplicationOperation::Normalize);
    assert_eq!(error.issues().len(), 3);
    let callback = CallbackIdentity::new(CallbackKind::Normalizer, std::any::type_name::<Reject>());
    for (field, expected) in [("scalar", 1), ("optional", 1), ("repeated", 2)] {
        let issues = error.issues().get(field).unwrap();
        assert_eq!(issues.len(), expected);
        assert!(
            issues
                .iter()
                .all(|issue| issue.callback() == Some(&callback))
        );
    }
}

#[test]
fn absent_and_empty_fields_invoke_no_rejecting_callback() {
    let mut record = RejectedFields {
        scalar: "scalar".into(),
        optional: None,
        repeated: Vec::new(),
    };
    let error = normalize(&mut record).unwrap_err();
    assert_eq!(error.issues().len(), 1);
    assert_eq!(error.issues().get("scalar").unwrap().len(), 1);
    assert!(error.issues().get("optional").is_none());
    assert!(error.issues().get("repeated").is_none());
}
