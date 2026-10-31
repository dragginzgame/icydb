//! Composite declarations execute their type and payload hooks explicitly.

use icydb_model::visitor::{
    ApplicationOperation, CallbackIdentity, CallbackKind, Issue, Normalizer, Validator,
    VisitorContext, VisitorError,
};
use icydb_model::{normalize, prelude::*, validate};

#[normalizer]
pub struct Append(pub &'static str);

impl Append {
    /// Construct a callback that appends one visible ordering marker.
    #[must_use]
    pub const fn new(suffix: &'static str) -> Self {
        Self(suffix)
    }
}

impl Normalizer<String> for Append {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
        value.push_str(self.0);
        Ok(())
    }
}

impl Normalizer<Box<String>> for Append {
    fn normalize(&self, value: &mut Box<String>) -> Result<(), String> {
        value.push_str(self.0);
        Ok(())
    }
}

#[validator]
pub struct Reject;

impl<T> Validator<T> for Reject {
    fn validate(&self, _value: &T, ctx: &mut dyn VisitorContext) {
        ctx.add_issue(Issue::from("declared validator"));
    }
}

#[normalizer]
pub struct RejectNormalize;

impl<T> Normalizer<T> for RejectNormalize {
    fn normalize(&self, _value: &mut T) -> Result<(), String> {
        Err("declared normalizer".into())
    }
}

#[record(
    ty(
        normalizer(path = "Append", args("type-1")),
        normalizer(path = "Append", args("/type-2")),
        validator(path = "Reject")
    ),
    fields(field(
        name = "text",
        value(item(prim = "Text", unbounded, normalizer(path = "Append", args("/item"))))
    ))
)]
pub struct HookRecord;

impl Normalizer<HookRecord> for Append {
    fn normalize(&self, value: &mut HookRecord) -> Result<(), String> {
        value.text.push_str(self.0);
        Ok(())
    }
}

#[canister(memory_namespace = "composite_hooks")]
pub struct HookCanister;

#[store(canister = "HookCanister", storage(heap()))]
pub struct HookStore;

#[entity(
    store = "HookStore",
    version = 1,
    pk(field = "id"),
    ty(
        normalizer(path = "Append", args("type-1")),
        normalizer(path = "Append", args("/type-2")),
        validator(path = "Reject")
    ),
    fields(
        field(name = "id", value(item(prim = "Nat64"))),
        field(
            name = "text",
            value(item(prim = "Text", unbounded, normalizer(path = "Append", args("/item"))))
        ),
        field(name = "child", value(item(is = "HookRecord")))
    )
)]
pub struct HookEntity;

impl Normalizer<HookEntity> for Append {
    fn normalize(&self, value: &mut HookEntity) -> Result<(), String> {
        value.text.push_str(self.0);
        Ok(())
    }
}

#[tuple(
    ty(
        normalizer(path = "Append", args("type-1")),
        normalizer(path = "Append", args("/type-2")),
        validator(path = "Reject")
    ),
    value(item(
        prim = "Text",
        unbounded,
        normalizer(path = "Append", args("/item")),
        validator(path = "Reject")
    )),
    value(
        opt,
        item(
            prim = "Text",
            unbounded,
            normalizer(path = "Append", args("/optional")),
            validator(path = "Reject")
        )
    ),
    value(
        many,
        item(
            prim = "Text",
            unbounded,
            normalizer(path = "Append", args("/repeated")),
            validator(path = "Reject")
        )
    ),
    value(item(is = "HookRecord"))
)]
pub struct HookTuple;

impl Normalizer<HookTuple> for Append {
    fn normalize(&self, value: &mut HookTuple) -> Result<(), String> {
        value.0.push_str(self.0);
        Ok(())
    }
}

#[enum_(
    ty(
        normalizer(path = "Append", args("type-1")),
        normalizer(path = "Append", args("/type-2")),
        validator(path = "Reject")
    ),
    variant(name = "Empty"),
    variant(
        name = "Scalar",
        value(item(
            prim = "Text",
            unbounded,
            normalizer(path = "Append", args("/item")),
            validator(path = "Reject")
        ))
    ),
    variant(
        name = "Optional",
        value(
            opt,
            item(
                prim = "Text",
                unbounded,
                normalizer(path = "Append", args("/optional")),
                validator(path = "Reject")
            )
        )
    ),
    variant(
        name = "Repeated",
        value(
            many,
            item(
                indirect,
                prim = "Text",
                unbounded,
                normalizer(path = "Append", args("/repeated")),
                validator(path = "Reject")
            )
        )
    ),
    variant(name = "Nested", value(item(is = "HookRecord")))
)]
pub struct HookEnum;

impl Normalizer<HookEnum> for Append {
    fn normalize(&self, value: &mut HookEnum) -> Result<(), String> {
        // The first whole-type hook can change the active payload before its
        // own item hooks and child traversal are selected.
        if matches!(value, HookEnum::Empty) {
            *value = HookEnum::Scalar(String::new());
        }
        match value {
            HookEnum::Scalar(text) => text.push_str(self.0),
            HookEnum::Empty
            | HookEnum::Optional(_)
            | HookEnum::Repeated(_)
            | HookEnum::Nested(_) => {}
        }
        Ok(())
    }
}

fn assert_rejections(error: &VisitorError, expected: &[(&str, usize)]) {
    assert_eq!(error.operation(), ApplicationOperation::Validate);
    assert_eq!(error.issues().len(), expected.len());
    let callback = CallbackIdentity::new(CallbackKind::Validator, std::any::type_name::<Reject>());
    for &(path, count) in expected {
        let issues = error.issues().get(path).unwrap();
        assert_eq!(issues.len(), count, "{path}");
        assert!(
            issues
                .iter()
                .all(|issue| issue.callback() == Some(&callback))
        );
    }
}

#[test]
fn record_type_hooks_run_before_field_hooks() {
    let mut record = HookRecord {
        text: String::new(),
    };
    normalize(&mut record).unwrap();
    assert_eq!(record.text, "type-1/type-2/item");
    assert_rejections(&validate(&record).unwrap_err(), &[("", 1)]);
}

#[test]
fn entity_type_hooks_preserve_fields_and_child_traversal() {
    let mut entity = HookEntity {
        id: 17,
        text: String::new(),
        child: HookRecord {
            text: String::new(),
        },
    };
    normalize(&mut entity).unwrap();
    assert_eq!(entity.id, 17);
    assert_eq!(entity.text, "type-1/type-2/item");
    assert_eq!(entity.child.text, "type-1/type-2/item");
    assert_rejections(&validate(&entity).unwrap_err(), &[("", 1), ("child", 1)]);
}

#[test]
fn tuple_hooks_preserve_cardinality_order_and_nested_paths() {
    let mut tuple = HookTuple(
        String::new(),
        Some("some".into()),
        vec!["a".into(), "b".into()],
        HookRecord {
            text: String::new(),
        },
    );
    normalize(&mut tuple).unwrap();
    assert_eq!(tuple.0, "type-1/type-2/item");
    assert_eq!(tuple.1.as_deref(), Some("some/optional"));
    assert_eq!(tuple.2, ["a/repeated", "b/repeated"]);
    assert_eq!(tuple.3.text, "type-1/type-2/item");
    assert_rejections(
        &validate(&tuple).unwrap_err(),
        &[("", 1), ("0", 1), ("1", 1), ("2", 2), ("3", 1)],
    );
    tuple.1 = None;
    tuple.2.clear();
    assert_rejections(
        &validate(&tuple).unwrap_err(),
        &[("", 1), ("0", 1), ("3", 1)],
    );
}

#[test]
fn enum_type_hook_selects_payload_before_item_hooks() {
    let mut value = HookEnum::Empty;
    normalize(&mut value).unwrap();
    assert!(matches!(&value, HookEnum::Scalar(text) if text == "type-1/type-2/item"));
    assert_rejections(&validate(&value).unwrap_err(), &[("", 1), ("Scalar", 1)]);
}

#[test]
fn enum_payload_hooks_preserve_cardinality_and_child_traversal() {
    let mut optional = HookEnum::Optional(Some("some".into()));
    normalize(&mut optional).unwrap();
    assert!(matches!(&optional, HookEnum::Optional(Some(text)) if text == "some/optional"));
    assert_rejections(
        &validate(&optional).unwrap_err(),
        &[("", 1), ("Optional", 1)],
    );
    let mut repeated = HookEnum::Repeated(vec![Box::new("a".into()), Box::new("b".into())]);
    normalize(&mut repeated).unwrap();
    assert!(
        matches!(&repeated, HookEnum::Repeated(items) if items.iter().map(|item| item.as_str()).collect::<Vec<_>>() == ["a/repeated", "b/repeated"])
    );
    assert_rejections(
        &validate(&repeated).unwrap_err(),
        &[("", 1), ("Repeated", 2)],
    );
    let mut nested = HookEnum::Nested(HookRecord {
        text: String::new(),
    });
    normalize(&mut nested).unwrap();
    assert!(matches!(&nested, HookEnum::Nested(record) if record.text == "type-1/type-2/item"));
    assert_rejections(&validate(&nested).unwrap_err(), &[("", 1), ("Nested", 1)]);
}

#[test]
fn enum_absent_empty_and_unit_variants_invoke_no_payload_validator() {
    for value in [
        HookEnum::Optional(None),
        HookEnum::Repeated(Vec::new()),
        HookEnum::Empty,
    ] {
        assert_rejections(&validate(&value).unwrap_err(), &[("", 1)]);
    }
    for mut value in [HookEnum::Optional(None), HookEnum::Repeated(Vec::new())] {
        normalize(&mut value).unwrap();
        assert!(matches!(
            value,
            HookEnum::Optional(None) | HookEnum::Repeated(_)
        ));
    }
}

#[record(ty(normalizer(path = "RejectNormalize")), fields())]
pub struct RejectedRecord;

#[tuple(
    ty(normalizer(path = "RejectNormalize")),
    value(item(prim = "Nat32", normalizer(path = "RejectNormalize")))
)]
pub struct RejectedTuple;

#[enum_(
    ty(normalizer(path = "RejectNormalize")),
    variant(name = "Empty"),
    variant(
        name = "Value",
        value(item(prim = "Nat32", normalizer(path = "RejectNormalize")))
    )
)]
pub struct RejectedEnum;

#[test]
fn composite_normalizer_failures_keep_paths_and_callback_identity() {
    let callback = CallbackIdentity::new(
        CallbackKind::Normalizer,
        std::any::type_name::<RejectNormalize>(),
    );
    for (mut value, paths) in [
        (
            Box::new(RejectedRecord {}) as Box<dyn icydb_model::visitor::Visitable>,
            vec![""],
        ),
        (Box::new(RejectedTuple(0)), vec!["", "0"]),
        (Box::new(RejectedEnum::Value(0)), vec!["", "Value"]),
        (Box::new(RejectedEnum::Empty), vec![""]),
    ] {
        let error = normalize(value.as_mut()).unwrap_err();
        assert_eq!(error.operation(), ApplicationOperation::Normalize);
        assert_eq!(error.issues().len(), paths.len());
        for path in paths {
            assert_eq!(
                error.issues().get(path).unwrap()[0].callback(),
                Some(&callback)
            );
        }
    }
}
