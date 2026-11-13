//! Collection directives run explicitly; sorted keys remain immutable.

use icydb_model::{normalize, prelude::*};
use std::collections::BTreeMap;

#[normalizer]
/// Append a marker to prove that a field normalizer ran.
pub struct Suffix;

impl icydb_model::visitor::Normalizer<String> for Suffix {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
        value.push('!');
        Ok(())
    }
}

#[list(item(
    prim = "Text",
    unbounded,
    normalizer(path = "base::normalizer::text::Trim"),
    normalizer(path = "Suffix")
))]
/// List applying text normalizers to each element.
pub struct NormalizedList;

#[map(
    key(prim = "Text", unbounded),
    value(item(
        prim = "Text",
        unbounded,
        normalizer(path = "base::normalizer::text::Trim"),
        normalizer(path = "Suffix")
    ))
)]
/// Map applying text normalizers to each value.
pub struct NormalizedMap;

#[map(
    key(prim = "Text", unbounded),
    value(opt, item(prim = "Text", unbounded, normalizer(path = "Suffix")))
)]
/// Map exercising normalization of optional values.
pub struct OptionalMap;

#[map(
    key(prim = "Text", unbounded),
    value(many, item(prim = "Text", unbounded, normalizer(path = "Suffix")))
)]
/// Map exercising normalization of repeated values.
pub struct RepeatedMap;

#[test]
fn list_item_directives_run_in_declaration_order() {
    let mut list = NormalizedList(vec![" A ".into(), " B ".into()]);
    normalize(&mut list).unwrap();
    assert_eq!(list.0, ["A!", "B!"]);
    let mut empty = NormalizedList(Vec::new());
    normalize(&mut empty).unwrap();
    assert!(empty.0.is_empty());
}

#[test]
fn map_value_directives_preserve_keys_and_cardinality() {
    let mut map = NormalizedMap(BTreeMap::from([(" key ".into(), " V ".into())]));
    normalize(&mut map).unwrap();
    assert_eq!(map.0[" key "], "V!");
    let mut optional = OptionalMap(BTreeMap::from([
        ("none".into(), None),
        ("some".into(), Some("value".into())),
    ]));
    normalize(&mut optional).unwrap();
    assert_eq!(optional.0["none"], None);
    assert_eq!(optional.0["some"].as_deref(), Some("value!"));
    let mut repeated = RepeatedMap(BTreeMap::from([
        ("empty".into(), Vec::new()),
        ("values".into(), vec!["a".into(), "b".into()]),
    ]));
    normalize(&mut repeated).unwrap();
    assert!(repeated.0["empty"].is_empty());
    assert_eq!(repeated.0["values"], ["a!", "b!"]);
}

#[newtype(
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Lower"))
)]
/// Text wrapper with its own lowercase normalization hook.
pub struct NestedText;

impl icydb_model::visitor::Normalizer<NestedText> for Suffix {
    fn normalize(&self, value: &mut NestedText) -> Result<(), String> {
        value.0.push('!');
        Ok(())
    }
}

#[list(item(is = "NestedText", normalizer(path = "Suffix")))]
/// List combining nested-value normalization with an element attachment.
pub struct NestedList;

#[normalizer]
/// Report a diagnostic without accepting the visited value.
pub struct Reject;

impl icydb_model::visitor::Normalizer<String> for Reject {
    fn normalize(&self, _value: &mut String) -> Result<(), String> {
        Err("rejected".into())
    }
}

#[list(item(prim = "Text", unbounded, normalizer(path = "Reject")))]
/// List whose element normalizer reports failures.
pub struct RejectedList;

#[map(
    key(prim = "Text", unbounded),
    value(opt, item(prim = "Text", unbounded, normalizer(path = "Reject")))
)]
/// Map whose optional-value normalizer reports failures.
pub struct RejectedMap;

#[test]
fn attached_hooks_preserve_named_child_normalization() {
    let mut list = NestedList(vec![NestedText("MiXeD".into())]);
    normalize(&mut list).unwrap();
    assert_eq!(list.0[0].0, "mixed!");
}

#[test]
fn collection_callback_failures_retain_index_paths_and_identity() {
    use icydb_model::visitor::{ApplicationOperation, CallbackIdentity, CallbackKind};

    let callback = CallbackIdentity::new(CallbackKind::Normalizer, std::any::type_name::<Reject>());
    let mut list = RejectedList(vec!["a".into(), "b".into()]);
    let error = normalize(&mut list).unwrap_err();
    assert_eq!(error.operation(), ApplicationOperation::Normalize);
    assert_eq!(error.issues().len(), 2);
    for index in ["[0]", "[1]"] {
        let issues = error.issues().get(index).unwrap();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].callback(), Some(&callback));
    }
    let mut map = RejectedMap(BTreeMap::from([
        ("absent".into(), None),
        ("present".into(), Some("value".into())),
    ]));
    let error = normalize(&mut map).unwrap_err();
    assert_eq!(error.issues().len(), 1);
    let issue = &error.issues().get("[1]").unwrap()[0];
    assert_eq!(issue.callback(), Some(&callback));
    assert!(map.0["absent"].is_none());
}

#[normalizer]
/// Remove collection contents to test normalization before child traversal.
pub struct Clear;

impl icydb_model::visitor::Normalizer<std::collections::BTreeSet<String>> for Clear {
    fn normalize(&self, value: &mut std::collections::BTreeSet<String>) -> Result<(), String> {
        value.clear();
        Ok(())
    }
}

impl icydb_model::visitor::Normalizer<BTreeMap<String, String>> for Clear {
    fn normalize(&self, value: &mut BTreeMap<String, String>) -> Result<(), String> {
        value.clear();
        Ok(())
    }
}

#[validator]
/// Reject blank text while validating collection contents.
pub struct NotBlank;

impl icydb_model::visitor::Validator<String> for NotBlank {
    fn validate(&self, value: &String, ctx: &mut dyn icydb_model::visitor::VisitorContext) {
        if value.is_empty() {
            ctx.add_issue(icydb_model::visitor::Issue::from("blank key"));
        }
    }
}

#[set(
    item(prim = "Text", unbounded, validator(path = "NotBlank")),
    ty(normalizer(path = "Clear"))
)]
/// Set testing whether clearing happens before element validation.
pub struct CheckedSet;

#[map(
    key(prim = "Text", unbounded, validator(path = "NotBlank")),
    value(item(prim = "Text", unbounded)),
    ty(normalizer(path = "Clear"))
)]
/// Map testing whether clearing happens before key validation.
pub struct CheckedMap;

#[test]
fn immutable_key_validators_and_container_normalizers_remain_supported() {
    let mut set = CheckedSet(std::collections::BTreeSet::from([String::new()]));
    let mut map = CheckedMap(BTreeMap::from([(String::new(), "value".into())]));
    assert!(icydb_model::validate(&set).is_err());
    assert!(icydb_model::validate(&map).is_err());
    normalize(&mut set).unwrap();
    normalize(&mut map).unwrap();
    assert!(set.0.is_empty());
    assert!(map.0.is_empty());
}
