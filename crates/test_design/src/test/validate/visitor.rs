use crate::prelude::*;

///
/// VisitorLowerText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(validator(path = "base::validator::text::case::Lower"))
)]
pub struct VisitorLowerText {}

///
/// VisitorLowerTextList
///

#[list(item(is = "VisitorLowerText"))]
pub struct VisitorLowerTextList {}

///
/// VisitorLowerTextTuple
///

#[tuple(
    value(item(is = "VisitorLowerText")),
    value(item(is = "VisitorLowerText"))
)]
pub struct VisitorLowerTextTuple {}

///
/// VisitorLowerTextMap
///

#[map(key(prim = "Text"), value(item(is = "VisitorLowerText")))]
pub struct VisitorLowerTextMap {}

///
/// VisitorInner
///

#[record(fields(field(ident = "leaf", value(item(is = "VisitorLowerText")))))]
pub struct VisitorInner {}

///
/// VisitorOuter
///

#[record(fields(
    field(ident = "list", value(item(is = "VisitorLowerTextList"))),
    field(ident = "rec", value(item(is = "VisitorInner"))),
    field(ident = "tup", value(item(is = "VisitorLowerTextTuple"))),
    field(ident = "map", value(item(is = "VisitorLowerTextMap"))),
))]
pub struct VisitorOuter {}

///
/// VisitorLowerTextSetValidated
///

#[set(item(prim = "Text", validator(path = "base::validator::text::case::Lower")))]
pub struct VisitorLowerTextSetValidated {}

///
/// VisitorLowerTextKeyMapValidated
///

#[map(
    key(prim = "Text", validator(path = "base::validator::text::case::Lower")),
    value(item(prim = "Text"))
)]
pub struct VisitorLowerTextKeyMapValidated {}

///
/// VisitorLowerTextValueMapValidated
///

#[map(
    key(prim = "Text"),
    value(item(prim = "Text", validator(path = "base::validator::text::case::Lower")))
)]
pub struct VisitorLowerTextValueMapValidated {}

///
/// VisitorSetOuter
///

#[record(fields(field(ident = "set", value(item(is = "VisitorLowerTextSetValidated")))))]
pub struct VisitorSetOuter {}

///
/// VisitorMapKeyOuter
///

#[record(fields(field(ident = "map", value(item(is = "VisitorLowerTextKeyMapValidated")))))]
pub struct VisitorMapKeyOuter {}

///
/// VisitorMapValueOuter
///

#[record(fields(field(ident = "map", value(item(is = "VisitorLowerTextValueMapValidated")))))]
pub struct VisitorMapValueOuter {}

///
/// VisitorLengthList
///

#[list(
    item(prim = "Text"),
    ty(validator(path = "base::validator::len::Max", args(1)))
)]
pub struct VisitorLengthList {}

///
/// VisitorLengthSet
///

#[set(
    item(prim = "Text"),
    ty(validator(path = "base::validator::len::Max", args(1)))
)]
pub struct VisitorLengthSet {}

///
/// VisitorLengthMap
///

#[map(
    key(prim = "Text"),
    value(item(prim = "Text")),
    ty(validator(path = "base::validator::len::Max", args(1)))
)]
pub struct VisitorLengthMap {}

///
/// VisitorLengthOuter
///

#[record(fields(
    field(ident = "list", value(item(is = "VisitorLengthList"))),
    field(ident = "set", value(item(is = "VisitorLengthSet"))),
    field(ident = "map", value(item(is = "VisitorLengthMap"))),
))]
pub struct VisitorLengthOuter {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::core::{Error, validate};

    #[test]
    fn validate_tracks_paths_for_nested_structures() {
        let node = VisitorOuter {
            list: VisitorLowerTextList::from(vec!["lower".to_string(), "MiXeD".to_string()]),
            rec: VisitorInner {
                leaf: VisitorLowerText::from("MiXeD"),
            },
            tup: VisitorLowerTextTuple(
                VisitorLowerText::from("MiXeD"),
                VisitorLowerText::from("lower"),
            ),
            map: VisitorLowerTextMap::from(vec![("KeyOne".to_string(), "MiXeD".to_string())]),
        };

        let err = validate(&node).expect_err("expected validation issues");
        let err_string = err.to_string();
        let issues = match &err {
            Error::ValidateError(issues) => issues,
            other => panic!("unexpected error: {other:?}"),
        };

        for key in ["list[1]", "tup.0", "map[0]", "rec.leaf"] {
            let messages = issues
                .get(key)
                .unwrap_or_else(|| panic!("missing issues for {key}"));
            assert!(
                messages.iter().any(|msg| msg.contains("not lower case")),
                "missing validation message for {key}"
            );
            assert!(
                err_string.contains(key),
                "expected error string to mention {key}"
            );
        }
    }

    #[test]
    fn validate_tracks_paths_for_set_item_validators() {
        let node = VisitorSetOuter {
            set: VisitorLowerTextSetValidated::from(vec!["MiXeD".to_string()]),
        };

        let err = validate(&node).expect_err("expected validation issues");
        let err_string = err.to_string();
        let issues = match &err {
            Error::ValidateError(issues) => issues,
            other => panic!("unexpected error: {other:?}"),
        };

        let key = "set[0]";
        let messages = issues
            .get(key)
            .unwrap_or_else(|| panic!("missing issues for {key}"));
        assert!(
            messages.iter().any(|msg| msg.contains("not lower case")),
            "missing validation message for {key}"
        );
        assert!(
            err_string.contains(key),
            "expected error string to mention {key}"
        );
    }

    #[test]
    fn validate_tracks_paths_for_map_key_validators() {
        let node = VisitorMapKeyOuter {
            map: VisitorLowerTextKeyMapValidated::from(vec![(
                "MiXeD".to_string(),
                "lower".to_string(),
            )]),
        };

        let err = validate(&node).expect_err("expected validation issues");
        let err_string = err.to_string();
        let issues = match &err {
            Error::ValidateError(issues) => issues,
            other => panic!("unexpected error: {other:?}"),
        };

        let key = "map[0]";
        let messages = issues
            .get(key)
            .unwrap_or_else(|| panic!("missing issues for {key}"));
        assert!(
            messages.iter().any(|msg| msg.contains("not lower case")),
            "missing validation message for {key}"
        );
        assert!(
            err_string.contains(key),
            "expected error string to mention {key}"
        );
    }

    #[test]
    fn validate_tracks_paths_for_map_value_validators() {
        let node = VisitorMapValueOuter {
            map: VisitorLowerTextValueMapValidated::from(vec![(
                "lower".to_string(),
                "MiXeD".to_string(),
            )]),
        };

        let err = validate(&node).expect_err("expected validation issues");
        let err_string = err.to_string();
        let issues = match &err {
            Error::ValidateError(issues) => issues,
            other => panic!("unexpected error: {other:?}"),
        };

        let key = "map[0]";
        let messages = issues
            .get(key)
            .unwrap_or_else(|| panic!("missing issues for {key}"));
        assert!(
            messages.iter().any(|msg| msg.contains("not lower case")),
            "missing validation message for {key}"
        );
        assert!(
            err_string.contains(key),
            "expected error string to mention {key}"
        );
    }

    #[test]
    fn validate_tracks_paths_for_collection_length_validators() {
        let node = VisitorLengthOuter {
            list: VisitorLengthList::from(vec!["one".to_string(), "two".to_string()]),
            set: VisitorLengthSet::from(vec!["one".to_string(), "two".to_string()]),
            map: VisitorLengthMap::from(vec![
                ("one".to_string(), "a".to_string()),
                ("two".to_string(), "b".to_string()),
            ]),
        };

        let err = validate(&node).expect_err("expected validation issues");
        let err_string = err.to_string();
        let issues = match &err {
            Error::ValidateError(issues) => issues,
            other => panic!("unexpected error: {other:?}"),
        };

        for key in ["list", "set", "map"] {
            let messages = issues
                .get(key)
                .unwrap_or_else(|| panic!("missing issues for {key}"));
            assert!(
                !messages.is_empty(),
                "expected validation messages for {key}"
            );
            assert!(
                err_string.contains(key),
                "expected error string to mention {key}"
            );
        }
    }
}
