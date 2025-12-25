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

        assert_eq!(issues.len(), 4);

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
}
