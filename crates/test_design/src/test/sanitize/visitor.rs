use crate::prelude::*;
use icydb::core::traits::Sanitizer;

///
/// VisitorLowerText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(sanitizer(path = "base::sanitizer::text::case::Lower"))
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
/// VisitorOuter
///

#[record(fields(
    field(ident = "list", value(item(is = "VisitorLowerTextList"))),
    field(ident = "tup", value(item(is = "VisitorLowerTextTuple"))),
    field(ident = "map", value(item(is = "VisitorLowerTextMap"))),
))]
pub struct VisitorOuter {}

///
/// Reject
///

#[sanitizer]
pub struct Reject;

impl Sanitizer<String> for Reject {
    fn sanitize(&self, _value: &mut String) -> Result<(), String> {
        Err("rejected".to_string())
    }
}

///
/// VisitorRejectText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(sanitizer(path = "crate::test::sanitize::visitor::Reject"))
)]
pub struct VisitorRejectText {}

///
/// VisitorRejectTextList
///

#[list(item(is = "VisitorRejectText"))]
pub struct VisitorRejectTextList {}

///
/// VisitorRejectOuter
///

#[record(fields(
    field(
        ident = "field",
        value(item(
            prim = "Text",
            sanitizer(path = "crate::test::sanitize::visitor::Reject")
        ))
    ),
    field(ident = "list", value(item(is = "VisitorRejectTextList"))),
))]
pub struct VisitorRejectOuter {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::core::{Error, sanitize};
    use std::collections::HashMap;

    #[test]
    fn sanitize_updates_nested_structures() {
        let mut node = VisitorOuter {
            list: VisitorLowerTextList::from(vec!["MiXeD".to_string(), "AnOtHeR".to_string()]),
            tup: VisitorLowerTextTuple(
                VisitorLowerText::from("MiXeD"),
                VisitorLowerText::from("AnOtHeR"),
            ),
            map: VisitorLowerTextMap::from(vec![
                ("KeyOne".to_string(), "MiXeD".to_string()),
                ("KeyTwo".to_string(), "AnOtHeR".to_string()),
            ]),
        };

        sanitize(&mut node).unwrap();

        let expected_list = vec!["mixed".to_string(), "another".to_string()];
        assert_eq!(*node.list, expected_list);
        assert_eq!(&*node.tup.0, "mixed");
        assert_eq!(&*node.tup.1, "another");

        let actual_map: HashMap<_, _> = node
            .map
            .iter()
            .map(|(k, v)| (k.clone(), v.to_string()))
            .collect();
        let expected_map = HashMap::from([
            ("KeyOne".to_string(), "mixed".to_string()),
            ("KeyTwo".to_string(), "another".to_string()),
        ]);

        assert_eq!(actual_map, expected_map);
    }

    #[test]
    fn sanitize_collects_issue_paths() {
        let mut node = VisitorRejectOuter {
            field: "bad".to_string(),
            list: VisitorRejectTextList::from(vec!["one".to_string(), "two".to_string()]),
        };

        let err = sanitize(&mut node).expect_err("expected sanitization issues");
        let err_string = err.to_string();
        let issues = match &err {
            Error::SanitizeError(issues) => issues,
            other => panic!("unexpected error: {other:?}"),
        };

        assert_eq!(issues.len(), 3);

        for key in ["field", "list[0]", "list[1]"] {
            let messages = issues
                .get(key)
                .unwrap_or_else(|| panic!("missing issues for {key}"));
            assert!(
                messages.iter().any(|msg| msg.contains("rejected")),
                "missing rejection message for {key}"
            );
            assert!(
                err_string.contains(key),
                "expected error string to mention {key}"
            );
        }
    }
}
