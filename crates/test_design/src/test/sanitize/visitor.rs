use crate::prelude::*;

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
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::core::sanitize;
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
}
