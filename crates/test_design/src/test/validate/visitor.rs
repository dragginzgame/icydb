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
    use icydb::core::{traits::Visitable, validate, visitor::ValidateError};

    fn flatten_errs(node: &dyn Visitable) -> Vec<(String, String)> {
        match validate(node) {
            Ok(()) => Vec::new(),
            Err(ValidateError::ValidationFailed(tree)) => tree.flatten_ref(),
            Err(e @ ValidateError::ValidatorError(_)) => panic!("unexpected config error: {e}"),
        }
    }

    #[test]
    fn root_level_error_is_at_root() {
        let value = VisitorLowerText::from("NotLower");
        let flat = flatten_errs(&value);

        assert!(flat.iter().any(|(k, _)| k.is_empty()));
    }

    #[test]
    fn list_item_path_is_indexed() {
        let node = VisitorOuter {
            list: VisitorLowerTextList::from(vec!["lower".to_string(), "NotLower".to_string()]),
            rec: VisitorInner {
                leaf: VisitorLowerText::from("lower"),
            },
            tup: VisitorLowerTextTuple(
                VisitorLowerText::from("lower"),
                VisitorLowerText::from("lower"),
            ),
            map: VisitorLowerTextMap::from(vec![("key".to_string(), "lower".to_string())]),
        };

        let flat = flatten_errs(&node);
        assert!(flat.iter().any(|(k, _)| k == "list[1]"));
    }

    #[test]
    fn nested_record_tuple_map_paths_are_dotted() {
        let node = VisitorOuter {
            list: VisitorLowerTextList::from(vec!["lower".to_string()]),
            rec: VisitorInner {
                leaf: VisitorLowerText::from("NotLower"),
            },
            tup: VisitorLowerTextTuple(
                VisitorLowerText::from("lower"),
                VisitorLowerText::from("NotLower"),
            ),
            map: VisitorLowerTextMap::from(vec![("key".to_string(), "NotLower".to_string())]),
        };

        let flat = flatten_errs(&node);

        assert!(flat.iter().any(|(k, _)| k == "rec.leaf"));
        assert!(flat.iter().any(|(k, _)| k == "tup.1"));
        assert!(flat.iter().any(|(k, _)| k == "map[0]"));
    }
}
