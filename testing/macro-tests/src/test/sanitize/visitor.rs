#[cfg(test)]
use crate::prelude::*;

pub use icydb_testing_test_fixtures::macro_test::sanitize::visitor::*;

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::visitor::sanitize;
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
        let actual_list: Vec<_> = node
            .list
            .iter()
            .map(|value| value.inner().clone())
            .collect();
        assert_eq!(actual_list, expected_list);
        assert_eq!(node.tup.0.inner().as_str(), "mixed");
        assert_eq!(node.tup.1.inner().as_str(), "another");

        let actual_map: HashMap<_, _> = node
            .map
            .iter()
            .map(|(k, v)| (k.clone(), v.inner().clone()))
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

        let err = sanitize(&mut node).expect_err("expected sanitization error");
        let issues = err.issues();

        for key in ["field", "list[0]", "list[1]"] {
            assert!(
                issues.contains_key(key),
                "expected error issues to include path `{key}`"
            );
        }

        assert_eq!(issues.len(), 3);
    }

    #[test]
    fn sanitize_tracks_paths_for_map_value_sanitizers() {
        let mut node = VisitorRejectMapOuter {
            map: VisitorRejectTextMap::from(vec![("key".to_string(), "bad".to_string())]),
        };

        let err = sanitize(&mut node).expect_err("expected sanitization error");
        let issues = err.issues();

        let key = "map[0]";
        assert!(
            issues.contains_key(key),
            "expected error issues to include path `{key}`"
        );
        assert_eq!(issues.len(), 1);
    }
}
