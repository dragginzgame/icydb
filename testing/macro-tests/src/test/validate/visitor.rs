pub use icydb_testing_test_fixtures::macro_test::validate::visitor::*;

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::visitor::validate;

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

        let err = validate(&node).expect_err("expected validation error");
        let issues = err.issues();

        for key in ["list[1]", "tup.0", "map[0]", "rec.leaf"] {
            assert!(
                issues.contains_key(key),
                "expected error issues to include `{key}`"
            );
        }
        assert_eq!(issues.len(), 4);
    }

    #[test]
    fn validate_tracks_paths_for_set_item_validators() {
        let node = VisitorSetOuter {
            set: VisitorLowerTextSetValidated::from(vec!["MiXeD".to_string()]),
        };

        let err = validate(&node).expect_err("expected validation error");
        let issues = err.issues();

        let key = "set[0]";
        assert!(
            issues.contains_key(key),
            "expected error issues to include `{key}`"
        );
        assert_eq!(issues.len(), 1);
    }

    #[test]
    fn validate_tracks_paths_for_map_key_validators() {
        let node = VisitorMapKeyOuter {
            map: VisitorLowerTextKeyMapValidated::from(vec![(
                "MiXeD".to_string(),
                "lower".to_string(),
            )]),
        };

        let err = validate(&node).expect_err("expected validation error");
        let issues = err.issues();

        let key = "map[0]";
        assert!(
            issues.contains_key(key),
            "expected error issues to include `{key}`"
        );
        assert_eq!(issues.len(), 1);
    }

    #[test]
    fn validate_tracks_paths_for_map_value_validators() {
        let node = VisitorMapValueOuter {
            map: VisitorLowerTextValueMapValidated::from(vec![(
                "lower".to_string(),
                "MiXeD".to_string(),
            )]),
        };

        let err = validate(&node).expect_err("expected validation error");
        let issues = err.issues();

        let key = "map[0]";
        assert!(
            issues.contains_key(key),
            "expected error issues to include `{key}`"
        );
        assert_eq!(issues.len(), 1);
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

        let err = validate(&node).expect_err("expected validation error");
        let issues = err.issues();

        for key in ["list", "set", "map"] {
            assert!(
                issues.contains_key(key),
                "expected error issues to include `{key}`"
            );
        }
        assert_eq!(issues.len(), 3);
    }
}
