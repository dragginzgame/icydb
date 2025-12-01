use icydb::{
    core::{
        Value,
        types::{Decimal, Principal},
    },
    prelude::*,
};
use test_design::e2e::filter::{Filterable, FilterableOpt};

pub struct DeleteFilterSuite {}

impl DeleteFilterSuite {
    pub fn test() {
        let tests: Vec<(&str, fn())> = vec![
            ("delete_eq_category_a", Self::delete_eq_category_a),
            ("delete_contains_tag_green", Self::delete_contains_tag_green),
            ("delete_in_category_a_or_c", Self::delete_in_category_a_or_c),
            ("delete_eq_principal_1", Self::delete_eq_principal_1),
            // optional fields
            ("delete_opt_name_is_none", Self::delete_opt_name_is_none),
            ("delete_opt_eq_name_alice", Self::delete_opt_eq_name_alice),
        ];

        for (name, t) in tests {
            Self::reset_and_insert_all();

            println!("Running delete test: {name}");
            t();
        }
    }

    // --- Helpers -------------------------------------------------------------

    fn reset_and_insert_all() {
        // Clear the store fully so each test is isolated
        crate::clear_test_data_store();

        Self::insert_filterable();
        Self::insert_filterable_opt();

        let count = db!().load::<Filterable>().all().unwrap().count();
        assert_eq!(count, 10);
    }

    fn insert_filterable() {
        let fixtures = [
            ("Alpha", "A", true, 87.2, 1, -10, vec!["red", "blue"], 1),
            ("Beta", "B", false, 65.1, 2, 0, vec!["green"], 2),
            ("Gamma", "C", true, 92.5, 3, 10, vec!["red", "yellow"], 3),
            ("Delta", "B", false, 15.3, 2, 5, vec![], 4),
            ("Epsilon", "A", true, 75.0, 4, -5, vec!["green", "blue"], 5),
            ("Zeta", "C", false, 88.8, 5, 15, vec!["purple"], 6),
            ("Eta", "B", true, 30.5, 1, 8, vec!["red"], 7),
            ("Theta", "A", true, 99.9, 6, -20, vec!["blue", "green"], 8),
            ("Iota", "C", false, 42.0, 3, 0, vec!["yellow", "red"], 9),
            ("Kappa", "B", true, 50.0, 2, 3, vec!["green", "blue"], 10),
        ];

        for (name, category, active, score, level, offset, tags, pid_index) in fixtures {
            db!()
                .replace(Filterable {
                    name: name.into(),
                    category: category.into(),
                    active,
                    score: Decimal::from(score),
                    level,
                    offset,
                    tags: tags.iter().map(ToString::to_string).collect(),
                    pid: Principal::dummy(pid_index),
                    ..Default::default()
                })
                .unwrap();
        }
    }

    fn insert_filterable_opt() {
        let fixtures = [
            (Some("Alice"), Some(1), Some(-10), Some(Principal::dummy(1))),
            (Some("Bob"), Some(2), Some(0), Some(Principal::dummy(2))),
            (None, Some(3), None, Some(Principal::dummy(3))),
            (Some("Charlie"), None, Some(5), None),
            (None, None, None, None),
        ];

        for (name, level, offset, pid) in fixtures {
            db!()
                .replace(FilterableOpt {
                    name: name.map(str::to_string),
                    level,
                    offset,
                    pid,
                    ..Default::default()
                })
                .unwrap();
        }
    }

    fn remaining_count_filterable() -> u32 {
        db!().load::<Filterable>().all().unwrap().count()
    }

    fn remaining_count_filterable_opt() -> u32 {
        db!().load::<FilterableOpt>().all().unwrap().count()
    }

    // --- Tests: Filterable ---------------------------------------------------

    // delete where category == "A" (expect 3 deleted, 7 remain)
    fn delete_eq_category_a() {
        let deleted = db!()
            .delete::<Filterable>()
            .filter(|f| f.eq("category", "A"))
            .unwrap();

        assert_eq!(deleted.count(), 3, "expected to delete 3 A-category rows");

        let remaining = Self::remaining_count_filterable();
        assert_eq!(remaining, 7, "expected 7 remaining after deleting A");

        // sanity: none of the remaining should be category A
        let rest = db!()
            .load::<Filterable>()
            .filter(|f| f.eq("category", "A"))
            .unwrap()
            .entities();

        assert!(rest.is_empty(), "no A rows should remain");
    }

    // delete where tags CONTAINS "green" (expect 4 deleted, 6 remain)
    fn delete_contains_tag_green() {
        let deleted = db!()
            .delete::<Filterable>()
            .filter(|f| f.contains("tags", "green"))
            .unwrap();

        assert_eq!(
            deleted.count(),
            4,
            "expected to delete 4 rows with green tag"
        );

        let remaining = Self::remaining_count_filterable();
        assert_eq!(remaining, 6);
    }

    // delete where category IN ["A", "C"] (expect 6 deleted, 4 remain)
    fn delete_in_category_a_or_c() {
        let deleted = db!()
            .delete::<Filterable>()
            .filter(|f| f.in_iter("category", ["A", "C"]))
            .unwrap();

        assert_eq!(deleted.count(), 6, "expected to delete 6 rows (A or C)");

        let remaining = Self::remaining_count_filterable();
        assert_eq!(remaining, 4);
    }

    // delete where pid == dummy(1) (expect 1 deleted, 9 remain)
    fn delete_eq_principal_1() {
        let expected = Principal::dummy(1);
        let deleted = db!()
            .delete::<Filterable>()
            .filter(|f| f.eq("pid", expected))
            .unwrap();

        assert_eq!(
            deleted.count(),
            1,
            "expected to delete exactly one row by pid"
        );

        let remaining = Self::remaining_count_filterable();
        assert_eq!(remaining, 9);
    }

    // --- Tests: FilterableOpt (Option fields) --------------------------------

    // delete where name == None (expect 2 deleted, 3 remain)
    fn delete_opt_name_is_none() {
        let deleted = db!()
            .delete::<FilterableOpt>()
            .filter(|f| f.eq("name", Value::None))
            .unwrap();

        assert_eq!(
            deleted.count(),
            2,
            "expected to delete 2 rows with name=None"
        );
        let remaining = Self::remaining_count_filterable_opt();
        assert_eq!(remaining, 3);

        // sanity: ensure no remaining have name == None
        let rest = db!()
            .load::<FilterableOpt>()
            .filter(|f| f.eq("name", Value::None))
            .unwrap()
            .entities();

        assert!(rest.is_empty());
    }

    // delete where name == "Alice" (expect 1 deleted, 4 remain)
    fn delete_opt_eq_name_alice() {
        let deleted = db!()
            .delete::<FilterableOpt>()
            .filter(|f| f.eq("name", "Alice"))
            .unwrap();

        assert_eq!(deleted.count(), 1, "expected to delete Alice only");
        let remaining = Self::remaining_count_filterable_opt();
        assert_eq!(remaining, 4);

        // sanity: ensure Alice is gone
        let rest = db!()
            .load::<FilterableOpt>()
            .filter(|f| f.eq("name", "Alice"))
            .unwrap()
            .entities();

        assert!(rest.is_empty());
    }
}
