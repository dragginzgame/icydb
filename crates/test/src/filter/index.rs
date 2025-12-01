use icydb::{core::traits::FieldValue, db::query::QueryPlan, prelude::*};
use test_design::e2e::filter::FilterableIndex;

///
/// IndexFilterSuite
///

pub struct IndexFilterSuite {}

impl IndexFilterSuite {
    pub fn test() {
        let tests: Vec<(&str, fn())> = vec![
            // existing
            ("filter_eq_name_alpha", Self::filter_eq_name_alpha),
            ("filter_eq_name_zeta", Self::filter_eq_name_zeta),
            ("filter_ne_name_alpha", Self::filter_ne_name_alpha),
            ("filter_eq_name_missing", Self::filter_eq_name_missing),
            (
                "filter_eq_name_case_sensitive",
                Self::filter_eq_name_case_sensitive,
            ),
            // NEW — Option<Text> tests
            ("filter_is_null_name_opt", Self::filter_is_null_name_opt),
            (
                "filter_is_not_null_name_opt",
                Self::filter_is_not_null_name_opt,
            ),
            ("filter_eq_name_opt_beta", Self::filter_eq_name_opt_beta),
            ("filter_eq_name_opt_alpha", Self::filter_eq_name_opt_alpha),
            // NEW — Vec<Text> membership tests
            (
                "filter_contains_name_many_alpha",
                Self::filter_contains_name_many_alpha,
            ),
            (
                "filter_contains_name_many_blue",
                Self::filter_contains_name_many_blue,
            ),
            (
                "filter_contains_name_many_missing",
                Self::filter_contains_name_many_missing,
            ),
            (
                "filter_contains_both_blue_and_green",
                Self::filter_contains_both_blue_and_green,
            ),
            ("filter_gt_name_delta", Self::filter_gt_name_delta),
            ("filter_le_name_gamma", Self::filter_le_name_gamma),
            ("filter_between_delta_iota", Self::filter_between_delta_iota),
            (
                "filter_compound_alpha_opt_and_many",
                Self::filter_compound_alpha_opt_and_many,
            ),
            (
                "filter_name_opt_null_and_name_has_a",
                Self::filter_name_opt_null_and_name_has_a,
            ),
        ];

        // insert data
        Self::insert();

        for (name, test_fn) in tests {
            println!("Running test: {name}");
            test_fn();
        }
    }

    // insert
    fn insert() {
        // (name, name_opt, name_many)
        let fixtures: [(&str, Option<&str>, &[&str]); 10] = [
            ("Alpha", None, &["alpha", "blue"]),
            ("Beta", Some("Beta"), &[]),
            ("Delta", None, &[]),
            ("Epsilon", Some("Epsilon"), &["green", "blue"]),
            ("Eta", Some("Beta"), &["blue"]),
            ("Gamma", Some("Alpha"), &["red", "alpha"]),
            ("Iota", Some("Alpha"), &["alpha"]),
            ("Kappa", None, &["green", "blue"]),
            ("Theta", None, &["yellow"]),
            ("Zeta", None, &[]),
        ];

        for (name, name_opt, name_many) in fixtures {
            db!()
                .insert(FilterableIndex {
                    name: name.into(),
                    name_opt: name_opt.map(ToString::to_string),
                    name_many: name_many.iter().map(ToString::to_string).collect(),
                    ..Default::default()
                })
                .unwrap();
        }
    }

    ///
    /// NORMAL (FilterableIndex)
    ///

    /// name == "Alpha" -> exactly one row
    fn filter_eq_name_alpha() {
        let query = db::query::load().filter(|f| f.eq("name", "Alpha"));

        // explain plan
        let plan = db!()
            .load::<FilterableIndex>()
            .explain(query.clone())
            .unwrap();
        println!("Plan for filter_eq_name_alpha: {plan}");

        match plan {
            QueryPlan::Index(p) => {
                assert_eq!(p.index.fields, &["name"]);
                assert_eq!(p.values.len(), 1);
                assert_eq!(p.values[0], "Alpha".to_value());
            }
            _ => panic!("expected Index plan, got {plan:?}"),
        }

        // then execute
        let results = db!()
            .load::<FilterableIndex>()
            .execute(query)
            .unwrap()
            .entities();

        assert!(results.iter().all(|e| e.name == "Alpha"));
        assert_eq!(results.len(), 1);
    }

    /// name == "Zeta" -> exactly one row
    fn filter_eq_name_zeta() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.eq("name", "Zeta"))
            .unwrap()
            .entities();

        assert!(results.iter().all(|e| e.name == "Zeta"));
        assert_eq!(results.len(), 1);
    }

    /// name != "Alpha" -> 9 rows (everything except Alpha)
    fn filter_ne_name_alpha() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.ne("name", "Alpha"))
            .unwrap()
            .entities();

        assert!(results.iter().all(|e| e.name != "Alpha"));
        assert_eq!(results.len(), 9);
    }

    /// name == "Nope" -> 0 rows
    fn filter_eq_name_missing() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.eq("name", "Nope"))
            .unwrap()
            .entities();

        assert!(results.is_empty());
    }

    /// Case-sensitivity: assuming raw byte collation (no normalization),
    /// "alpha" should not match "Alpha".
    fn filter_eq_name_case_sensitive() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.eq("name", "alpha"))
            .unwrap()
            .entities();

        assert!(results.is_empty(), "expected case-sensitive match for name");
    }

    // ---------- Option<Text> tests ----------

    // Expect 5 rows: Alpha, Delta, Zeta, Theta, Kappa
    fn filter_is_null_name_opt() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.is_none("name_opt"))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 5);
        assert!(results.iter().all(|e| e.name_opt.is_none()));
    }

    // Complement of the above: 5 rows with Some(_)
    fn filter_is_not_null_name_opt() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.is_some("name_opt"))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 5);
        assert!(results.iter().all(|e| e.name_opt.is_some()));
    }

    // Two rows have Some("Beta"): Beta, Eta
    fn filter_eq_name_opt_beta() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.eq("name_opt", "Beta"))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 2);
        assert!(
            results
                .iter()
                .all(|e| e.name_opt.as_deref() == Some("Beta"))
        );
    }

    // Two rows have Some("Alpha"): Gamma, Iota
    fn filter_eq_name_opt_alpha() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.eq("name_opt", "Alpha"))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 2);
        assert!(
            results
                .iter()
                .all(|e| e.name_opt.as_deref() == Some("Alpha"))
        );
    }

    // ---------- Vec<Text> membership tests ----------

    // "alpha" appears in Alpha, Gamma, Iota
    fn filter_contains_name_many_alpha() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.contains("name_many", "alpha"))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 3);
        assert!(
            results
                .iter()
                .all(|e| e.name_many.iter().any(|t| t == "alpha"))
        );
    }

    // "blue" appears in Alpha, Epsilon, Eta, Kappa
    fn filter_contains_name_many_blue() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.contains("name_many", "blue"))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 4);
        assert!(
            results
                .iter()
                .all(|e| e.name_many.iter().any(|t| t == "blue"))
        );
    }

    // Missing token yields 0 results
    fn filter_contains_name_many_missing() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.contains("name_many", "zzzz"))
            .unwrap()
            .entities();

        assert!(results.is_empty());
    }

    // Require both "blue" AND "green": expect Epsilon and Kappa
    fn filter_contains_both_blue_and_green() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.contains("name_many", "blue") & f.contains("name_many", "green"))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|e| {
            e.name_many.iter().any(|t| t == "blue") && e.name_many.iter().any(|t| t == "green")
        }));
    }

    // ---------- Range & Ordering tests ----------

    // All names > "Delta" (lex order)
    fn filter_gt_name_delta() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.gt("name", "Delta"))
            .unwrap()
            .entities();

        assert!(results.iter().all(|e| *e.name > *"Delta"));
        assert_eq!(results.len(), 7);
    }

    // All names <= "Delta" => Alpha, Beta, Delta
    fn filter_le_name_gamma() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.lte("name", "Delta"))
            .unwrap()
            .entities();

        assert!(results.iter().all(|e| *e.name <= *"Delta"));
        assert_eq!(results.len(), 3);
    }

    // Names between Delta and Iota inclusive
    fn filter_between_delta_iota() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.gte("name", "Delta") & f.lte("name", "Iota"))
            .unwrap()
            .entities();

        assert!(
            results
                .iter()
                .all(|e| *e.name >= *"Delta" && *e.name <= *"Iota")
        );
        assert_eq!(results.len(), 5); // Delta, Epsilon, Eta, Gamma, Iota
    }

    // ---------- Compound filter tests ----------

    // (name_opt = "Alpha") AND (name_many contains "alpha") => Gamma, Iota
    fn filter_compound_alpha_opt_and_many() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.eq("name_opt", "Alpha") & f.contains("name_many", "alpha"))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|e| {
            e.name_opt.as_deref() == Some("Alpha") && e.name_many.iter().any(|t| t == "alpha")
        }));
    }

    // ---------- Null + filter combinations ----------

    // name_opt is None AND name contains "a"
    fn filter_name_opt_null_and_name_has_a() {
        let results = db!()
            .load::<FilterableIndex>()
            .filter(|f| f.is_none("name_opt") & f.contains("name", "a"))
            .unwrap()
            .entities();

        assert!(
            results
                .iter()
                .all(|e| e.name_opt.is_none() && e.name.contains('a'))
        );
    }
}
