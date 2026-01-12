use icydb::{db::primitives::*, design::prelude::*, view::Filter};
use test_design::e2e::filter::{Filterable, FilterableEnum, FilterableEnumFake, FilterableOpt};

use super::fixtures;

///
/// LoadFilterSuite
///

pub struct LoadFilterSuite {}

impl LoadFilterSuite {
    #[allow(clippy::too_many_lines)]
    pub fn test() {
        let tests: Vec<(&str, fn())> = vec![
            ("filter_eq_string", Self::filter_eq_string),
            ("filter_eq_bool", Self::filter_eq_bool),
            ("group_count_by_category", Self::group_count_by_category),
            (
                "group_count_by_filtered_active",
                Self::group_count_by_filtered_active,
            ),
            ("filter_gt_score", Self::filter_gt_score),
            ("filter_le_level", Self::filter_le_level),
            ("filter_ne_category", Self::filter_ne_category),
            ("filter_and_group", Self::filter_and_group),
            ("filter_or_group", Self::filter_or_group),
            ("filter_nested_groups", Self::filter_nested_groups),
            ("filter_startswith_name", Self::filter_startswith_name),
            ("filter_not_clause", Self::filter_not_clause),
            ("filter_always", Self::filter_always),
            ("filter_never", Self::filter_never),
            ("filter_empty_result", Self::filter_empty_result),
            ("filter_in_category", Self::filter_in_category),
            ("filter_allin_tags", Self::filter_allin_tags),
            ("filter_anyin_tags", Self::filter_anyin_tags),
            ("filter_allin_ci_tags", Self::filter_allin_ci_tags),
            ("filter_anyin_ci_tags", Self::filter_anyin_ci_tags),
            (
                "filter_anyin_tags_no_match",
                Self::filter_anyin_tags_no_match,
            ),
            (
                "filter_allin_tags_no_match",
                Self::filter_allin_tags_no_match,
            ),
            (
                "filter_anyin_tags_with_duplicates",
                Self::filter_anyin_tags_with_duplicates,
            ),
            ("filter_eq_principal", Self::filter_eq_principal),
            ("filter_contains_tag", Self::filter_contains_tag),
            // opt
            ("filter_opt_eq_name", Self::filter_opt_eq_name),
            ("filter_opt_lt_level", Self::filter_opt_lt_level),
            ("filter_opt_is_none_name", Self::filter_opt_is_none_name),
            ("filter_opt_ne_pid_null", Self::filter_opt_ne_pid_null),
            ("filter_opt_rel_eq_some", Self::filter_opt_rel_eq_some),
            ("filter_opt_rel_eq_none", Self::filter_opt_rel_eq_none),
            ("filter_opt_rel_ne_none", Self::filter_opt_rel_ne_none),
            ("filter_opt_rel_is_some", Self::filter_opt_rel_is_some),
            ("filter_opt_rel_is_none", Self::filter_opt_rel_is_none),
            ("filter_opt_rel_in_list", Self::filter_opt_rel_in_list),
            (
                "filter_opt_rel_not_in_list",
                Self::filter_opt_rel_not_in_list,
            ),
            (
                "filter_opt_rel_eq_ulid_vs_text_mismatch",
                Self::filter_opt_rel_eq_ulid_vs_text_mismatch,
            ),
            // enum
            ("filter_eq_enum", Self::filter_eq_enum),
            ("filter_eq_enum_fake", Self::filter_eq_enum_fake),
            // invalid comparator/value combos (validation errors)
            (
                "invalid_startswith_rhs_non_text",
                Self::invalid_startswith_rhs_non_text,
            ),
            (
                "invalid_eq_ci_rhs_non_text",
                Self::invalid_eq_ci_rhs_non_text,
            ),
            // previously invalid: AnyInCi rejected non-text RHS; now allowed
            (
                "any_in_ci_list_of_ulids_matches",
                Self::any_in_ci_list_of_ulids_matches,
            ),
            (
                "any_in_ci_list_non_text_still_validates",
                Self::any_in_ci_list_non_text_still_validates,
            ),
            (
                "invalid_presence_rhs_non_unit",
                Self::invalid_presence_rhs_non_unit,
            ),
            (
                "filter_builder_eq_category",
                Self::filter_builder_eq_category,
            ),
            ("filter_builder_gt_score", Self::filter_builder_gt_score),
            (
                "filter_builder_name_starts_and_ends",
                Self::filter_builder_name_starts_and_ends,
            ),
            (
                "filter_builder_category_not_equal_ci",
                Self::filter_builder_category_not_equal_ci,
            ),
            (
                "filter_builder_name_contains_ci",
                Self::filter_builder_name_contains_ci,
            ),
            (
                "filter_builder_score_between",
                Self::filter_builder_score_between,
            ),
            (
                "filter_builder_offset_negative",
                Self::filter_builder_offset_negative,
            ),
            (
                "filter_builder_category_ci_level",
                Self::filter_builder_category_ci_level,
            ),
            (
                "filter_builder_opt_name_present",
                Self::filter_builder_opt_name_present,
            ),
            (
                "filter_eq_principal_string_rhs",
                Self::filter_eq_principal_string_rhs,
            ),
            (
                "filter_eq_account_string_rhs",
                Self::filter_eq_account_string_rhs,
            ),
        ];

        // insert data
        fixtures::seed_filter_data();

        for (name, test_fn) in tests {
            println!("Running test: {name}");
            test_fn();
        }
    }

    ///
    /// NORMAL (Filterable)
    ///

    fn filter_eq_string() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.eq("category", "A"))
            .unwrap()
            .entities();

        assert!(results.iter().all(|e| e.category == "A"));
        assert_eq!(results.len(), 3);
    }

    fn filter_eq_bool() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.eq("active", true))
            .unwrap()
            .entities();

        assert!(results.iter().all(|e| e.active));
        assert_eq!(results.len(), 6);
    }

    fn group_count_by_category() {
        let counts = db!()
            .load::<Filterable>()
            .group_count_by(db::query::load(), |e| e.category.clone())
            .unwrap();

        assert_eq!(counts.len(), 3);
        assert_eq!(counts.get("A").copied(), Some(3));
        assert_eq!(counts.get("B").copied(), Some(4));
        assert_eq!(counts.get("C").copied(), Some(3));
    }

    fn group_count_by_filtered_active() {
        let query = db::query::load().filter(|f| f.eq("category", "B"));
        let counts = db!()
            .load::<Filterable>()
            .group_count_by(query, |e| e.active)
            .unwrap();

        assert_eq!(counts.len(), 2);
        assert_eq!(counts.get(&true).copied(), Some(2));
        assert_eq!(counts.get(&false).copied(), Some(2));
    }

    fn filter_gt_score() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.gt("score", Decimal::from(80.0)))
            .unwrap()
            .entities();

        assert!(results.iter().all(|e| e.score > Decimal::from(80.0)));
        assert_eq!(results.len(), 4);
    }

    fn filter_le_level() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.lte("level", 3))
            .unwrap()
            .entities();

        assert!(results.iter().all(|e| e.level <= 3));
        assert_eq!(results.len(), 7);
    }

    fn filter_ne_category() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.ne("category", "B"))
            .unwrap()
            .entities();

        assert!(results.iter().all(|e| e.category != "B"));
        assert_eq!(results.len(), 6);
    }

    fn filter_and_group() {
        let query =
            db::query::load().filter(|f| f.gte("score", Decimal::from(60.0)) & f.gte("level", 2));

        let results = db!()
            .load::<Filterable>()
            .execute(query)
            .unwrap()
            .entities();

        assert!(
            results
                .iter()
                .all(|e| e.score >= Decimal::from(60.0) && e.level >= 2)
        );
        assert_eq!(results.len(), 5);
    }

    fn filter_or_group() {
        let query = db::query::load().filter(|f| f.eq("category", "A") | f.eq("category", "C"));

        let results = db!()
            .load::<Filterable>()
            .execute(query)
            .unwrap()
            .entities();

        assert!(
            results
                .iter()
                .all(|e| e.category == "A" || e.category == "C")
        );
        assert_eq!(results.len(), 6);
    }

    fn filter_nested_groups() {
        let query = db::query::load().filter(|f| {
            f.eq("active", true) | f.lt("score", Decimal::from(40.0)) | f.lt("offset", 0)
        });

        let results = db!()
            .load::<Filterable>()
            .execute(query)
            .unwrap()
            .entities();

        assert_eq!(results.len(), 7);
    }

    fn filter_startswith_name() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.starts_with("name", "A"))
            .unwrap()
            .entities();

        assert!(results.iter().all(|e| e.name.starts_with('A')));
        assert_eq!(results.len(), 1);
    }

    fn filter_not_clause() {
        let query = db::query::load().filter(|f| f.ne("category", "C"));

        let results = db!()
            .load::<Filterable>()
            .execute(query)
            .unwrap()
            .entities();

        assert!(results.iter().all(|e| e.category != "C"));
        assert_eq!(results.len(), 7);
    }

    fn filter_always() {
        let results = db!()
            .load::<Filterable>()
            .filter(FilterDsl::always)
            .unwrap()
            .entities();

        assert_eq!(results.len(), 10); // all fixtures
    }

    fn filter_never() {
        let results = db!()
            .load::<Filterable>()
            .filter(FilterDsl::never)
            .unwrap()
            .entities();

        assert_eq!(results.len(), 0);
    }

    fn filter_empty_result() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.eq("category", "Nonexistent"))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 0);
    }

    fn filter_in_category() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.in_iter("category", ["A", "C"]))
            .unwrap()
            .entities();

        assert!(
            !results.is_empty(),
            "Expected results for IN filter, got none"
        );

        assert!(
            results
                .iter()
                .all(|e| e.category == "A" || e.category == "C")
        );
        assert_eq!(results.len(), 6);
    }

    fn filter_allin_tags() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.all_in("tags", ["blue", "green"]))
            .unwrap()
            .entities();

        assert!(
            !results.is_empty(),
            "Expected results for ALL IN filter, got none"
        );

        assert!(results.iter().all(|e| {
            e.tags.contains(&"blue".to_string()) && e.tags.contains(&"green".to_string())
        }));

        assert_eq!(results.len(), 3);
    }

    fn filter_anyin_tags() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.any_in("tags", ["blue", "green"]))
            .unwrap()
            .entities();

        assert!(
            !results.is_empty(),
            "Expected results for ANY IN filter, got none"
        );

        for e in &results {
            assert!(
                e.tags.contains(&"blue".to_string()) || e.tags.contains(&"green".to_string()),
                "Entity {:?} did not match ANY IN condition",
                e.name
            );
        }

        assert_eq!(results.len(), 5);
    }

    fn filter_anyin_tags_no_match() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.any_in("tags", ["orange", "black"]))
            .unwrap()
            .entities();

        assert!(
            results.is_empty(),
            "Expected no results for ANY IN no-match"
        );
    }

    fn filter_allin_tags_no_match() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.all_in("tags", ["blue", "black"]))
            .unwrap()
            .entities();

        assert!(
            results.is_empty(),
            "Expected no results for ALL IN no-match"
        );
    }

    fn filter_anyin_tags_with_duplicates() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.any_in("tags", ["blue", "blue", "green"]))
            .unwrap()
            .entities();

        assert!(
            !results.is_empty(),
            "Expected results for ANY IN with duplicates, got none"
        );

        assert!(
            results
                .iter()
                .all(|e| e.tags.contains(&"blue".to_string())
                    || e.tags.contains(&"green".to_string()))
        );
    }

    fn filter_allin_ci_tags() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.all_in_ci("tags", ["BLUE", "Green"]))
            .unwrap()
            .entities();

        assert!(
            !results.is_empty(),
            "Expected results for ALL IN CI filter, got none"
        );

        assert!(results.iter().all(|e| {
            e.tags.iter().any(|t| t.eq_ignore_ascii_case("blue"))
                && e.tags.iter().any(|t| t.eq_ignore_ascii_case("green"))
        }));
    }

    fn filter_anyin_ci_tags() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.any_in_ci("tags", ["BLUE", "Green"]))
            .unwrap()
            .entities();

        assert!(
            !results.is_empty(),
            "Expected results for ANY IN CI filter, got none"
        );

        assert!(results.iter().all(|e| {
            e.tags.iter().any(|t| t.eq_ignore_ascii_case("blue"))
                || e.tags.iter().any(|t| t.eq_ignore_ascii_case("green"))
        }));
    }

    fn filter_eq_principal() {
        // Use dummy principal that matches the one used in fixtures
        let expected = Principal::dummy(1);

        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.eq("pid", expected))
            .unwrap()
            .entities();

        assert!(
            !results.is_empty(),
            "Expected at least one entity with matching principal"
        );
        assert!(
            results.iter().all(|e| e.pid == expected),
            "All results should have matching principal"
        );
        assert_eq!(results.len(), 1);
    }

    fn filter_eq_principal_string_rhs() {
        let expected = Principal::dummy(1);
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.eq("pid", expected.to_string()))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 1);
        assert!(results.iter().all(|e| e.pid == expected));
    }

    fn filter_eq_account_string_rhs() {
        // use dummy account based on principal/subaccount
        let account = Account::dummy(1);

        // add a filterable fixture with pid principal == account.owner
        // The fixtures already include Principal::dummy(1) on Alpha row, so reuse that
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.eq("pid", account.to_string()))
            .unwrap()
            .entities();

        // account string encodes owner+subaccount; we expect no matches for pid field
        assert!(results.is_empty());
    }

    fn filter_contains_tag() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.contains("tags", "green"))
            .unwrap()
            .entities();

        assert!(
            results
                .iter()
                .all(|e| e.tags.contains(&"green".to_string()))
        );
        assert_eq!(results.len(), 4);
    }

    ///
    /// OPTIONAL (FilterableOpt)
    ///

    fn rel_id_for(name: &str) -> Ulid {
        db!()
            .load::<Filterable>()
            .filter(|f| f.eq("name", name))
            .unwrap()
            .entities()
            .first()
            .map_or_else(|| panic!("missing relation '{name}'"), |e| e.id)
    }

    fn filter_opt_eq_name() {
        let results = db!()
            .load::<FilterableOpt>()
            .filter(|f| f.eq("name", "Alice"))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_deref(), Some("Alice"));
    }

    fn filter_opt_is_none_name() {
        let results = db!()
            .load::<FilterableOpt>()
            .filter(|f| f.eq("name", Value::None))
            .unwrap()
            .entities();

        assert!(results.iter().all(|e| e.name.is_none()));
        assert_eq!(results.len(), 2); // Charlie and None
    }

    fn filter_opt_lt_level() {
        let results = db!()
            .load::<FilterableOpt>()
            .filter(|f| f.lt("level", 3))
            .unwrap()
            .entities();

        assert!(results.iter().all(|e| e.level.unwrap_or(255) < 3));
        assert_eq!(results.len(), 2); // Alice (1), Bob (2)
    }

    fn filter_opt_ne_pid_null() {
        let results = db!()
            .load::<FilterableOpt>()
            .filter(|f| f.ne("pid", Value::None))
            .unwrap()
            .entities();

        assert!(results.iter().all(|e| e.pid.is_some()));
        assert_eq!(results.len(), 3);
    }

    fn filter_opt_rel_eq_some() {
        let alpha = Self::rel_id_for("Alpha");

        let results = db!()
            .load::<FilterableOpt>()
            .filter(|f| f.eq("rel_id", alpha))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|e| e.rel_id == Some(alpha)));
    }

    fn filter_opt_rel_eq_none() {
        let results = db!()
            .load::<FilterableOpt>()
            .filter(|f| f.eq("rel_id", Value::None))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|e| e.rel_id.is_none()));
    }

    fn filter_opt_rel_ne_none() {
        let results = db!()
            .load::<FilterableOpt>()
            .filter(|f| f.ne("rel_id", Value::None))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|e| e.rel_id.is_some()));
    }

    fn filter_opt_rel_is_some() {
        let results = db!()
            .load::<FilterableOpt>()
            .filter(|f| f.is_some("rel_id"))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|e| e.rel_id.is_some()));
    }

    fn filter_opt_rel_is_none() {
        let results = db!()
            .load::<FilterableOpt>()
            .filter(|f| f.is_none("rel_id"))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|e| e.rel_id.is_none()));
    }

    fn filter_opt_rel_in_list() {
        let alpha = Self::rel_id_for("Alpha");
        let beta = Self::rel_id_for("Beta");

        let results = db!()
            .load::<FilterableOpt>()
            .filter(|f| f.in_iter("rel_id", [alpha, beta]))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 3);
        assert!(
            results
                .iter()
                .all(|e| matches!(e.rel_id, Some(id) if id == alpha || id == beta))
        );
    }

    fn filter_opt_rel_not_in_list() {
        let alpha = Self::rel_id_for("Alpha");

        let results = db!()
            .load::<FilterableOpt>()
            .filter(|f| f.not_in_iter("rel_id", [alpha]))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|e| e.rel_id != Some(alpha)));
    }

    fn filter_opt_rel_eq_ulid_vs_text_mismatch() {
        // RHS mimics the dfx bug: ULID rendered as Text instead of Ulid.
        let alpha = Self::rel_id_for("Alpha");
        let bad_rhs = Value::Text(alpha.to_string());

        let expr = FilterExpr::Clause(FilterClause::new("rel_id", Cmp::Eq, bad_rhs.clone()));
        let query = db::query::load().filter(|_| expr);

        let results = db!()
            .load::<FilterableOpt>()
            .execute(query)
            .unwrap()
            .entities();

        assert_eq!(results.len(), 2, "Text RHS should coerce to Ulid");
        assert!(results.iter().all(|e| e.rel_id == Some(alpha)));

        // Direct Value comparison remains strict (different variants are not equal).
        assert_ne!(Value::Ulid(alpha), bad_rhs);
    }

    // --------------------------- enum ---------------------

    fn filter_eq_enum() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.eq("abc", FilterableEnum::A))
            .unwrap()
            .entities();

        assert!(results.iter().all(|e| e.abc == FilterableEnum::A));
        assert_eq!(results.len(), 3);
    }

    fn filter_eq_enum_fake() {
        let results = db!()
            .load::<Filterable>()
            .filter(|f| f.eq("abc", FilterableEnumFake::A))
            .unwrap()
            .entities();

        assert_eq!(results.len(), 0);
    }

    // -------------------- invalid comparator/value validation ---------------------

    fn invalid_startswith_rhs_non_text() {
        // name is Text; starts_with requires Text RHS
        let res = db!()
            .load::<Filterable>()
            .filter(|f| f.starts_with("name", 1));
        assert!(
            res.is_err(),
            "Expected validation error for starts_with(name, 1)"
        );
    }

    fn invalid_eq_ci_rhs_non_text() {
        // level is numeric; eq_ci requires Text RHS (by comparator family)
        let res = db!().load::<Filterable>().filter(|f| f.eq_ci("level", 1));
        assert!(
            res.is_err(),
            "Expected validation error for eq_ci(level, 1)"
        );
    }

    fn any_in_ci_list_of_ulids_matches() {
        use icydb::{
            db::primitives::{Cmp, FilterClause, FilterExpr},
            types::Ulid,
            value::Value,
        };

        // tags is list of Text; filter with AnyInCi where RHS is list of ULID strings.
        // Should validate and match case-insensitively (ULIDs are folded as text).
        let needles = vec![Value::Ulid(Ulid::generate()), Value::Ulid(Ulid::generate())];

        let bad = FilterExpr::Clause(FilterClause::new(
            "tags",
            Cmp::AnyInCi,
            Value::from(needles),
        ));

        let q = db::query::load().filter(|_| bad);

        // Expect no validation error and possibly empty result set; we're asserting validation only.
        db!()
            .load::<Filterable>()
            .execute(q)
            .expect("AnyInCi with ULID list should validate");
    }

    fn any_in_ci_list_non_text_still_validates() {
        use icydb::{
            db::primitives::{Cmp, FilterClause, FilterExpr},
            value::Value,
        };

        // Ensure non-text RHS (ints) does not trigger validation error after relaxation.
        let clause = FilterExpr::Clause(FilterClause::new(
            "tags",
            Cmp::AnyInCi,
            Value::from(vec![Value::Int(1), Value::Int(2)]),
        ));

        let q = db::query::load().filter(|_| clause);
        db!()
            .load::<Filterable>()
            .execute(q)
            .expect("AnyInCi with non-text RHS should validate (may return zero rows)");
    }

    fn invalid_presence_rhs_non_unit() {
        use icydb::{
            db::primitives::{Cmp, FilterClause, FilterExpr},
            value::Value,
        };

        // Manually construct an invalid presence filter: IsNone should use Unit RHS
        let bad = FilterExpr::Clause(FilterClause::new(
            "name",
            Cmp::IsNone,
            Value::Text("x".into()),
        ));
        let q = db::query::load().filter(|_| bad);
        let res = db!().load::<Filterable>().execute(q);

        assert!(
            res.is_err(),
            "Expected validation error for IsNone with non-Unit RHS"
        );
    }

    // -------------------- builder-based filters ---------------------

    fn filter_builder_eq_category() {
        let filter = Filter::<Filterable> {
            category: Some(TextFilter::new().equal("A")),
            ..Default::default()
        };

        let results = db!()
            .load::<Filterable>()
            .filter(|_| filter)
            .unwrap()
            .entities();

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|e| e.category == "A"));
    }

    fn filter_builder_gt_score() {
        let filter = Filter::<Filterable> {
            score: Some(RangeFilter::new().gt(80)),
            ..Default::default()
        };

        let results = db!()
            .load::<Filterable>()
            .filter(|_| filter)
            .unwrap()
            .entities();

        assert_eq!(results.len(), 4);
        assert!(results.iter().all(|e| e.score > Decimal::from(80)));
    }

    fn filter_builder_name_starts_and_ends() {
        let filter = Filter::<Filterable> {
            name: Some(TextFilter::new().starts_with("Al").ends_with("ha")),
            ..Default::default()
        };

        let results = db!()
            .load::<Filterable>()
            .filter(|_| filter)
            .unwrap()
            .entities();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "Alpha");
    }

    fn filter_builder_category_not_equal_ci() {
        let filter = Filter::<Filterable> {
            category: Some(TextFilter::new().not_equal_ci("b")),
            ..Default::default()
        };

        let results = db!()
            .load::<Filterable>()
            .filter(|_| filter)
            .unwrap()
            .entities();

        assert_eq!(results.len(), 6);
        assert!(results.iter().all(|e| e.category != "B"));
    }

    fn filter_builder_name_contains_ci() {
        let filter = Filter::<Filterable> {
            name: Some(TextFilter::new().contains_ci("ta")),
            ..Default::default()
        };

        let results = db!()
            .load::<Filterable>()
            .filter(|_| filter)
            .unwrap()
            .entities();

        assert_eq!(results.len(), 6);
        assert!(results.iter().all(|e| e.name.to_lowercase().contains("ta")));
    }

    fn filter_builder_score_between() {
        let filter = Filter::<Filterable> {
            score: Some(RangeFilter::new().between(70, 90)),
            ..Default::default()
        };

        let results = db!()
            .load::<Filterable>()
            .filter(|_| filter)
            .unwrap()
            .entities();

        assert_eq!(results.len(), 3);
        assert!(
            results
                .iter()
                .all(|e| { e.score >= Decimal::from(70) && e.score <= Decimal::from(90) })
        );
    }

    fn filter_builder_offset_negative() {
        let filter = Filter::<Filterable> {
            offset: Some(RangeFilter::new().lt(0)),
            ..Default::default()
        };

        let results = db!()
            .load::<Filterable>()
            .filter(|_| filter)
            .unwrap()
            .entities();

        let mut names: Vec<_> = results.iter().map(|e| e.name.as_str()).collect();
        names.sort_unstable();

        assert_eq!(names, vec!["Alpha", "Epsilon", "Theta"]);
    }

    fn filter_builder_category_ci_level() {
        let filter = Filter::<Filterable> {
            category: Some(TextFilter::new().equal_ci("a")),
            level: Some(RangeFilter::new().gte(4u64)),
            ..Default::default()
        };

        let results = db!()
            .load::<Filterable>()
            .filter(|_| filter)
            .unwrap()
            .entities();

        let mut names: Vec<_> = results.iter().map(|e| e.name.as_str()).collect();
        names.sort_unstable();

        assert_eq!(names, vec!["Epsilon", "Theta"]);
    }

    fn filter_builder_opt_name_present() {
        // name != empty
        let filter = Filter::<FilterableOpt> {
            name: Some(TextFilter::new().is_empty(false)),
            ..Default::default()
        };

        let results = db!()
            .load::<FilterableOpt>()
            .filter(|_| filter)
            .unwrap()
            .entities();

        // Expect exactly 3 rows with Some(name) and non-empty text
        assert_eq!(results.len(), 3);
        assert!(
            results
                .iter()
                .all(|e| { e.name.is_some() && !e.name.as_ref().unwrap().is_empty() })
        );
    }
}
