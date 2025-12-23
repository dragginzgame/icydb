use icydb::core::{
    Key, Value,
    db::{
        primitives::filter::{Cmp, FilterClause, FilterExpr},
        query::{QueryPlan, QueryPlanner},
    },
};
use icydb::prelude::*;
use test_design::e2e::db::Index;

///
/// QueryPlannerSuite
///

pub struct QueryPlannerSuite;

impl QueryPlannerSuite {
    pub fn test() {
        let tests: Vec<(&str, fn())> = vec![
            (
                "planner_in_list_empty_returns_empty_keys",
                Self::planner_in_list_empty_returns_empty_keys,
            ),
            (
                "planner_in_list_dedups_keys",
                Self::planner_in_list_dedups_keys,
            ),
            (
                "pk_in_rejects_non_key_values",
                Self::pk_in_rejects_non_key_values,
            ),
            ("pk_in_accepts_text_keys", Self::pk_in_accepts_text_keys),
        ];

        for (name, test_fn) in tests {
            crate::clear_test_data_store();
            println!("Running test: {name}");
            test_fn();
        }
    }

    fn planner_in_list_empty_returns_empty_keys() {
        let expr = FilterExpr::Clause(FilterClause::new(
            Index::PRIMARY_KEY,
            Cmp::In,
            Value::List(Vec::new()),
        ));
        let plan = QueryPlanner::new(Some(&expr)).plan::<Index>();

        match plan {
            QueryPlan::Keys(keys) => assert!(keys.is_empty()),
            _ => panic!("expected empty key plan"),
        }
    }

    fn planner_in_list_dedups_keys() {
        let first = Ulid::from_parts(1, 1);
        let second = Ulid::from_parts(2, 2);

        let expr = FilterExpr::Clause(FilterClause::new(
            Index::PRIMARY_KEY,
            Cmp::In,
            Value::List(vec![
                Value::Ulid(second),
                Value::Ulid(first),
                Value::Ulid(second),
            ]),
        ));
        let plan = QueryPlanner::new(Some(&expr)).plan::<Index>();

        match plan {
            QueryPlan::Keys(keys) => {
                assert_eq!(keys, vec![Key::Ulid(first), Key::Ulid(second)]);
            }
            _ => panic!("expected key plan"),
        }
    }

    fn pk_in_rejects_non_key_values() {
        let expr = FilterExpr::Clause(FilterClause::new(
            Index::PRIMARY_KEY,
            Cmp::In,
            Value::List(vec![Value::Text("nope".to_string())]),
        ));
        let query = db::query::load().filter_expr(expr);

        let err = db!().load::<Index>().execute(query).unwrap_err();
        assert!(err.to_string().contains("invalid filter value"));
    }

    fn pk_in_accepts_text_keys() {
        let id = Ulid::from_parts(42, 42);
        db!()
            .insert(Index {
                id,
                x: 1,
                y: 2,
                ..Default::default()
            })
            .unwrap();

        let expr = FilterExpr::Clause(FilterClause::new(
            Index::PRIMARY_KEY,
            Cmp::In,
            Value::List(vec![Value::Text(id.to_string())]),
        ));
        let query = db::query::load().filter_expr(expr);
        let plan = db!().load::<Index>().explain(query.clone()).unwrap();

        match plan {
            QueryPlan::Keys(_) => {}
            _ => panic!("expected key plan for PK IN"),
        }

        let res = db!().load::<Index>().execute(query).unwrap();
        assert_eq!(res.pks(), vec![id]);
    }
}
