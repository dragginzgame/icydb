use icydb::{
    db::query::{self, LoadQuery, QueryPlan, QueryPlanner},
    design::prelude::*,
};
use test_design::e2e::index::{Indexable, IndexableOptText, NotIndexable};

pub struct IndexSuite;

impl IndexSuite {
    pub fn test() {
        let tests: Vec<(&str, fn())> = vec![
            (
                "insert_indexed_empty_store",
                Self::insert_indexed_empty_store,
            ),
            ("index_on_principal", Self::index_on_principal),
            ("index_on_principal_ulid", Self::index_on_principal_ulid),
            ("index_uses_all_fields", Self::index_uses_all_fields),
            ("index_cant_use_all_fields", Self::index_cant_use_all_fields),
            ("fallback_to_range", Self::fallback_to_range),
            ("negative_index_miss", Self::negative_index_miss),
            ("indexable_opt_text", Self::indexable_opt_text),
        ];

        for (name, test_fn) in tests {
            println!("Running test: {name}");
            test_fn();
        }
    }

    fn insert_indexed_empty_store() {
        crate::clear_test_data_store();

        let db = db!();
        let res = db.insert(IndexableOptText {
            username: Some("first".into()),
            ..Default::default()
        });

        assert!(
            res.is_ok(),
            "expected insert into empty indexed table to succeed"
        );
    }

    fn index_on_principal() {
        let db = db!();
        let pid = Principal::from_slice(&[1; 29]);

        db.replace(Indexable {
            pid,
            ulid: Ulid::from_u128(1),
            score: 42,
            ..Default::default()
        })
        .unwrap();

        let query = query::load().filter(|f| f.eq("pid", pid));

        assert_uses_index::<Indexable>(&query);

        let results = db
            .load::<Indexable>()
            .execute(query.clone())
            .unwrap()
            .entities();
        let count = db.load::<Indexable>().count(query).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(count, 1);
    }

    fn index_on_principal_ulid() {
        let pid = Principal::from_slice(&[1; 29]);
        let ulid = Ulid::from_u128(1);
        let query = query::load().filter(|f| f.eq("pid", pid) & f.eq("ulid", ulid));

        assert_uses_index::<Indexable>(&query);
    }

    fn index_uses_all_fields() {
        let query = query::load().filter(|f| {
            f.eq("pid", Principal::from_slice(&[1; 29]))
                & f.eq("score", 1u32)
                & f.eq("ulid", Ulid::from_u128(10))
        });

        let planner = QueryPlanner::new(query.filter.as_ref());
        let plan = planner.plan::<Indexable>();

        match &plan {
            QueryPlan::Index(index_plan) => {
                let len = index_plan.values.len();

                assert_eq!(
                    len, 3,
                    "Expected all 3 index fields to be matched, got {len}",
                );
                println!("✅ Index plan uses {len} fields");
            }
            _ => panic!("❌ Expected index plan, got: {plan:?}"),
        }

        assert_uses_index::<Indexable>(&query);
    }

    fn index_cant_use_all_fields() {
        let query = query::load()
            .filter(|f| f.eq("pid", Principal::from_slice(&[1; 29])) & f.eq("score", 1u32));

        let planner = QueryPlanner::new(query.filter.as_ref());
        let plan = planner.plan::<Indexable>();

        match &plan {
            QueryPlan::Index(index_plan) => {
                let len = index_plan.values.len();

                assert_eq!(len, 1, "Expected one index field to be matched, got {len}",);
                println!("✅ Index plan uses {len} fields");
            }
            _ => panic!("❌ Expected index plan, got: {plan:?}"),
        }

        assert_uses_index::<Indexable>(&query);
    }

    fn fallback_to_range() {
        let query = query::load().filter(|f| f.gt("score", 50));

        let planner = QueryPlanner::new(query.filter.as_ref());
        let plan = planner.plan::<NotIndexable>();

        match plan {
            QueryPlan::Range(_, _) | QueryPlan::FullScan => {
                println!("✅ Fallback to range/full scan plan");
            }
            _ => panic!("❌ Expected fallback Range plan, got: {plan:?}"),
        }
    }

    fn negative_index_miss() {
        let query = query::load().filter(|f| f.eq("pid", Principal::from_slice(&[99; 29])));
        assert_uses_index::<Indexable>(&query);

        let results = db!().load::<Indexable>().execute(query).entities().unwrap();
        assert!(
            results.is_empty(),
            "Expected no results from unmatched index lookup"
        );
    }

    fn indexable_opt_text() {
        let db = db!();

        // case 1: insert with Some("bob") — should work
        db.replace(IndexableOptText {
            username: Some("bob".into()),
            ..Default::default()
        })
        .unwrap();

        // case 2: insert with None — None should be excluded from the UNIQUE index.
        let first_none_insert = db.replace(IndexableOptText {
            username: None,
            ..Default::default()
        });
        assert!(
            first_none_insert.is_ok(),
            "First NULL username insert should succeed"
        );

        let second_none_insert = db.replace(IndexableOptText {
            username: None,
            ..Default::default()
        });
        assert!(
            second_none_insert.is_ok(),
            "Multiple NULL usernames should be allowed (NULL excluded from index)"
        );
        println!("✅ Multiple NULL usernames allowed (NULL excluded from index)");

        // case 3: insert with Some("bob") again — should violate UNIQUE index
        let dup_bob_insert = db.replace(IndexableOptText {
            username: Some("bob".into()),
            ..Default::default()
        });

        match dup_bob_insert {
            Err(_) => {
                println!("✅ Duplicate 'bob' violates UNIQUE index as expected");
            }
            Ok(_) => panic!("❌ Expected duplicate 'bob' to violate UNIQUE index"),
        }
    }
}

fn assert_uses_index<E: EntityKind>(query: &LoadQuery) {
    let planner = QueryPlanner::new(query.filter.as_ref());
    let plan = planner.plan::<E>();

    match plan {
        QueryPlan::Index { .. } => println!("✅ Used index"),
        _ => panic!("❌ Expected index-based query plan, got: {plan:?}"),
    }
}
