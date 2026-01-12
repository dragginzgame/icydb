use icydb::{db::query::QueryPlan, design::prelude::*};
use test_design::e2e::db::{ContainsBlob, Index};

///
/// LoadExecutorSuite
///

pub struct LoadExecutorSuite;

impl LoadExecutorSuite {
    pub fn test() {
        let tests: Vec<(&str, fn())> = vec![
            ("pagination_empty_table", Self::pagination_empty_table),
            (
                "pagination_offset_beyond_len_clears",
                Self::pagination_offset_beyond_len_clears,
            ),
            (
                "pagination_no_limit_from_offset",
                Self::pagination_no_limit_from_offset,
            ),
            ("pagination_exact_window", Self::pagination_exact_window),
            (
                "pagination_limit_exceeds_tail",
                Self::pagination_limit_exceeds_tail,
            ),
            ("sort_orders_descending", Self::sort_orders_descending),
            (
                "sort_uses_secondary_field_for_ties",
                Self::sort_uses_secondary_field_for_ties,
            ),
            (
                "sort_places_none_before_some_and_falls_back",
                Self::sort_places_none_before_some_and_falls_back,
            ),
            (
                "index_load_returns_deterministic_key_order",
                Self::index_load_returns_deterministic_key_order,
            ),
        ];

        for (name, test_fn) in tests {
            crate::clear_test_data_store();
            println!("Running test: {name}");
            test_fn();
        }
    }

    fn pagination_empty_table() {
        let res = db!()
            .load::<Index>()
            .execute(db::query::load().offset(0).limit(10))
            .unwrap();

        assert!(res.is_empty());
    }

    fn pagination_offset_beyond_len_clears() {
        let ids = Self::seed_index_rows(&[(1, 10), (2, 20), (3, 30)]);

        let res = db!()
            .load::<Index>()
            .execute(db::query::load().offset(10).limit(5))
            .unwrap();

        assert!(
            res.is_empty(),
            "expected empty result for offset beyond length"
        );
        assert_eq!(ids.len(), 3);
    }

    fn pagination_no_limit_from_offset() {
        let ids = Self::seed_index_rows(&[(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]);

        let res = db!()
            .load::<Index>()
            .execute(db::query::load().offset(2))
            .unwrap();

        assert_eq!(
            res.keys(),
            vec![Key::Ulid(ids[2]), Key::Ulid(ids[3]), Key::Ulid(ids[4])]
        );
    }

    fn pagination_exact_window() {
        let ids = Self::seed_index_rows(&[(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]);

        let res = db!()
            .load::<Index>()
            .execute(db::query::load().offset(1).limit(3))
            .unwrap();

        assert_eq!(
            res.keys(),
            vec![Key::Ulid(ids[1]), Key::Ulid(ids[2]), Key::Ulid(ids[3])]
        );
    }

    fn pagination_limit_exceeds_tail() {
        let ids = Self::seed_index_rows(&[(1, 10), (2, 20), (3, 30)]);

        let res = db!()
            .load::<Index>()
            .execute(db::query::load().offset(1).limit(999))
            .unwrap();

        assert_eq!(res.keys(), vec![Key::Ulid(ids[1]), Key::Ulid(ids[2])]);
    }

    fn sort_orders_descending() {
        Self::seed_index_rows(&[(1, 10), (2, 30), (3, 20)]);

        let res = db!()
            .load::<Index>()
            .execute(db::query::load().sort(|s| s.desc("x")))
            .unwrap();

        let xs: Vec<i32> = res.entities().into_iter().map(|e| e.x).collect();
        assert_eq!(xs, vec![30, 20, 10]);
    }

    fn sort_uses_secondary_field_for_ties() {
        let ids = Self::seed_index_rows_xy(&[(1, 1, 5), (2, 1, 8), (3, 2, 3)]);

        let res = db!()
            .load::<Index>()
            .execute(db::query::load().sort(|s| s.asc("x")).sort(|s| s.desc("y")))
            .unwrap();

        let pks = res.pks();
        assert_eq!(pks, vec![ids[1], ids[0], ids[2]]);
    }

    fn sort_places_none_before_some_and_falls_back() {
        let none_id = Ulid::from_parts(1, 1);
        let first_id = Ulid::from_parts(2, 2);
        let second_id = Ulid::from_parts(3, 3);

        db!()
            .insert(ContainsBlob {
                id: none_id,
                bytes: None,
                ..Default::default()
            })
            .unwrap();
        db!()
            .insert(ContainsBlob {
                id: first_id,
                bytes: Some(vec![1_u8].into()),
                ..Default::default()
            })
            .unwrap();
        db!()
            .insert(ContainsBlob {
                id: second_id,
                bytes: Some(vec![1_u8].into()),
                ..Default::default()
            })
            .unwrap();

        let res = db!()
            .load::<ContainsBlob>()
            .execute(
                db::query::load()
                    .sort(|s| s.asc("bytes"))
                    .sort(|s| s.asc("id")),
            )
            .unwrap();

        assert_eq!(res.pks(), vec![none_id, first_id, second_id]);
    }

    fn index_load_returns_deterministic_key_order() {
        let id_two = Ulid::from_parts(2, 2);
        let id_one = Ulid::from_parts(1, 1);
        let id_three = Ulid::from_parts(3, 3);

        db!()
            .insert(Index {
                id: id_two,
                x: 7,
                y: 1,
                ..Default::default()
            })
            .unwrap();
        db!()
            .insert(Index {
                id: id_one,
                x: 7,
                y: 2,
                ..Default::default()
            })
            .unwrap();
        db!()
            .insert(Index {
                id: id_three,
                x: 7,
                y: 3,
                ..Default::default()
            })
            .unwrap();

        let query = db::query::load().filter(|f| f.eq("x", 7));
        let plan = db!().load::<Index>().explain(query.clone()).unwrap();

        match plan {
            QueryPlan::Index(_) => {}
            _ => panic!("expected index plan for x filter"),
        }

        let res = db!().load::<Index>().execute(query).unwrap();
        assert_eq!(
            res.keys(),
            vec![Key::Ulid(id_one), Key::Ulid(id_two), Key::Ulid(id_three)]
        );
    }

    fn seed_index_rows(rows: &[(u64, i32)]) -> Vec<Ulid> {
        let mut ids = Vec::with_capacity(rows.len());

        for (idx, x) in rows {
            let id = Ulid::from_parts(*idx, u128::from(*idx));
            db!()
                .insert(Index {
                    id,
                    x: *x,
                    y: *x,
                    ..Default::default()
                })
                .unwrap();
            ids.push(id);
        }

        ids
    }

    fn seed_index_rows_xy(rows: &[(u64, i32, i32)]) -> Vec<Ulid> {
        let mut ids = Vec::with_capacity(rows.len());

        for (idx, x, y) in rows {
            let id = Ulid::from_parts(*idx, u128::from(*idx));
            db!()
                .insert(Index {
                    id,
                    x: *x,
                    y: *y,
                    ..Default::default()
                })
                .unwrap();
            ids.push(id);
        }

        ids
    }
}
