use std::collections::HashMap;

use icydb::{
    core::types::{Principal, Ulid},
    prelude::*,
};
use test_design::e2e::filter::{Filterable, FilterableEnum, FilterableOpt};

pub fn insert_filterable_data() -> HashMap<String, Ulid> {
    use FilterableEnum::{A, B, C};

    #[rustfmt::skip]
    let fixtures = [
        ("Alpha", "A", true, 87.2, 1, -10, vec!["red", "blue"], 1, A),
        ("Beta", "B", false, 65.1, 2, 0, vec!["green"], 2, B),
        ("Gamma", "C", true, 92.5, 3, 10, vec!["red", "yellow"], 3, C),
        ("Delta", "B", false, 15.3, 2, 5, vec![], 4, B),
        ("Epsilon", "A", true, 75.0, 4, -5, vec!["green", "blue"], 5, A),
        ("Zeta", "C", false, 88.8, 5, 15, vec!["purple"], 6, C),
        ("Eta", "B", true, 30.5, 1, 8, vec!["red"], 7, B),
        ("Theta", "A", true, 99.9, 6, -20, vec!["blue", "green"], 8 ,A),
        ("Iota", "C", false, 42.0, 3, 0, vec!["yellow", "red"], 9, C),
        ("Kappa", "B", true, 50.0, 2, 3, vec!["green", "blue"], 10, B),
    ];

    let mut ids = HashMap::new();

    for (name, category, active, score, level, offset, tags, pid_index, abc) in fixtures {
        let res = db!()
            .insert(Filterable {
                name: name.into(),
                category: category.into(),
                active,
                score: Decimal::from(score),
                level,
                offset,
                tags: tags.iter().map(ToString::to_string).collect(),
                pid: Principal::dummy(pid_index),
                abc,
                ..Default::default()
            })
            .unwrap();

        ids.insert(name.to_string(), res.primary_key());
    }

    ids
}

pub fn insert_filterable_opt_data(relations: &HashMap<String, Ulid>) {
    let fixtures = [
        (
            Some("Alice"),
            Some(1),
            Some(-10),
            Some(Principal::dummy(1)),
            Some("Alpha"),
        ),
        (
            Some("Bob"),
            Some(2),
            Some(0),
            Some(Principal::dummy(2)),
            Some("Beta"),
        ),
        (
            None,
            Some(3),
            None,
            Some(Principal::dummy(3)),
            Some("Alpha"),
        ),
        (Some("Charlie"), None, Some(5), None, None),
        (None, None, None, None, None),
    ];

    for (name, level, offset, pid, rel_name) in fixtures {
        db!()
            .insert(FilterableOpt {
                name: name.map(str::to_string),
                level,
                offset,
                pid,
                rel_id: rel_name.and_then(|r| relations.get(r).copied()),
                ..Default::default()
            })
            .unwrap();
    }
}

pub fn seed_filter_data() {
    let relations = insert_filterable_data();
    insert_filterable_opt_data(&relations);
}
