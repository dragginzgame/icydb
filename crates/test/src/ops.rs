use icydb::{
    core::{deserialize, serialize},
    prelude::*,
};

///
/// OpsSuite
///

pub struct OpsSuite {}

impl OpsSuite {
    // test
    // best if these are kept in code order so we can see where it failed
    pub fn test() {
        let tests: Vec<(&str, fn())> = vec![("missing_field", Self::missing_field)];

        for (name, test_fn) in tests {
            println!("Running test: {name}");
            test_fn();
        }
    }

    // missing_field
    fn missing_field() {
        use test_design::e2e::db::{MissingFieldLarge, MissingFieldSmall};

        let small = MissingFieldSmall {
            a_id: Ulid::generate(),
            b_id: Ulid::generate(),
        };

        // move from small to large
        let bytes = serialize(&small).unwrap();
        let large = deserialize::<MissingFieldLarge>(&bytes).unwrap();

        assert!(!large.a_id.is_nil());
        assert!(!large.b_id.is_nil());
        assert!(large.c_id.is_nil());
    }
}
