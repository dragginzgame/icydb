pub use icydb_testing_test_fixtures::macro_test::collection::*;

#[cfg(test)]
pub mod test {
    use super::*;
    use icydb::validate;

    //
    // Helpers
    //

    macro_rules! assert_valid {
        ($val:expr) => {
            assert!(validate(&$val).is_ok(), "expected valid: {:?}", &$val);
        };
    }

    macro_rules! assert_invalid {
        ($val:expr) => {
            assert!(validate(&$val).is_err(), "expected invalid: {:?}", &$val);
        };
    }

    #[test]
    fn test_collections() {
        //
        // PASS
        //
        assert_valid!(ListValidated::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]));
        assert_valid!(MapValidated::from(vec![(1, 2), (3, 4), (5, 6)]));
        assert_valid!(SetValidated::from(vec![1, 2, 2, 2, 3, 4, 8]));

        //
        // FAIL
        //
        assert_invalid!(ListValidated::from(vec![1, 25, 38]));
        assert_invalid!(MapValidated::from(vec![(1, 2), (113, 4), (5, 6)]));
        assert_invalid!(SetValidated::from(vec![1, 2, 2, 52, 3, 4, 8]));
    }
}
