pub use icydb_testing_fixtures::macro_test::validate::option::*;

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::validate;

    #[test]
    fn none_is_valid() {
        let value = OptionalThreshold { threshold: None };
        assert!(validate(&value).is_ok());
    }

    #[test]
    fn above_threshold_is_valid() {
        let value = OptionalThreshold {
            threshold: Some(42),
        };
        assert!(validate(&value).is_ok());
    }

    #[test]
    fn below_threshold_is_invalid() {
        let value = OptionalThreshold { threshold: Some(5) };
        assert!(validate(&value).is_err());
    }
}
