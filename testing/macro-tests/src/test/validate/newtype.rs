pub use icydb_testing_fixtures::macro_test::validate::newtype::*;

#[cfg(test)]
mod test {
    use super::*;
    use icydb::validate;

    #[test]
    fn blob_length_validation() {
        // too long: 600 bytes
        let too_long = Blob::from(vec![0u8; 600]);
        assert!(validate(&too_long).is_err(), "expected bytes length error");

        // valid: exactly 500 bytes
        let valid = Blob::from(vec![0u8; 500]);
        assert!(validate(&valid).is_ok(), "500 bytes should be valid");

        // valid: shorter (e.g., 100 bytes)
        let short = Blob::from(vec![0u8; 100]);
        assert!(validate(&short).is_ok(), "shorter blobs should be valid");
    }
}
