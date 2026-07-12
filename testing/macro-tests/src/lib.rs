pub mod admin;
pub mod e2e;
pub mod schema;
pub mod test;

///
/// Prelude
///

pub(crate) mod prelude {
    #[cfg(test)]
    pub use crate::schema::test::TestStore;

    #[cfg(test)]
    pub(crate) use crate::{assert_invalid, assert_valid, test_ulid};

    #[cfg(test)]
    #[cfg(test)]
    pub use icydb::design::prelude::*;
}

#[cfg(test)]
pub(crate) const fn test_ulid(timestamp_ms: u64, randomness: u128) -> icydb::types::Ulid {
    let timestamp = timestamp_ms.to_be_bytes();
    let random = randomness.to_be_bytes();

    icydb::types::Ulid::from_bytes([
        timestamp[2],
        timestamp[3],
        timestamp[4],
        timestamp[5],
        timestamp[6],
        timestamp[7],
        random[6],
        random[7],
        random[8],
        random[9],
        random[10],
        random[11],
        random[12],
        random[13],
        random[14],
        random[15],
    ])
}

#[macro_export]
macro_rules! assert_valid {
    ($value:expr) => {
        assert!(
            icydb::validate(&$value).is_ok(),
            "expected valid: {:?}",
            &$value
        );
    };
}

#[macro_export]
macro_rules! assert_invalid {
    ($value:expr) => {
        assert!(
            icydb::validate(&$value).is_err(),
            "expected invalid: {:?}",
            &$value
        );
    };
}
