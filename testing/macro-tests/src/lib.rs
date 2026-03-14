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
    pub use crate::{assert_invalid, assert_valid};

    #[cfg(test)]
    pub use icydb::base;

    #[cfg(test)]
    pub use icydb::design::prelude::*;
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
