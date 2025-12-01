pub mod sanitizer;
pub mod types;
pub mod validator;

// Re-export core for convenience inside this crate
pub use icydb_core as core;

pub mod prelude {
    pub use crate::{sanitizer, types, validator};
    pub use icydb_core::{
        design::prelude::*,
        traits::{Sanitizer, Validator},
        visitor::{sanitize, validate},
    };
    pub use std::str::FromStr as _;
}
