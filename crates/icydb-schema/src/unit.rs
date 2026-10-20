//! Module: types::unit
//! Defines the zero-sized unit key/value wrapper used by schemas that need an
//! explicit empty identity.

use serde::{Deserialize, Serialize};

//
// Unit
//

#[derive(
    Serialize, Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Ord, PartialOrd, Deserialize,
)]
pub struct Unit;
