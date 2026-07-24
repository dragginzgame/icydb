//! Module: types::unit
//! Defines the zero-sized unit key/value wrapper used by schemas that need an
//! explicit empty identity.

use candid::CandidType;
use serde::{Deserialize, Serialize};

//
// Unit
//

#[derive(
    CandidType,
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Deserialize,
    Serialize,
)]
pub struct Unit;
