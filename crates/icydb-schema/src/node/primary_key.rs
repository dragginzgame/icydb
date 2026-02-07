use crate::prelude::*;

///
/// PrimaryKey
///
/// Structured primary-key metadata for an entity schema.
///

#[derive(Clone, Debug, Serialize)]
pub struct PrimaryKey {
    pub field: &'static str,

    #[serde(default)]
    pub source: PrimaryKeySource,
}

///
/// PrimaryKeySource
///
/// Declares where primary-key values originate.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub enum PrimaryKeySource {
    #[default]
    Internal,

    External,
}
