use crate::prelude::*;
use std::{
    fmt::{self, Display},
    ops::Not,
};

///
/// Index
///

#[derive(Clone, Debug, Serialize)]
pub struct Index {
    pub fields: &'static [&'static str],

    #[serde(default, skip_serializing_if = "Not::not")]
    pub unique: bool,
}

impl Index {
    #[must_use]
    pub fn is_prefix_of(&self, other: &Self) -> bool {
        self.fields.len() < other.fields.len() && other.fields.starts_with(self.fields)
    }
}

impl Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fields = self.fields.join(", ");

        if self.unique {
            write!(f, "UNIQUE ({fields})")
        } else {
            write!(f, "({fields})")
        }
    }
}

impl MacroNode for Index {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ValidateNode for Index {}

impl VisitableNode for Index {
    fn route_key(&self) -> String {
        self.fields.join(", ")
    }
}
