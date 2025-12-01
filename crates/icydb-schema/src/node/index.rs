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
    pub store: &'static str,
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
            write!(f, "UNIQUE {}({})", self.store, fields)
        } else {
            write!(f, "{}({})", self.store, fields)
        }
    }
}

impl MacroNode for Index {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ValidateNode for Index {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        let schema = schema_read();

        // store
        match schema.cast_node::<Store>(self.store) {
            Ok(store) if !matches!(store.ty, StoreType::Index) => {
                err!(errs, "store is not type Index");
            }
            Ok(_) => {}
            Err(e) => errs.add(e),
        }

        errs.result()
    }
}

impl VisitableNode for Index {
    fn route_key(&self) -> String {
        self.fields.join(", ")
    }
}
