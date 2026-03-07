use crate::prelude::*;

///
/// Validator
///

#[derive(Clone, Debug, Serialize)]
pub struct Validator {
    def: Def,
}

impl Validator {
    /// Creates a validator node from definition metadata.
    #[must_use]
    pub const fn new(def: Def) -> Self {
        Self { def }
    }

    /// Returns the validator definition metadata.
    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }
}

impl MacroNode for Validator {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ValidateNode for Validator {}

impl VisitableNode for Validator {
    fn route_key(&self) -> String {
        self.def().path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def().accept(v);
    }
}
