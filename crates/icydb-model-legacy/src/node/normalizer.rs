use crate::prelude::*;

///
/// Normalizer
///
/// Schema node describing one named normalizer definition and its shared
/// definition metadata.
///

#[derive(Clone, Debug, Serialize)]
pub struct Normalizer {
    def: Def,
}

impl Normalizer {
    /// Creates a normalizer node from definition metadata.
    #[must_use]
    pub const fn new(def: Def) -> Self {
        Self { def }
    }

    /// Returns the normalizer definition metadata.
    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }
}

impl MacroNode for Normalizer {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ValidateNode for Normalizer {}

impl VisitableNode for Normalizer {
    fn route_key(&self) -> String {
        self.def().path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def().accept(v);
    }
}
