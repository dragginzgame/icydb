use crate::prelude::*;

///
/// Sanitizer
///
/// Schema node describing one named sanitizer definition and its shared
/// definition metadata.
///

#[derive(Clone, Debug, Serialize)]
pub struct Sanitizer {
    def: Def,
}

impl Sanitizer {
    /// Creates a sanitizer node from definition metadata.
    #[must_use]
    pub const fn new(def: Def) -> Self {
        Self { def }
    }

    /// Returns the sanitizer definition metadata.
    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }
}

impl MacroNode for Sanitizer {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ValidateNode for Sanitizer {}

impl VisitableNode for Sanitizer {
    fn route_key(&self) -> String {
        self.def().path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def().accept(v);
    }
}
