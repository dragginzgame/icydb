use crate::prelude::*;

//
// Def
//

#[derive(CandidType, Clone, Debug, Serialize)]
pub struct Def {
    module_path: &'static str,
    ident: &'static str,
}

impl Def {
    /// Build one schema-definition identity pair.
    #[must_use]
    pub const fn new(module_path: &'static str, ident: &'static str) -> Self {
        Self { module_path, ident }
    }

    /// Borrow definition module path.
    #[must_use]
    pub const fn module_path(&self) -> &'static str {
        self.module_path
    }

    /// Borrow definition identifier.
    #[must_use]
    pub const fn ident(&self) -> &'static str {
        self.ident
    }

    // path
    // the path to the actual Type
    // ie. design::game::Rarity
    #[must_use]
    pub fn path(&self) -> String {
        format!("{}::{}", self.module_path(), self.ident())
    }
}

impl ValidateNode for Def {}

impl VisitableNode for Def {}
