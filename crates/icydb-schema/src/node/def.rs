use crate::prelude::*;

///
/// Def
///

#[derive(CandidType, Clone, Debug, Serialize)]
pub struct Def {
    pub module_path: &'static str,
    pub ident: &'static str,
    pub comments: Option<&'static str>,
}

impl Def {
    // path
    // the path to the actual Type
    // ie. design::game::Rarity
    #[must_use]
    pub fn path(&self) -> String {
        format!("{}::{}", self.module_path, self.ident)
    }
}

impl ValidateNode for Def {}

impl VisitableNode for Def {}
