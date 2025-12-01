use crate::prelude::*;
use quote::ToTokens;

///
/// View
///
/// A node that emits additional derived view representations
/// (e.g., main View, Edit, Filter).
///

pub trait View: ToTokens {
    /// Generate the view's token stream.
    fn generate(&self) -> TokenStream;

    /// List of traits this node participates in
    /// (either derived or implemented).
    fn traits(&self) -> TraitSet {
        TraitSet::from(vec![
            TraitKind::CandidType,
            TraitKind::Clone,
            TraitKind::Debug,
            TraitKind::Serialize,
            TraitKind::Deserialize,
        ])
    }
}

///
/// ViewExpr
/// for when a node makes up a part of a larger View
///

pub trait ViewExpr {
    fn expr(&self) -> Option<TokenStream>;
}
