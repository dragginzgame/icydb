use crate::prelude::*;

///
/// TraitStrategy
///

#[derive(Debug, Default)]
pub struct TraitStrategy {
    pub derive: Option<TraitKind>,
    pub imp: Option<TokenStream>,
}

impl TraitStrategy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_derive(t: TraitKind) -> Self {
        Self::new().with_derive(t)
    }

    pub fn from_impl(tokens: TokenStream) -> Self {
        Self::new().with_impl(tokens)
    }

    pub const fn with_derive(mut self, t: TraitKind) -> Self {
        self.derive = Some(t);
        self
    }

    pub fn with_impl(mut self, tokens: TokenStream) -> Self {
        self.imp = Some(tokens);
        self
    }
}
