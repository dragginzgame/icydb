//! Module: gen
//! Responsibility: schema registration tokens.
//! Does not own: node parsing or runtime validation.
//! Boundary: parsed nodes to schema writes.

pub mod implementor;

use crate::prelude::*;

pub use implementor::*;

///
/// Nodes
///

macro_rules! define_gen {
    ($gen:ident, $node:ty $(,)?) => {
        pub struct $gen<'a>(pub &'a $node);

        impl ToTokens for $gen<'_> {
            fn to_tokens(&self, tokens: &mut TokenStream) {
                self.0.to_tokens(tokens);
            }
        }
    };
}

//
// Types
//

define_gen!(EntityGen, Entity);
define_gen!(EnumGen, Enum);
define_gen!(ListGen, List);
define_gen!(MapGen, Map);
define_gen!(NewtypeGen, Newtype);
define_gen!(RecordGen, Record);
define_gen!(SetGen, Set);
define_gen!(TupleGen, Tuple);

//
// Infrastructure
//

define_gen!(CanisterGen, Canister);
define_gen!(NormalizerGen, Normalizer);
define_gen!(StoreGen, Store);
define_gen!(ValidatorGen, Validator);
