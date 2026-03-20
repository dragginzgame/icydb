pub mod implementor;

use crate::prelude::*;

pub use implementor::*;

///
/// Common interface for all node generators.
///
/// Each generator emits the base node plus any supporting impls or schema items
/// attached to that node.
///

pub trait NodeGen {
    /// Emit the complete code for this node.
    fn generate(&self) -> TokenStream;
}

///
/// Nodes
///

macro_rules! define_gen {
    ($gen:ident, $node:ty $(,)?) => {
        pub struct $gen<'a>(pub &'a $node);

        impl NodeGen for $gen<'_> {
            fn generate(&self) -> TokenStream {
                self.0.to_token_stream()
            }
        }

        impl ToTokens for $gen<'_> {
            fn to_tokens(&self, tokens: &mut TokenStream) {
                tokens.extend(self.generate());
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
define_gen!(SanitizerGen, Sanitizer);
define_gen!(StoreGen, Store);
define_gen!(ValidatorGen, Validator);
