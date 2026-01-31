pub mod implementor;

pub use implementor::*;

use crate::{prelude::*, view::*};

///
/// Common interface for all node generators.
///
/// Each generator emits both the base node and all its derived representations
/// (views, create/update types, etc.).
///

pub trait NodeGen {
    /// Emit the code for this node and all derived forms.
    fn generate(&self) -> TokenStream;
}

///
/// Nodes
///

macro_rules! define_gen {
    (
        $gen:ident, $node:ty,
        view = $view:tt,
        create = $create:tt,
        update = $update:tt $(,)?
    ) => {
        pub struct $gen<'a>(pub &'a $node);

        impl NodeGen for $gen<'_> {
            fn generate(&self) -> TokenStream {
                let node = self.0;

                // Expand helper
                macro_rules! expand {
                    (_) => {
                        quote!()
                    };
                    ($path:ident) => {
                        $path(node)
                    };
                }

                let view = expand!($view);
                let create = expand!($create);
                let update = expand!($update);

                quote! {
                    #node
                    #view
                    #create
                    #update
                }
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

define_gen!(
    EntityGen,
    Entity,
    view = EntityView,
    create = EntityCreate,
    update = EntityUpdate,
);

define_gen!(
    EnumGen,
    Enum,
    view = EnumView,
    create = _,
    update = EnumUpdate,
);

define_gen!(
    ListGen,
    List,
    view = ListView,
    create = _,
    update = ListUpdate,
);

define_gen!(MapGen, Map, view = MapView, create = _, update = MapUpdate,);

define_gen!(
    NewtypeGen,
    Newtype,
    view = NewtypeView,
    create = _,
    update = NewtypeUpdate,
);

define_gen!(
    RecordGen,
    Record,
    view = RecordView,
    create = _,
    update = RecordUpdate,
);

define_gen!(SetGen, Set, view = SetView, create = _, update = SetUpdate,);

define_gen!(
    TupleGen,
    Tuple,
    view = TupleView,
    create = _,
    update = TupleUpdate,
);

//
// Infrastructure
//

define_gen!(CanisterGen, Canister, view = _, create = _, update = _,);
define_gen!(DataStoreGen, DataStore, view = _, create = _, update = _,);
define_gen!(IndexStoreGen, IndexStore, view = _, create = _, update = _,);
define_gen!(SanitizerGen, Sanitizer, view = _, create = _, update = _,);
define_gen!(ValidatorGen, Validator, view = _, create = _, update = _,);
