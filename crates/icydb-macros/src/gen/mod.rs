pub mod implementor;

pub use implementor::*;

use crate::{prelude::*, view::*};

///
/// Common interface for all node generators.
///
/// Each generator emits both the base node and all its derived representations
/// (views, filters, create/update types, etc.).
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
        update = $update:tt,
        filter = $filter:tt $(,)?
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
                let filter = expand!($filter);

                quote! {
                    #node
                    #view
                    #create
                    #update
                    #filter
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
    filter = EntityFilter,
);

define_gen!(
    EnumGen,
    Enum,
    view = EnumView,
    create = _,
    update = EnumUpdate,
    filter = _,
);

define_gen!(
    ListGen,
    List,
    view = ListView,
    create = _,
    update = ListUpdate,
    filter = _,
);

define_gen!(
    MapGen,
    Map,
    view = MapView,
    create = _,
    update = MapUpdate,
    filter = _,
);

define_gen!(
    NewtypeGen,
    Newtype,
    view = NewtypeView,
    create = _,
    update = NewtypeUpdate,
    filter = _,
);

define_gen!(
    RecordGen,
    Record,
    view = RecordView,
    create = _,
    update = RecordUpdate,
    filter = RecordFilter,
);

define_gen!(
    SetGen,
    Set,
    view = SetView,
    create = _,
    update = SetUpdate,
    filter = _,
);

define_gen!(
    TupleGen,
    Tuple,
    view = TupleView,
    create = _,
    update = TupleUpdate,
    filter = _,
);

//
// Infrastructure
//

define_gen!(
    CanisterGen,
    Canister,
    view = _,
    create = _,
    update = _,
    filter = _,
);

define_gen!(
    SanitizerGen,
    Sanitizer,
    view = _,
    create = _,
    update = _,
    filter = _,
);
define_gen!(
    StoreGen,
    Store,
    view = _,
    create = _,
    update = _,
    filter = _,
);

define_gen!(
    ValidatorGen,
    Validator,
    view = _,
    create = _,
    update = _,
    filter = _,
);
