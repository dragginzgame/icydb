use crate::prelude::*;

///
/// IndexStoreKindTrait
///

pub struct IndexStoreKindTrait {}

impl Imp<IndexStore> for IndexStoreKindTrait {
    fn strategy(node: &IndexStore) -> Option<TraitStrategy> {
        let canister = &node.canister;

        // static definitions
        let q = quote! {
            type Canister = #canister;
        };

        let tokens = Implementor::new(node.def(), TraitKind::IndexStoreKind)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
