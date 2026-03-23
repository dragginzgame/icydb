use crate::prelude::*;

///
/// StoreKindTrait
///

pub struct StoreKindTrait {}

impl Imp<Store> for StoreKindTrait {
    fn strategy(node: &Store) -> Option<TraitStrategy> {
        Some(store_kind_strategy(node))
    }
}

fn store_kind_strategy(node: &Store) -> TraitStrategy {
    let canister = &node.canister;
    let tokens = Implementor::new(node.def(), TraitKind::StoreKind)
        .set_tokens(quote! {
            type Canister = #canister;
        })
        .to_token_stream();

    TraitStrategy::from_impl(tokens)
}
