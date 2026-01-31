use crate::prelude::*;

///
/// DataStoreKindTrait
///

pub struct DataStoreKindTrait {}

impl Imp<DataStore> for DataStoreKindTrait {
    fn strategy(node: &DataStore) -> Option<TraitStrategy> {
        let canister = &node.canister;

        // static definitions
        let q = quote! {
            type Canister = #canister;
        };

        let tokens = Implementor::new(node.def(), TraitKind::DataStoreKind)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
