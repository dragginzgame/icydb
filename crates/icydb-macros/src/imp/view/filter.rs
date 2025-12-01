use crate::prelude::*;

///
/// FilterViewTrait
///

pub struct FilterViewTrait;

impl Imp<Entity> for FilterViewTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let filter_ident = node.filter_ident();

        let q = quote! {
            type FilterViewType = #filter_ident;
        };

        let tokens = Implementor::new(node.def(), TraitKind::FilterView)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
