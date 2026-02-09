use crate::prelude::*;

///
/// UpdateViewTrait
///

pub struct UpdateViewTrait {}

impl Imp<Entity> for UpdateViewTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        Some(update_impl(node))
    }
}

impl Imp<Enum> for UpdateViewTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        Some(update_impl(node))
    }
}

impl Imp<List> for UpdateViewTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        Some(update_impl(node))
    }
}

impl Imp<Map> for UpdateViewTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        Some(update_impl(node))
    }
}

impl Imp<Newtype> for UpdateViewTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        Some(update_impl(node))
    }
}

impl Imp<Record> for UpdateViewTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        Some(update_impl(node))
    }
}

impl Imp<Set> for UpdateViewTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        Some(update_impl(node))
    }
}

impl Imp<Tuple> for UpdateViewTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        Some(update_impl(node))
    }
}

fn update_impl(node: &impl HasType) -> TraitStrategy {
    let update_ident = node.update_ident();
    let q = quote! {
        type UpdateViewType = #update_ident;
    };

    TraitStrategy::from_impl(
        Implementor::new(node.def(), TraitKind::UpdateView)
            .set_tokens(q)
            .to_token_stream(),
    )
}
