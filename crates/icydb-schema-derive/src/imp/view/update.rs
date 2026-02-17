use crate::prelude::*;

///
/// UpdateViewTrait
///

pub struct UpdateViewTrait {}

impl Imp<Entity> for UpdateViewTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        Some(update_impl_fields(node, |n| {
            n.iter_editable_fields().map(|f| f.ident.clone()).collect()
        }))
    }
}

impl Imp<Record> for UpdateViewTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        Some(update_impl_fields(node, |n| {
            n.fields.iter().map(|f| f.ident.clone()).collect()
        }))
    }
}

impl Imp<List> for UpdateViewTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        Some(update_impl_delegate(node))
    }
}

impl Imp<Set> for UpdateViewTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        Some(update_impl_delegate(node))
    }
}

impl Imp<Map> for UpdateViewTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        Some(update_impl_delegate(node))
    }
}

impl Imp<Newtype> for UpdateViewTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        Some(update_impl_delegate(node))
    }
}

impl Imp<Enum> for UpdateViewTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        let merge_fn = quote! {
            fn merge(
                &mut self,
                patch: Self::UpdateViewType,
            ) -> ::core::result::Result<(), ::icydb::patch::MergePatchError> {
                *self = patch.into();

                Ok(())
            }
        };

        Some(update_impl(node, merge_fn))
    }
}

impl Imp<Tuple> for UpdateViewTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let values = &node.values;

        let merge_parts = values.iter().enumerate().map(|(i, _)| {
            let idx = syn::Index::from(i);
            quote! {
                if let Some(v) = patch.#idx {
                    ::icydb::__macro::CoreUpdateView::merge(&mut next.#idx, v)
                        .map_err(|err| err.with_index(#i))?;
                }
            }
        });

        let merge_fn = quote! {
            fn merge(
                &mut self,
                patch: Self::UpdateViewType,
            ) -> ::core::result::Result<(), ::icydb::patch::MergePatchError> {
                let mut next = self.clone();
                #(#merge_parts)*
                *self = next;

                Ok(())
            }
        };

        Some(update_impl(node, merge_fn))
    }
}

fn update_impl_fields<N, F>(node: &N, iter_fields: F) -> TraitStrategy
where
    N: HasType,
    F: Fn(&N) -> Vec<syn::Ident>,
{
    let field_idents = iter_fields(node);

    let merge_pairs: Vec<_> = field_idents
        .iter()
        .map(|ident| {
            quote! {
                if let Some(v) = patch.#ident {
                    ::icydb::__macro::CoreUpdateView::merge(&mut next.#ident, v)
                        .map_err(|err| err.with_field(stringify!(#ident)))?;
                }
            }
        })
        .collect();

    let merge_fn = quote! {
        fn merge(
            &mut self,
            patch: Self::UpdateViewType,
        ) -> ::core::result::Result<(), ::icydb::patch::MergePatchError> {
            let mut next = self.clone();
            #(#merge_pairs)*
            *self = next;

            Ok(())
        }
    };

    update_impl(node, merge_fn)
}

fn update_impl_delegate(node: &impl HasType) -> TraitStrategy {
    let merge_fn = quote! {
        fn merge(
            &mut self,
            patch: Self::UpdateViewType,
        ) -> ::core::result::Result<(), ::icydb::patch::MergePatchError> {
            let mut next = self.clone();
            ::icydb::__macro::CoreUpdateView::merge(&mut next.0, patch)
                .map_err(|err| err.with_index(0))?;
            *self = next;

            Ok(())
        }
    };

    update_impl(node, merge_fn)
}

fn update_impl(node: &impl HasType, merge_fn: TokenStream) -> TraitStrategy {
    let update_path = node.update_path();
    let q = quote! {
        type UpdateViewType = #update_path;

        #merge_fn
    };

    TraitStrategy::from_impl(
        Implementor::new(node.def(), TraitKind::UpdateView)
            .set_tokens(q)
            .to_token_stream(),
    )
}
