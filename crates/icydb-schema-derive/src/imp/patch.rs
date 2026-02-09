use crate::prelude::*;

///
/// MergePatchTrait
///

pub struct MergePatchTrait {}

impl Imp<Entity> for MergePatchTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        Some(merge_impl_fields(node, |n| {
            n.iter_editable_fields().map(|f| f.ident.clone()).collect()
        }))
    }
}

impl Imp<Record> for MergePatchTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        Some(merge_impl_fields(node, |n| {
            n.fields.iter().map(|f| f.ident.clone()).collect()
        }))
    }
}

impl Imp<List> for MergePatchTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        Some(merge_impl_delegate(node))
    }
}

impl Imp<Set> for MergePatchTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        Some(merge_impl_delegate(node))
    }
}

impl Imp<Map> for MergePatchTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        Some(merge_impl_delegate(node))
    }
}

impl Imp<Newtype> for MergePatchTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        Some(merge_impl_delegate(node))
    }
}

///
/// Enum
///

impl Imp<Enum> for MergePatchTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        let q = quote! {
            fn merge(
                &mut self,
                patch: Self::UpdateViewType,
            ) -> ::core::result::Result<(), ::icydb::patch::MergePatchError> {
                *self = patch.into();

                Ok(())
            }
        };

        Some(TraitStrategy::from_impl(
            Implementor::new(node.def(), TraitKind::MergePatch)
                .set_tokens(q)
                .to_token_stream(),
        ))
    }
}

///
/// Tuple
///

impl Imp<Tuple> for MergePatchTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let values = &node.values;

        // paths
        let merge_parts = values.iter().enumerate().map(|(i, _)| {
            let idx = syn::Index::from(i);
            quote! {
                if let Some(v) = patch.#idx {
                    ::icydb::patch::MergePatch::merge(&mut next.#idx, v)
                        .map_err(|err| err.with_index(#i))?;
                }
            }
        });

        // quote
        let q = quote! {
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

        Some(TraitStrategy::from_impl(
            Implementor::new(node.def(), TraitKind::MergePatch)
                .set_tokens(q)
                .to_token_stream(),
        ))
    }
}

///
/// Shared Generators
///

fn merge_impl_fields<N, F>(node: &N, iter_fields: F) -> TraitStrategy
where
    N: HasType,
    F: Fn(&N) -> Vec<syn::Ident>,
{
    let field_idents = iter_fields(node);

    // paths
    let merge_pairs: Vec<_> = field_idents
        .iter()
        .map(|ident| {
            quote! {
                if let Some(v) = patch.#ident {
                    ::icydb::patch::MergePatch::merge(&mut next.#ident, v)
                        .map_err(|err| err.with_field(stringify!(#ident)))?;
                }
            }
        })
        .collect();

    let q = quote! {
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

    let merge_impl = Implementor::new(node.def(), TraitKind::MergePatch)
        .set_tokens(q)
        .to_token_stream();

    let tokens = quote! {
        #merge_impl
    };

    TraitStrategy::from_impl(tokens)
}

fn merge_impl_delegate(node: &impl HasType) -> TraitStrategy {
    // quote
    let q = quote! {
        fn merge(
            &mut self,
            patch: Self::UpdateViewType,
        ) -> ::core::result::Result<(), ::icydb::patch::MergePatchError> {
            // Forward to the inner collection (Vec, BTreeSet, BTreeMap)
            let mut next = self.clone();
            ::icydb::patch::MergePatch::merge(&mut next.0, patch)
                .map_err(|err| err.with_index(0))?;
            *self = next;

            Ok(())
        }
    };

    TraitStrategy::from_impl(
        Implementor::new(node.def(), TraitKind::MergePatch)
            .set_tokens(q)
            .to_token_stream(),
    )
}
